/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/topic_manifest_upload_manager.h"

#include "cloud_io/remote.h"
#include "cloud_storage/topic_path_provider.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/topic_manifest_uploader.h"
#include "cluster/partition.h"
#include "ssx/actor.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>

#include <chrono>
#include <exception>
#include <variant>

namespace cloud_topics {

namespace {
constexpr auto upload_timeout = std::chrono::seconds(30);
constexpr auto upload_backoff = std::chrono::milliseconds(100);
constexpr auto retry_backoff = std::chrono::seconds(5);
} // namespace

// Loop managed by the manager, with the expectation that it is running on the
// leader of partition 0.
class topic_manifest_upload_manager::loop
  : public ssx::actor<std::monostate, 1, ssx::overflow_policy::drop_oldest> {
public:
    loop(
      model::topic_id_partition tidp,
      ss::lw_shared_ptr<cluster::partition> partition,
      cloud_io::remote& remote,
      const cloud_storage_clients::bucket_name& bucket)
      : _tidp(tidp)
      , _partition(std::move(partition))
      , _uploader(cd_log, bucket, remote)
      , _rtc(_as) {}

    using ssx::actor<std::monostate, 1, ssx::overflow_policy::drop_oldest>::
      start;
    using ssx::actor<std::monostate, 1, ssx::overflow_policy::drop_oldest>::
      stop;

    void signal_upload_needed() { tell({}); }

protected:
    ss::future<> process(std::monostate) override {
        co_await upload_until_success();
    }

    void on_error(std::exception_ptr ex) noexcept override {
        vlog(
          cd_log.error,
          "Unexpected error in topic manifest upload loop for {}: {}",
          _tidp,
          ex);
    }

private:
    ss::future<std::expected<void, topic_manifest_uploader::error>>
    upload_once() {
        auto topic_cfg_opt = _partition->get_topic_config();
        if (!topic_cfg_opt.has_value()) {
            // Topic config may not yet be set if we raced with partition
            // creation. Retry after backoff.
            co_return std::unexpected(
              topic_manifest_uploader::error{
                topic_manifest_uploader::errc::io_error,
                fmt::format(
                  "topic config not yet set on partition 0: {}", _tidp)});
        }

        // Tweak the replication factor based on the current number of
        // replicas, matching the behavior in tiered storage.
        // TODO: probably not needed?
        auto& topic_cfg = topic_cfg_opt->get();
        auto replication_factor = cluster::replication_factor(
          _partition->raft()->config().current_config().voters.size());
        auto cfg_copy = topic_cfg;
        cfg_copy.replication_factor = replication_factor;

        cloud_storage::topic_path_provider path_provider(
          topic_cfg.properties.remote_label,
          topic_cfg.properties.remote_topic_namespace_override);

        auto rev = _partition->get_ntp_config().get_remote_revision();
        retry_chain_node fib(upload_timeout, upload_backoff, &_rtc);

        vlog(cd_log.info, "Uploading topic manifest for {}", _tidp);
        co_return co_await _uploader.upload_manifest(
          path_provider, cfg_copy, rev, fib);
    }

    // Retries uploading until it succeeds or until we're shutting down.
    ss::future<> upload_until_success() {
        while (!_as.abort_requested()) {
            auto res = co_await upload_once();
            if (res.has_value()) {
                // Success! Break out of here to wait for the next signal.
                break;
            }
            using errc = topic_manifest_uploader::errc;
            if (res.error().e == errc::shutting_down) {
                vlog(
                  cd_log.debug,
                  "Exiting topic manifest upload loop for {}",
                  _tidp);
                co_return;
            }
            vlog(
              cd_log.warn,
              "Topic manifest upload failed for {}, retrying in {}: {}",
              _tidp,
              retry_backoff,
              res.error());
            try {
                co_await ss::sleep_abortable(retry_backoff, _as);
            } catch (const ss::sleep_aborted&) {
                co_return;
            }
        }
    }

    model::topic_id_partition _tidp;
    ss::lw_shared_ptr<cluster::partition> _partition;
    topic_manifest_uploader _uploader;
    retry_chain_node _rtc;
};

topic_manifest_upload_manager::topic_manifest_upload_manager(
  ss::sharded<cloud_io::remote>& remote,
  cloud_storage_clients::bucket_name bucket)
  : _remote(remote)
  , _bucket(std::move(bucket))
  , _queue([](const std::exception_ptr& ex) {
      vlog(
        cd_log.error,
        "Unexpected error in topic manifest uploader work queue: {}",
        ex);
  }) {}

topic_manifest_upload_manager::~topic_manifest_upload_manager() = default;

void topic_manifest_upload_manager::on_leadership_or_properties_change(
  model::topic_id_partition tidp,
  ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>> partition) {
    _queue.submit([this, tidp, partition = std::move(partition)]() mutable {
        return reset_or_signal_loop(tidp, std::move(partition));
    });
}

ss::future<> topic_manifest_upload_manager::reset_or_signal_loop(
  model::topic_id_partition tidp,
  ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>> partition) {
    auto it = _loops.find(tidp);
    if (!partition) {
        // We're not the leader. If we have a loop, stop it now.
        if (it != _loops.end()) {
            co_await it->second->stop();
            _loops.erase(it);
        }
        co_return;
    }
    // We're leader. Either start a loop or signal the existing one.
    if (it != _loops.end()) {
        it->second->signal_upload_needed();
        co_return;
    }
    auto lp = std::make_unique<loop>(
      tidp, std::move(*partition), _remote.local(), _bucket);
    co_await lp->start();
    lp->signal_upload_needed();
    _loops.emplace(tidp, std::move(lp));
}

ss::future<> topic_manifest_upload_manager::start() { co_return; }

ss::future<> topic_manifest_upload_manager::stop() {
    vlog(cd_log.info, "Stopping topic manifest upload manager...");
    co_await _queue.shutdown();
    for (auto& [tidp, loop] : _loops) {
        co_await loop->stop();
    }
    _loops.clear();
    vlog(cd_log.info, "Stopped topic manifest upload manager...");
}

} // namespace cloud_topics
