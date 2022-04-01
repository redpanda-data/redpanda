/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/service.h"

#include "archival/logger.h"
#include "archival/ntp_archiver_service.h"
#include "archival/types.h"
#include "cloud_storage/types.h"
#include "cluster/partition_manager.h"
#include "cluster/topic_table.h"
#include "config/configuration.h"
#include "config/property.h"
#include "http/client.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "s3/client.h"
#include "s3/error.h"
#include "s3/signature.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/log.h"
#include "storage/ntp_config.h"
#include "utils/gate_guard.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <boost/iterator/counting_iterator.hpp>

#include <algorithm>
#include <exception>
#include <optional>
#include <stdexcept>
#include <tuple>

using namespace std::chrono_literals;

namespace archival::internal {

static ss::sstring get_value_or_throw(
  const config::property<std::optional<ss::sstring>>& prop, const char* name) {
    auto opt = prop.value();
    if (!opt) {
        vlog(
          archival_log.error,
          "Configuration property {} is required to enable archival storage",
          name);
        throw std::runtime_error(
          fmt::format("configuration property {} is not set", name));
    }
    return *opt;
}

/// Use shard-local configuration to generate configuration
ss::future<archival::configuration>
scheduler_service_impl::get_archival_service_config(
  ss::scheduling_group sg, ss::io_priority_class p) {
    vlog(archival_log.debug, "Generating archival configuration");
    auto disable_metrics = net::metrics_disabled(
      config::shard_local_cfg().disable_metrics());

    auto time_limit = config::shard_local_cfg()
                        .cloud_storage_segment_max_upload_interval_sec.value();
    if (time_limit and time_limit.value() == 0s) {
        vlog(
          archival_log.error,
          "Configuration property "
          "cloud_storage_segment_max_upload_interval_sec can't be 0");
        throw std::runtime_error(
          "cloud_storage_segment_max_upload_interval_sec is invalid");
    }
    auto time_limit_opt = time_limit ? std::make_optional(
                            segment_time_limit(*time_limit))
                                     : std::nullopt;
    archival::configuration cfg{
      .bucket_name = s3::bucket_name(get_value_or_throw(
        config::shard_local_cfg().cloud_storage_bucket,
        "cloud_storage_bucket")),
      .interval
      = config::shard_local_cfg().cloud_storage_reconciliation_ms.value(),
      .initial_backoff
      = config::shard_local_cfg().cloud_storage_initial_backoff_ms.value(),
      .segment_upload_timeout
      = config::shard_local_cfg()
          .cloud_storage_segment_upload_timeout_ms.value(),
      .manifest_upload_timeout
      = config::shard_local_cfg()
          .cloud_storage_manifest_upload_timeout_ms.value(),
      .svc_metrics_disabled = service_metrics_disabled(
        static_cast<bool>(disable_metrics)),
      .ntp_metrics_disabled = per_ntp_metrics_disabled(
        static_cast<bool>(disable_metrics)),
      .time_limit = time_limit_opt,
      .upload_scheduling_group = sg,
      .upload_io_priority = p};
    vlog(archival_log.debug, "Archival configuration generated: {}", cfg);
    co_return cfg;
}

scheduler_service_impl::scheduler_service_impl(
  const configuration& conf,
  ss::sharded<cloud_storage::remote>& remote,
  ss::sharded<storage::api>& api,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::topic_table>& tt)
  : _conf(conf)
  , _partition_manager(pm)
  , _topic_table(tt)
  , _storage_api(api)
  , _jitter(conf.interval, 1ms)
  , _rtclog(archival_log, _rtcnode)
  , _probe(conf.svc_metrics_disabled)
  , _remote(remote)
  , _topic_manifest_upload_timeout(conf.manifest_upload_timeout)
  , _initial_backoff(conf.initial_backoff)
  , _upload_sg(conf.upload_scheduling_group) {}

scheduler_service_impl::scheduler_service_impl(
  ss::sharded<cloud_storage::remote>& remote,
  ss::sharded<storage::api>& api,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::topic_table>& tt,
  ss::sharded<archival::configuration>& config)
  : scheduler_service_impl(config.local(), remote, api, pm, tt) {}

void scheduler_service_impl::rearm_timer() {
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return ss::with_scheduling_group(_upload_sg, [this] {
                              return reconcile_archivers().finally([this] {
                                  if (_gate.is_closed()) {
                                      return;
                                  }
                                  _timer.rearm(_jitter());
                              });
                          });
                      }).handle_exception([this](std::exception_ptr e) {
        vlog(_rtclog.info, "Error in timer callback: {}", e);
    });
}
ss::future<> scheduler_service_impl::start() {
    _timer.set_callback([this] { rearm_timer(); });
    _timer.rearm(_jitter());
    return ss::now();
}

ss::future<> scheduler_service_impl::stop() {
    vlog(_rtclog.info, "Scheduler service stop");
    _timer.cancel();
    std::vector<ss::future<>> outstanding;
    for (auto& it : _archivers) {
        outstanding.emplace_back(it.second->stop());
    }
    return ss::do_with(
      std::move(outstanding), [this](std::vector<ss::future<>>& outstanding) {
          return ss::when_all_succeed(outstanding.begin(), outstanding.end())
            .finally([this] { return _gate.close(); });
      });
}

ss::future<> scheduler_service_impl::upload_topic_manifest(
  model::topic_namespace topic_ns, model::initial_revision_id rev) {
    gate_guard gg(_gate);
    auto cfg = _topic_table.local().get_topic_cfg(topic_ns);
    if (!cfg) {
        co_return;
    }
    try {
        bool uploaded = false;
        while (!uploaded && !_gate.is_closed()) {
            // This runs asynchronously so we can just retry indefinetly
            retry_chain_node fib(
              _topic_manifest_upload_timeout, _initial_backoff, &_rtcnode);
            retry_chain_logger ctxlog(archival_log, fib);
            vlog(ctxlog.info, "Uploading topic manifest {}", topic_ns);
            cloud_storage::topic_manifest tm(*cfg, rev);
            auto key = tm.get_manifest_path();
            vlog(ctxlog.debug, "Topic manifest object key is '{}'", key);
            auto res = co_await _remote.local().upload_manifest(
              _conf.bucket_name, tm, fib);
            uploaded = res == cloud_storage::upload_result::success;
            if (!uploaded) {
                vlog(ctxlog.warn, "Topic manifest upload timed out: {}", key);
            }
        }
    } catch (const ss::gate_closed_exception& err) {
        vlog(
          _rtclog.error,
          "Topic manifest upload for {} failed, {}",
          topic_ns,
          err);
    } catch (const ss::abort_requested_exception& err) {
        vlog(
          _rtclog.error,
          "Topic manifest upload for {} failed, {}",
          topic_ns,
          err);
    }
}

ss::future<> scheduler_service_impl::add_ntp_archiver(
  ss::lw_shared_ptr<ntp_archiver> archiver) {
    vassert(
      !_archivers.contains(archiver->get_ntp()),
      "archiver for ntp {} already added!",
      archiver->get_ntp());

    if (_gate.is_closed()) {
        return ss::now();
    }
    return archiver->download_manifest().then(
      [this, archiver](cloud_storage::download_result result) {
          auto ntp = archiver->get_ntp();
          switch (result) {
          case cloud_storage::download_result::success:
              vlog(
                _rtclog.info,
                "Found manifest for partition {}",
                archiver->get_ntp());
              _probe.start_archiving_ntp();

              _archivers.emplace(archiver->get_ntp(), archiver);
              archiver->run_upload_loop();

              return ss::now();
          case cloud_storage::download_result::notfound:
              vlog(
                _rtclog.info,
                "Start archiving new partition {}",
                archiver->get_ntp());
              // Start topic manifest upload
              // asynchronously
              if (ntp.tp.partition == 0) {
                  // Upload manifest once per topic. GCS has strict
                  // limits for single object updates.
                  (void)upload_topic_manifest(
                    model::topic_namespace(ntp.ns, ntp.tp.topic),
                    archiver->get_revision_id());
              }
              _probe.start_archiving_ntp();

              _archivers.emplace(archiver->get_ntp(), archiver);
              archiver->run_upload_loop();

              return ss::now();
          case cloud_storage::download_result::failed:
          case cloud_storage::download_result::timedout:
              vlog(_rtclog.warn, "Manifest download failed");
              return ss::make_exception_future<>(ss::timed_out_error());
          }
          return ss::now();
      });
}

ss::future<>
scheduler_service_impl::create_archivers(std::vector<model::ntp> to_create) {
    gate_guard g(_gate);
    // add_ntp_archiver can potentially use two connections
    auto concurrency = std::max(1UL, _remote.local().concurrency() / 2);
    co_await ss::max_concurrent_for_each(
      std::move(to_create), concurrency, [this](const model::ntp& ntp) {
          storage::api& api = _storage_api.local();
          storage::log_manager& lm = api.log_mgr();
          auto log = lm.get(ntp);
          auto part = _partition_manager.local().get(ntp);
          if (log.has_value() && part && part->is_leader()
              && (part->get_ntp_config().is_archival_enabled()
                  || config::shard_local_cfg().cloud_storage_enable_remote_read())) {
              auto archiver = ss::make_lw_shared<ntp_archiver>(
                log->config(),
                _storage_api.local().log_mgr(),
                _conf,
                _remote.local(),
                part);
              return add_ntp_archiver(archiver);
          } else {
              return ss::now();
          }
      });
}

ss::future<>
scheduler_service_impl::remove_archivers(std::vector<model::ntp> to_remove) {
    gate_guard g(_gate);
    return ss::parallel_for_each(
             to_remove,
             [this](const model::ntp& ntp) -> ss::future<> {
                 vlog(_rtclog.info, "removing archiver for {}", ntp.path());
                 auto archiver = _archivers.at(ntp);
                 return archiver->stop().finally([this, ntp] {
                     vlog(_rtclog.info, "archiver stopped {}", ntp.path());
                     _archivers.erase(ntp);
                     _probe.stop_archiving_ntp();
                 });
             })
      .finally([g = std::move(g)] {});
}

ss::future<> scheduler_service_impl::reconcile_archivers() {
    gate_guard g(_gate);
    cluster::partition_manager& pm = _partition_manager.local();
    storage::api& api = _storage_api.local();
    storage::log_manager& lm = api.log_mgr();
    auto snapshot = lm.get_all_ntps();
    std::vector<model::ntp> to_remove;
    std::vector<model::ntp> to_create;
    // find archivers that have already stopped
    for (const auto& [ntp, archiver] : _archivers) {
        auto p = pm.get(ntp);
        if (!snapshot.contains(ntp) || !p || archiver->upload_loop_stopped()) {
            to_remove.push_back(ntp);
        }
    }

    // find new ntps that present in the snapshot only
    std::copy_if(
      snapshot.begin(),
      snapshot.end(),
      std::back_inserter(to_create),
      [this, &pm](const model::ntp& ntp) {
          auto p = pm.get(ntp);
          return ntp.ns != model::redpanda_ns && !_archivers.contains(ntp) && p
                 && p->is_leader();
      });
    // epxect to_create & to_remove be empty most of the time
    if (unlikely(!to_remove.empty() || !to_create.empty())) {
        // run in parallel
        ss::future<> fremove = ss::now();
        if (!to_remove.empty()) {
            fremove = remove_archivers(std::move(to_remove));
        }
        ss::future<> fcreate = ss::now();
        if (!to_create.empty()) {
            fcreate = create_archivers(std::move(to_create));
        }
        auto [remove_fut, create_fut] = co_await ss::when_all(
          std::move(fremove), std::move(fcreate));
        if (remove_fut.failed()) {
            vlog(_rtclog.error, "Failed to remove archivers");
            remove_fut.ignore_ready_future();
        }
        if (create_fut.failed()) {
            vlog(_rtclog.error, "Failed to create archivers");
            create_fut.ignore_ready_future();
        }
    }
}

cloud_storage::remote& scheduler_service_impl::get_remote() {
    return _remote.local();
}

s3::bucket_name scheduler_service_impl::get_bucket() const {
    return _conf.bucket_name;
}

uint64_t scheduler_service_impl::estimate_backlog_size() {
    uint64_t size = 0;
    for (const auto& [ntp, archiver] : _archivers) {
        std::ignore = ntp;
        size += archiver->estimate_backlog_size(_partition_manager.local());
    }
    return size;
}

} // namespace archival::internal
