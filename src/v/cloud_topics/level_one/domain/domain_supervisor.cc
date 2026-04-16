/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/domain/domain_supervisor.h"

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/domain/db_domain_manager.h"
#include "cloud_topics/level_one/domain/domain_manager_probe.h"
#include "cloud_topics/level_one/domain/simple_domain_manager.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "cloud_topics/logger.h"
#include "cluster/controller.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "ssx/when_all.h"
#include "ssx/work_queue.h"

#include <seastar/coroutine/as_future.hh>

using namespace std::chrono_literals;

namespace cloud_topics::l1 {
class domain_supervisor::impl {
public:
    explicit impl(
      cluster::controller* controller,
      io* io,
      std::filesystem::path staging_dir,
      cloud_io::remote* remote,
      cloud_storage_clients::bucket_name bucket,
      ss::scheduling_group sg)
      : _controller(controller)
      , _object_io(io)
      , _staging_dir(std::move(staging_dir))
      , _remote(remote)
      , _bucket(std::move(bucket))
      , _sg(sg)
      , _queue([](const std::exception_ptr& ex) {
          vlog(cd_log.error, "Unexpected domain supervisor error: {}", ex);
      }) {}

    ss::future<> start() {
        _probe.setup_metrics();
        if (ss::this_shard_id() == 0) {
            _as = {};
            _loop = do_topic_reconciliation_loop();
        }
        co_return;
    }

    void on_domain_leadership_change(
      const model::ntp& ntp,
      ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>> partition) {
        std::optional<model::term_id> term;
        if (partition) {
            auto raft = partition->get()->raft();
            vassert(
              raft->is_leader(),
              "Expected to be leader of {} with `partition` set",
              ntp);
            term = raft->term();
        }
        _queue.submit(
          [this, ntp = ntp, partition = std::move(partition), term]() mutable {
              return reset_domain_manager(
                std::move(ntp), std::move(partition), term);
          });
    }

    ss::future<> stop() {
        if (ss::this_shard_id() == 0 && _loop) {
            _as.request_abort();
            co_await *std::exchange(_loop, std::nullopt);
        }
        co_await _queue.shutdown();
        chunked_vector<ss::future<std::monostate>> stop_futs;
        stop_futs.reserve(_domains.size());
        for (auto& [_, domain_mgr] : _domains) {
            stop_futs.emplace_back(domain_mgr->stop_and_wait().then(
              [] { return std::monostate{}; }));
        }
        auto res = co_await ss::coroutine::as_future(
          ssx::when_all_succeed<chunked_vector<std::monostate>>(
            std::move(stop_futs)));
        if (res.failed()) {
            auto ex = res.get_exception();
            vlog(cd_log.error, "Error stopping domain managers: {}", ex);
        }
    }

    ss::shared_ptr<domain_manager> get(const model::ntp& ntp) const {
        auto it = _domains.find(ntp);
        if (it == _domains.end()) {
            return nullptr;
        }
        return it->second;
    }

    ss::future<bool>
    maybe_create_metastore_topic(std::optional<int> num_partitions) {
        if (
          _controller->get_topics_state().local().contains(
            model::l1_metastore_nt)) {
            co_return true;
        }
        co_return co_await create_domains_topic(num_partitions);
    }

private:
    ss::future<> do_topic_reconciliation_loop() {
        while (!_as.abort_requested()) {
            co_await ensure_domains_topic();

            bool aborted = co_await loop_sleep(10min);
            if (aborted) {
                // If we were aborted, we exit the loop.
                co_return;
            }
        }
    }

    ss::future<> ensure_domains_topic() {
        auto backoff = make_exponential_backoff_policy<ss::lowres_clock>(
          1s, 10s);
        while (!_as.abort_requested()) {
            if (
              _controller->get_topics_state().local().contains(
                model::l1_metastore_nt)) {
                if (co_await ensure_domains_replication_factor()) {
                    break;
                }
            } else {
                if (co_await create_domains_topic()) {
                    break;
                }
            }
            backoff.next_backoff();
            if (co_await loop_sleep(backoff.current_backoff_duration())) {
                break;
            }
        }
    }

    ss::future<bool> loop_sleep(std::chrono::milliseconds duration) {
        simple_time_jitter<ss::lowres_clock> jitter(duration);
        try {
            co_await ss::sleep_abortable<ss::lowres_clock>(
              jitter.next_duration(), _as);
            co_return false;
        } catch (const ss::sleep_aborted& ex) {
            // do nothing, the caller will handle exiting properly.
            std::ignore = ex;
            co_return true;
        }
    }

    ss::future<bool> ensure_domains_replication_factor() {
        auto tp_ns = model::l1_metastore_nt;
        auto rf = _controller->get_topics_state()
                    .local()
                    .get_topic_replication_factor(tp_ns);
        if (!rf) {
            vlog(cd_log.warn, "unable to find {} replication factor", tp_ns);
            co_return false;
        }
        auto target_rf = cluster::replication_factor(
          _controller->internal_topic_replication());
        if (*rf != target_rf) {
            vlog(
              cd_log.info,
              "updating {} replication factor to {}",
              tp_ns,
              target_rf);
            cluster::topic_properties_update update{tp_ns};
            update.custom_properties.replication_factor.op
              = cluster::incremental_update_operation::set;
            update.custom_properties.replication_factor.value = target_rf;
            co_await update_topic(std::move(update));
            co_return false;
        } else {
            vlog(
              cd_log.trace, "replication factor for {} is already set", tp_ns);
            co_return true;
        }
    }

    ss::future<bool>
    create_domains_topic(std::optional<int> num_partitions = std::nullopt) {
        auto tp_ns = model::l1_metastore_nt;
        cluster::topic_properties topic_props;
        // Mark all these as disabled
        topic_props.retention_bytes = tristate<size_t>();
        topic_props.retention_local_target_bytes = tristate<size_t>();
        topic_props.retention_duration = tristate<std::chrono::milliseconds>();
        topic_props.retention_local_target_ms
          = tristate<std::chrono::milliseconds>();
        topic_props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::none;
        co_return co_await create_topic(
          tp_ns,
          num_partitions.value_or(
            config::shard_local_cfg().cloud_topics_num_metastore_partitions()),
          topic_props);
    }

    ss::future<> update_topic(cluster::topic_properties_update update) {
        cluster::errc ec{};
        try {
            auto res = co_await _controller->get_topics_frontend()
                         .local()
                         .update_topic_properties(
                           {update},
                           ss::lowres_clock::now()
                             + config::shard_local_cfg()
                                 .internal_rpc_request_timeout_ms());
            vassert(res.size() == 1, "expected a single result");
            ec = res[0].ec;
        } catch (const std::exception& ex) {
            vlog(
              cd_log.warn, "unable to update topic {}: {}", update.tp_ns, ex);
            co_return;
        }
        if (ec != cluster::errc::success) {
            vlog(
              cd_log.warn, "failed to update topic {}: {}", update.tp_ns, ec);
        }
    }

    ss::future<bool> create_topic(
      model::topic_namespace_view tp_ns,
      int32_t partition_count,
      cluster::topic_properties properties) {
        cluster::topic_configuration topic_cfg(
          tp_ns.ns,
          tp_ns.tp,
          partition_count,
          _controller->internal_topic_replication());
        topic_cfg.properties = properties;

        cluster::errc ec{};
        try {
            auto res = co_await _controller->get_topics_frontend()
                         .local()
                         .autocreate_topics(
                           {std::move(topic_cfg)},
                           config::shard_local_cfg()
                             .internal_rpc_request_timeout_ms());
            vassert(res.size() == 1, "expected a single result");
            ec = res[0].ec;
            // fall through to handle ec value
        } catch (const std::exception& ex) {
            vlog(cd_log.warn, "unable to create topic {}: {}", tp_ns, ex);
            ec = cluster::errc::topic_operation_error;
            co_return false;
        }
        if (
          ec == cluster::errc::success
          || ec == cluster::errc::topic_already_exists) {
            vlog(cd_log.debug, "created topic {}", tp_ns);
            co_return true;
        }
        vlog(cd_log.warn, "failed to create topic {}: {}", tp_ns, ec);
        co_return false;
    }

    ss::future<> reset_domain_manager(
      model::ntp ntp,
      ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>> partition,
      std::optional<model::term_id> expected_term) {
        auto dm_id = domain_manager_id{ntp};
        auto dm_it = _domains.find(dm_id);

        // Unconditionally remove the domain manager if it exists. If it
        // exists, it belongs to an older term and we need to have one domain
        // manager open at a time.
        if (dm_it != _domains.end()) {
            auto dm = std::move(dm_it->second);
            _domains.erase(dm_it);
            auto stop_fut = co_await ss::coroutine::as_future(
              dm->stop_and_wait());
            if (stop_fut.failed()) {
                auto ex = stop_fut.get_exception();
                vlog(
                  cd_log.error,
                  "Removing domain manager {} failed: {}",
                  dm_id,
                  ex);
            }
        }
        if (!partition) {
            co_return;
        }
        vassert(
          expected_term.has_value(),
          "Expected call must be set of partition is set");
        auto raft = partition->get()->raft();
        if (!raft->is_leader() || raft->term() != *expected_term) {
            // Only open a new domain manager if we're still in the expected
            // term. If not, exit early and rely on a later queued call to open
            // the domain manager for it. This helps ensure exactly one domain
            // manager is opened per term, since we're expecting exacly one
            // reset_domain_manager() call per term.
            vlog(
              cd_log.trace,
              "No longer leader of {} term {}, current term: {}, exiting early",
              dm_id,
              *expected_term,
              raft->term());
            co_return;
        }
        vlog(
          cd_log.info,
          "Starting new domain manager for {} in term {}",
          dm_id,
          *expected_term);

        ss::shared_ptr<domain_manager> domain_mgr;
        auto& stm_manager = (*partition)->raft()->stm_manager();
        if (stm_manager->get<stm>()) {
            domain_mgr = ss::make_shared<db_domain_manager>(
              *expected_term,
              stm_manager->get<stm>(),
              _staging_dir,
              _remote,
              _bucket,
              _object_io,
              _sg,
              &_probe);
        } else {
            domain_mgr = ss::make_shared<simple_domain_manager>(
              stm_manager->get<simple_stm>(), _object_io);
        }
        domain_mgr->start();
        _domains.emplace(dm_id, std::move(domain_mgr));
    }

    cluster::controller* _controller;
    io* _object_io;
    std::filesystem::path _staging_dir;
    cloud_io::remote* _remote;
    cloud_storage_clients::bucket_name _bucket;
    ss::scheduling_group _sg;

    // Queue to process async work associated with starting and stopping domain
    // managers when handling partition notifications.
    ssx::work_queue _queue;

    // Container for domain managers, one per leader of L1 metastore topic
    // partition.
    using domain_manager_id = model::ntp;
    chunked_hash_map<domain_manager_id, ss::shared_ptr<domain_manager>>
      _domains;

    domain_manager_probe _probe;

    std::optional<ss::future<>> _loop;
    ss::abort_source _as;
};

domain_supervisor::domain_supervisor(
  cluster::controller* controller,
  io* io,
  std::filesystem::path staging_dir,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  ss::scheduling_group sg)
  : _impl(
      std::make_unique<impl>(
        controller,
        io,
        std::move(staging_dir),
        remote,
        std::move(bucket),
        sg)) {}

domain_supervisor::~domain_supervisor() = default;

ss::future<> domain_supervisor::start() { return _impl->start(); }

ss::future<> domain_supervisor::stop() { return _impl->stop(); }

ss::shared_ptr<domain_manager>
domain_supervisor::get(const model::ntp& ntp) const {
    return _impl->get(ntp);
}

ss::future<bool> domain_supervisor::maybe_create_metastore_topic(
  std::optional<int> num_partitions) {
    return _impl->maybe_create_metastore_topic(num_partitions);
}

void domain_supervisor::on_domain_leadership_change(
  const model::ntp& ntp,
  ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>> partition) {
    _impl->on_domain_leadership_change(ntp, std::move(partition));
}
} // namespace cloud_topics::l1
