/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/health_manager.h"

#include "base/seastarx.h"
#include "base/vlog.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "model/namespace.h"
#include "model/transform.h"

#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

namespace cluster {

health_manager::health_manager(
  model::node_id self,
  size_t target_replication_factor,
  std::chrono::milliseconds tick_interval,
  config::binding<size_t> max_concurrent_moves,
  ss::sharded<topic_table>& topics,
  ss::sharded<topics_frontend>& topics_frontend,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<members_table>& members,
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _target_replication_factor(target_replication_factor)
  , _tick_jitter(tick_interval)
  , _max_concurrent_moves(std::move(max_concurrent_moves))
  , _topics(topics)
  , _topics_frontend(topics_frontend)
  , _leaders(leaders)
  , _members(members)
  , _as(as) {}

ss::future<> health_manager::start() {
    _as_sub = _as.local().subscribe(
      [this]() noexcept { _reconciliation_executor.request_abort(); });
    submit_reconcile();
    co_return;
}

ss::future<> health_manager::stop() {
    vlog(clusterlog.info, "Stopping Health Manager...");
    co_await _reconciliation_executor.drain();
}

ss::future<bool> health_manager::ensure_topic_replication(
  model::topic_namespace_view topic, ss::abort_source& as) {
    if (as.abort_requested()) {
        co_return false;
    }
    auto tp_metadata = _topics.local().get_topic_metadata_ref(topic);
    if (!tp_metadata.has_value()) {
        vlog(clusterlog.debug, "Health manager: topic {} not found", topic);
        co_return true;
    }

    auto current_replication_factor
      = tp_metadata.value().get().get_replication_factor();
    if (current_replication_factor() >= _target_replication_factor) {
        vlog(
          clusterlog.debug,
          "Health manager: topic {} with replication {} reached target "
          "{}",
          topic,
          current_replication_factor,
          _target_replication_factor);
        co_return true;
    }

    if (
      _topics.local().updates_in_progress().size() >= _max_concurrent_moves()) {
        vlog(
          clusterlog.info,
          "Health manager: max number of reconfigurations reached");
        co_return false;
    }

    topic_properties_update properties_update(model::topic_namespace{topic});
    properties_update.custom_properties.replication_factor.op
      = incremental_update_operation::set;
    properties_update.custom_properties.replication_factor.value
      = replication_factor(_target_replication_factor);
    auto res = co_await _topics_frontend.local().do_update_topic_properties(
      std::move(properties_update),
      model::timeout_clock::now() + set_replicas_timeout);
    if (res.ec != errc::success) {
        vlog(
          clusterlog.warn,
          "Health manager: error updating properties for {}: {}",
          topic,
          res.ec);
        co_return false;
    }

    vlog(clusterlog.info, "Increased replication factor for {}", topic);
    co_return true;
}

void health_manager::submit_reconcile() {
    if (_as.local().abort_requested()) {
        return;
    }
    ssx::background = _reconciliation_executor.submit(
      [this](ss::abort_source& executor_as) {
          return reconcile_loop(executor_as);
      });
}

ss::future<> health_manager::reconcile_loop(ss::abort_source& executor_as) {
    while (!executor_as.abort_requested()) {
        auto next_pass = _tick_jitter.next_duration();
        auto cluster_leader = _leaders.local().get_leader(
          model::controller_ntp);
        if (cluster_leader != _self) {
            vlog(clusterlog.trace, "Health: skipping reconcile as non-leader");
            if (co_await sleep_or_aborted(next_pass, executor_as)) {
                co_return;
            }
            continue;
        }

        try {
            co_await do_reconcile(executor_as);
        } catch (...) {
            auto e = std::current_exception();
            vlog(clusterlog.info, "Health manager caught error {}", e);
        }

        if (co_await sleep_or_aborted(next_pass, executor_as)) {
            co_return;
        }
    }
}

ss::future<> health_manager::do_reconcile(ss::abort_source& as) {
    // Only ensure replication if we have a big enough cluster, to avoid
    // spamming log with replication complaints on single node cluster
    if (_members.local().node_count() < 3) {
        co_return;
    }

    const std::array<model::topic_namespace_view, 8> internal_topics{
      model::kafka_consumer_offsets_nt,
      model::id_allocator_nt,
      model::tx_manager_nt,
      model::schema_registry_nt,
      model::wasm_binaries_nt,
      model::transform_offsets_nt,
      model::kafka_audit_logging_nt,
      model::transform_log_internal_nt,
    };

    std::vector<ss::future<bool>> reconciles;
    reconciles.reserve(internal_topics.size());
    for (auto topic : internal_topics) {
        reconciles.push_back(ensure_topic_replication(topic, as));
    }
    co_await ss::when_all_succeed(reconciles.begin(), reconciles.end());
}

ss::future<bool> health_manager::sleep_or_aborted(
  std::chrono::milliseconds duration, ss::abort_source& executor_as) {
    auto slept = co_await ss::coroutine::as_future(
      ss::sleep_abortable<clock_type>(duration, executor_as));
    if (slept.failed()) {
        slept.ignore_ready_future();
        co_return true;
    }
    co_return false;
}

} // namespace cluster
