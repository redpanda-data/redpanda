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

    // Reconcile promptly on leadership/membership changes; runs are gated on
    // controller leadership, so notifications on non-leaders are no-ops.
    _leadership_notification_id
      = _leaders.local().register_leadership_change_notification(
        model::controller_ntp,
        [this](const model::ntp&, model::term_id, model::node_id) {
            submit_reconcile();
        });
    _members_notification_id
      = _members.local().register_members_updated_notification(
        [this](model::node_id, model::membership_state) {
            submit_reconcile();
        });
    submit_reconcile();
    co_return;
}

ss::future<> health_manager::stop() {
    vlog(clusterlog.info, "Stopping Health Manager...");
    _members.local().unregister_members_updated_notification(
      _members_notification_id);
    _leaders.local().unregister_leadership_change_notification(
      model::controller_ntp, _leadership_notification_id);
    // drain() aborts the in-flight task, aborts any enqueued task, blocks new
    // submits, and waits for both to resolve.
    co_await _reconciliation_executor.drain();
}

ss::future<> health_manager::ensure_topic_replication(
  model::topic_namespace_view topic, ss::abort_source& as) {
    while (!as.abort_requested()) {
        auto tp_metadata = _topics.local().get_topic_metadata_ref(topic);
        if (!tp_metadata.has_value()) {
            vlog(clusterlog.debug, "Health manager: topic {} not found", topic);
            co_return;
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
            co_return;
        }

        if (
          _topics.local().updates_in_progress().size()
          >= _max_concurrent_moves()) {
            vlog(
              clusterlog.info,
              "Health manager: max number of reconfigurations reached");
            co_return;
        }

        topic_properties_update properties_update(
          model::topic_namespace{topic});
        properties_update.custom_properties.replication_factor.op
          = incremental_update_operation::set;
        properties_update.custom_properties.replication_factor.value
          = replication_factor(_target_replication_factor);
        auto res = co_await _topics_frontend.local().do_update_topic_properties(
          std::move(properties_update),
          model::timeout_clock::now() + set_replicas_timeout);
        if (res.ec == errc::success) {
            vlog(clusterlog.info, "Increased replication factor for {}", topic);
            co_return;
        }

        // Right after cluster bootstrap the target replicas may not be
        // allocatable yet (brokers are still joining), so the update fails
        // transiently. Waiting for the next reconcile tick could leave an
        // internal topic born at a reduced replication factor (e.g. the
        // cloud-topics metastore, created r=1 before a quorum of brokers
        // joined) under-replicated for minutes. Retry promptly instead.
        if (
          res.ec == errc::no_eligible_allocation_nodes
          || res.ec == errc::topic_invalid_replication_factor) {
            vlog(
              clusterlog.info,
              "Health manager: transient error increasing replication factor "
              "for {}: {}; retrying",
              topic,
              res.ec);
            if (co_await sleep_or_aborted(std::chrono::seconds{1}, as)) {
                co_return;
            }
            continue;
        }

        vlog(
          clusterlog.warn,
          "Health manager: error updating properties for {}: {}",
          topic,
          res.ec);
        co_return;
    }
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
            co_return;
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

    const std::array<model::topic_namespace_view, 9> internal_topics{
      model::kafka_consumer_offsets_nt,
      model::id_allocator_nt,
      model::tx_manager_nt,
      model::schema_registry_nt,
      model::wasm_binaries_nt,
      model::transform_offsets_nt,
      model::kafka_audit_logging_nt,
      model::transform_log_internal_nt,
      model::l1_metastore_nt,
    };

    std::vector<ss::future<>> reconciles;
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
