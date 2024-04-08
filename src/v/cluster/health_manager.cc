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
#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "model/namespace.h"
#include "model/transform.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>

namespace cluster {

health_manager::health_manager(
  model::node_id self,
  size_t target_replication_factor,
  std::chrono::milliseconds tick_interval,
  config::binding<size_t> max_concurrent_moves,
  ss::sharded<topic_table>& topics,
  ss::sharded<topics_frontend>& topics_frontend,
  ss::sharded<partition_allocator>& allocator,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<members_table>& members,
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _target_replication_factor(target_replication_factor)
  , _tick_interval(tick_interval)
  , _max_concurrent_moves(std::move(max_concurrent_moves))
  , _topics(topics)
  , _topics_frontend(topics_frontend)
  , _allocator(allocator)
  , _leaders(leaders)
  , _members(members)
  , _as(as)
  , _timer([this] { tick(); }) {}

ss::future<> health_manager::start() {
    _timer.arm(_tick_interval);
    co_return;
}

ss::future<> health_manager::stop() {
    vlog(clusterlog.info, "Stopping Health Manager...");
    _timer.cancel();
    return _gate.close();
}

ss::future<bool>
health_manager::ensure_topic_replication(model::topic_namespace_view topic) {
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
    // short delay for things to stablize
    co_await ss::sleep_abortable(stabilize_delay, _as.local());
    co_return true;
}

void health_manager::tick() {
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return do_tick();
                      }).handle_exception([this](const std::exception_ptr& e) {
        vlog(clusterlog.info, "Health manager caught error {}", e);
        _timer.arm(_tick_interval * 2);
    });
}

ss::future<> health_manager::do_tick() {
    auto cluster_leader = _leaders.local().get_leader(model::controller_ntp);
    if (cluster_leader != _self) {
        vlog(clusterlog.trace, "Health: skipping tick as non-leader");
        co_return;
    }

    // Only ensure replication if we have a big enough cluster, to avoid
    // spamming log with replication complaints on single node cluster
    if (_members.local().node_count() >= 3) {
        /*
         * we try to be conservative here. if something goes wrong we'll
         * back off and wait before trying to fix replication for any
         * other internal topics.
         */
        auto ok = co_await ensure_topic_replication(
          model::kafka_consumer_offsets_nt);

        if (ok) {
            ok = co_await ensure_topic_replication(model::id_allocator_nt);
        }

        if (ok) {
            ok = co_await ensure_topic_replication(model::tx_manager_nt);
        }

        if (ok) {
            const model::topic_namespace schema_registry_nt{
              model::kafka_namespace, model::schema_registry_internal_tp.topic};
            ok = co_await ensure_topic_replication(schema_registry_nt);
        }

        if (ok) {
            ok = co_await ensure_topic_replication(
              model::topic_namespace_view(model::wasm_binaries_internal_ntp));
        }

        if (ok) {
            ok = co_await ensure_topic_replication(model::transform_offsets_nt);
        }

        if (ok) {
            ok = co_await ensure_topic_replication(
              model::kafka_audit_logging_nt);
        }

        if (ok) {
            ok = co_await ensure_topic_replication(
              model::transform_log_internal_nt);
        }
    }

    _timer.arm(_tick_interval);
}

} // namespace cluster
