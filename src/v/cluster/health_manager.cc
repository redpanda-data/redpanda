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

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "model/namespace.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>

namespace cluster {

health_manager::health_manager(
  model::node_id self,
  size_t target_replication_factor,
  std::chrono::milliseconds tick_interval,
  ss::sharded<topic_table>& topics,
  ss::sharded<topics_frontend>& topics_frontend,
  ss::sharded<partition_allocator>& allocator,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<members_table>& members,
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _target_replication_factor(target_replication_factor)
  , _tick_interval(tick_interval)
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

ss::future<bool> health_manager::ensure_partition_replication(model::ntp ntp) {
    auto assignment = _topics.local().get_partition_assignment(ntp);
    if (!assignment) {
        vlog(
          clusterlog.warn,
          "Health manager: partition {} has no assignment",
          ntp);
        co_return false;
    }

    if (assignment->replicas.size() >= _target_replication_factor) {
        vlog(
          clusterlog.debug,
          "Health manager: partition {} with replication {} reached target {}",
          ntp,
          assignment->replicas.size(),
          _target_replication_factor);
        co_return true;
    }

    partition_constraints constraints(
      ntp.tp.partition, _target_replication_factor);

    auto allocation = _allocator.local().reallocate_partition(
      constraints, *assignment, get_allocation_domain(ntp));
    if (!allocation) {
        vlog(
          clusterlog.warn,
          "Health manager: allocation for {} with replication factor {} "
          "failed: {}",
          ntp,
          _target_replication_factor,
          allocation.error().message());
        co_return false;
    }

    auto new_assignments = allocation.value().get_assignments();

    auto it = std::find_if(
      new_assignments.cbegin(),
      new_assignments.cend(),
      [id = ntp.tp.partition](const auto& a) { return a.id == id; });

    if (it == new_assignments.cend()) {
        vlog(
          clusterlog.warn,
          "Health manager: could not find new allocation for {}",
          ntp);
        co_return false;
    }

    /*
     * TODO: this check that the replicas are on different nodes should be
     * unncessary and removed once the allocation constraints are setup
     * correctly. current the following bug is related to the allocator
     * constraints not being applied properly:
     *
     *    https://github.com/redpanda-data/redpanda/issues/2195
     */
    {
        std::set<model::node_id> nodes;
        for (const auto& r : it->replicas) {
            nodes.insert(r.node_id);
        }
        if (nodes.size() != _target_replication_factor) {
            vlog(
              clusterlog.warn,
              "Health manager: could not satisfy allocation for {}",
              ntp);
            co_return false;
        }
    }

    auto err = co_await _topics_frontend.local().move_partition_replicas(
      ntp, it->replicas, model::timeout_clock::now() + set_replicas_timeout);
    if (err) {
        vlog(
          clusterlog.warn,
          "Health manager: error setting replicas for {}: {}",
          ntp,
          err);
        co_return false;
    }

    vlog(
      clusterlog.info,
      "Increasing replication factor for {} to {}",
      ntp,
      it->replicas.size());

    // short delay for things to stablize
    co_await ss::sleep_abortable(stabilize_delay, _as.local());
    co_return true;
}

ss::future<bool>
health_manager::ensure_topic_replication(model::topic_namespace_view topic) {
    auto metadata = _topics.local().get_topic_metadata(topic);
    if (!metadata) {
        vlog(clusterlog.debug, "Health manager: topic {} not found", topic);
        co_return true;
    }

    for (const auto& partition : metadata->get_assignments()) {
        model::ntp ntp(topic.ns, topic.tp, partition.id);
        if (!co_await ensure_partition_replication(std::move(ntp))) {
            co_return false;
        }
    }

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
    }

    _timer.arm(_tick_interval);
}

} // namespace cluster
