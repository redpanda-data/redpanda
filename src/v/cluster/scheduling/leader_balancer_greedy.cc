/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/scheduling/leader_balancer_greedy.h"

#include "base/vlog.h"
#include "cluster/logger.h"
#include "container/chunked_vector.h"

#include <map>
#include <tuple>

namespace cluster::leader_balancer_types {

greedy_topic_aware_strategy::greedy_topic_aware_strategy(
  size_t node_count,
  index_type index,
  group_id_to_topic_id group_to_topic,
  absl::flat_hash_set<topic_id_t> internal_topics,
  muted_index muted_index_value,
  std::optional<preference_index> preference_idx)
  : _muted_index(std::move(muted_index_value))
  , _group_to_topic(std::move(group_to_topic))
  , _shard_index(std::move(index))
  , _topic_distribution_constraint(_group_to_topic, _shard_index, _muted_index)
  , _shard_load_constraint(_shard_index, _muted_index)
  , _internal_topics(std::move(internal_topics))
  , _node_count(node_count) {
    if (preference_idx) {
        _pinning_constraint.emplace(
          _group_to_topic, std::move(*preference_idx));
    }
    build_target_assignment();
}

double greedy_topic_aware_strategy::error() const {
    return _shard_load_constraint.error()
           + _topic_distribution_constraint.error();
}

std::optional<leader_balancer_strategy::reassignment>
greedy_topic_aware_strategy::find_movement(
  const leader_balancer_types::muted_groups_t& skip) {
    for (size_t i = _next_pending; i < _pending_moves.size(); ++i) {
        const auto& move = _pending_moves[i];
        if (skip.contains(static_cast<uint64_t>(move.group))) {
            continue;
        }

        if (
          _muted_index.muted_nodes().contains(move.from.node_id)
          || _muted_index.muted_nodes().contains(move.to.node_id)) {
            continue;
        }

        if (_pinning_constraint && _pinning_constraint->evaluate(move) < 0) {
            continue;
        }

        _next_pending = i;
        return move;
    }

    return std::nullopt;
}

void greedy_topic_aware_strategy::apply_movement(
  const reassignment& reassignment) {
    _topic_distribution_constraint.update_index(reassignment);
    _shard_load_constraint.update_index(reassignment);
    _muted_index.update_index(reassignment);
    _shard_index.update_index(reassignment);

    ++_next_pending;
}

std::vector<shard_load> greedy_topic_aware_strategy::stats() const {
    return _shard_load_constraint.stats();
}

// Lower-is-better on every element:
// 1) at_quota: 0 while broker is below its per-topic fair share,
//    1 once reached — guarantees each broker fills to floor_quota
//    before any gets more, so shard preferences in fields 2-3
//    cannot cause broker imbalance.
// 2) fewest topic leaders on this broker-shard
// 3) fewest topic leaders on this broker (tie-break to spread
//    across nodes when shards tie)
// 4) fewest global leaders on this broker-shard
// 5) deterministic tie-break by broker-shard ordering
struct leader_preference {
    size_t at_quota;
    size_t shard_count;
    size_t broker_count;
    size_t global_shard_count;
    model::broker_shard replica;

    auto operator<=>(const leader_preference&) const = default;
};

void greedy_topic_aware_strategy::build_target_assignment() {
    // `partitions_by_topic` is the input to the greedy planner grouped by
    // topic. Each entry stores the raft groups for one topic along with
    // the broker-shards that can legally host leadership for those groups.
    std::map<topic_id_t, chunked_vector<partition_info>> partitions_by_topic;

    // `global_shard_counts` tracks how many target leaders have been
    // assigned to each broker-shard across all topics processed so far.
    // It serves as a cross-topic tie-breaker so that topics processed
    // later inherit shard awareness from earlier ones.
    chunked_hash_map<model::broker_shard, size_t> global_shard_counts;

    for (const auto& [leader, groups] : _shard_index.shards()) {
        for (const auto& [group, replicas] : groups) {
            auto topic_iterator = _group_to_topic.find(group);
            if (topic_iterator == _group_to_topic.end()) {
                vlog(
                  clusterlog.warn, "missing topic mapping for group {}", group);
                continue;
            }
            partitions_by_topic[topic_iterator->second].emplace_back(
              group, topic_iterator->second, leader, replicas);
        }
    }

    // Build targets topic-by-topic. For each topic the greedy walk
    // assigns each partition to the replica that minimises the
    // leader_preference scoring tuple defined above. The at_quota
    // gate guarantees broker balance while shard_count optimises
    // shard placement within that constraint.
    for (auto& [topic, partitions] : partitions_by_topic) {
        bool is_internal = _internal_topics.contains(topic);
        std::ranges::sort(
          partitions, std::ranges::less{}, &partition_info::group);

        // `floor_quota` is the minimum number of leaders every broker
        // should receive from this topic before any broker gets more.
        size_t floor_quota = partitions.size() / _node_count;

        // `assigned_broker_counts` and `assigned_shard_counts` track how
        // many target leaders have been handed out within this topic so
        // far.
        chunked_hash_map<model::node_id, size_t> assigned_broker_counts;
        chunked_hash_map<model::broker_shard, size_t> assigned_shard_counts;

        for (const partition_info& partition : partitions) {
            std::optional<leader_preference> best_assignment;
            for (const model::broker_shard& replica : partition.replicas) {
                size_t broker_count = assigned_broker_counts[replica.node_id];
                auto candidate = leader_preference{
                  .at_quota = broker_count >= floor_quota ? 1u : 0u,
                  .shard_count = assigned_shard_counts[replica],
                  .broker_count = broker_count,
                  .global_shard_count = global_shard_counts[replica],
                  .replica = replica};
                if (!best_assignment || candidate < *best_assignment) {
                    best_assignment = candidate;
                }
            }

            if (!best_assignment.has_value()) {
                // group has no replicas
                continue;
            }
            model::broker_shard selected = best_assignment->replica;
            if (partition.leader != selected) {
                _pending_moves.emplace_back(
                  partition.group, partition.leader, selected);
            }
            assigned_broker_counts[selected.node_id] += 1;
            assigned_shard_counts[selected] += 1;

            // Internal topics (e.g. id_allocator, tx_manager) are
            // excluded from global counts so their fixed placement
            // does not distort the cross-topic balance of user topics.
            if (!is_internal) {
                global_shard_counts[selected] += 1;
            }
        }
    }
}

} // namespace cluster::leader_balancer_types
