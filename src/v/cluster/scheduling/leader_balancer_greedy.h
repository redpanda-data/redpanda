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
#pragma once

#include "cluster/scheduling/leader_balancer_constraints.h"
#include "cluster/scheduling/leader_balancer_strategy.h"
#include "cluster/scheduling/leader_balancer_types.h"
#include "container/chunked_vector.h"
#include "raft/fundamental.h"

#include <cstddef>
#include <optional>
#include <vector>

namespace cluster::leader_balancer_types {

/// Greedy per-topic balancing strategy.
///
/// For each topic, this strategy computes a target leader assignment by
/// iterating partitions and assigning each leader to the broker and shard with
/// the lowest assigned count for that topic (see build_target_assignment for
/// exact strategy). It then emits moves that converge the current assignment to
/// that target.
class greedy_topic_aware_strategy final : public leader_balancer_strategy {
public:
    greedy_topic_aware_strategy(
      size_t node_count,
      index_type index,
      group_id_to_topic_id group_to_topic,
      absl::flat_hash_set<topic_id_t> internal_topics,
      muted_index muted_index_value,
      std::optional<preference_index> preference_idx);

    double error() const override;

    std::optional<reassignment>
    find_movement(const leader_balancer_types::muted_groups_t& skip) override;

    void apply_movement(const reassignment& reassignment) override;

    std::vector<shard_load> stats() const override;

private:
    struct partition_info {
        raft::group_id group;
        topic_id_t topic;
        model::broker_shard leader;
        std::vector<model::broker_shard> replicas;
    };

    void build_target_assignment();

private:
    muted_index _muted_index;
    group_id_to_topic_id _group_to_topic;
    shard_index _shard_index;
    even_topic_distribution_constraint _topic_distribution_constraint;
    even_shard_load_constraint _shard_load_constraint;

    std::optional<pinning_constraint> _pinning_constraint;

    absl::flat_hash_set<topic_id_t> _internal_topics;

    chunked_vector<reassignment> _pending_moves;
    size_t _next_pending{0};
    size_t _node_count;
};

} // namespace cluster::leader_balancer_types
