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
#pragma once

#include "cluster/scheduling/leader_balancer_constraints.h"
#include "cluster/scheduling/leader_balancer_strategy.h"
#include "cluster/scheduling/leader_balancer_types.h"
#include "model/metadata.h"

#include <absl/container/flat_hash_map.h>
#include <boost/range/adaptor/reversed.hpp>

#include <limits>
#include <memory>

/*
 * Greedy shard balancer strategy is to move leaders from the most loaded core
 * to the least loaded core. The strategy treats all cores equally, ignoring
 * node-level balancing.
 */
namespace cluster {

class greedy_balanced_shards final : public leader_balancer_strategy {
public:
    explicit greedy_balanced_shards(
      index_type cores, absl::flat_hash_set<model::node_id> muted_nodes)
      : _mi(std::make_unique<leader_balancer_types::muted_index>(
        std::move(muted_nodes), absl::flat_hash_set<raft::group_id>{}))
      , _even_shard_load_c(
          leader_balancer_types::shard_index{std::move(cores)}, *_mi) {}

    double error() const final { return _even_shard_load_c.error(); }

    /*
     * Find a group reassignment that improves overall error. The general
     * approach is to select a group from the highest loaded shard and move
     * leadership for that group to the least loaded shard that the group is
     * compatible with.
     *
     * Clearly this is a costly method in terms of runtime complexity.
     * Measurements for clusters with several thousand partitions indicate a
     * real time execution cost of at most a couple hundred micros. Other
     * strategies are sure to improve this as we tackle larger configurations.
     *
     * Muted nodes are nodes that should be treated as if they have no available
     * capacity. So do not move leadership to a muted node, but any leaders on a
     * muted node should not be touched in case the mute is temporary.
     */
    std::optional<reassignment>
    find_movement(const absl::flat_hash_set<raft::group_id>& skip) final {
        _mi->update_muted_groups(skip);
        return _even_shard_load_c.recommended_reassignment();
    }

    void apply_movement(const reassignment& r) final {
        _even_shard_load_c.update_index(r);
    }

    std::vector<shard_load> stats() const final {
        return _even_shard_load_c.stats();
    }

private:
    std::unique_ptr<leader_balancer_types::muted_index> _mi;
    leader_balancer_types::even_shard_load_constraint _even_shard_load_c;
};

} // namespace cluster
