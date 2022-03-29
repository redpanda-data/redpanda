/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "absl/container/btree_map.h"
#include "absl/container/node_hash_map.h"
#include "cluster/types.h"
#include "raft/types.h"

namespace cluster {

/*
 * Leader balancing strategy interface.
 *
 * An implementation will normally accept the current system state as an
 * instance of `index_type` and then be expected to return an assignment via
 * `find_movement` which improves the overall error metric.
 */
class leader_balancer_strategy {
public:
    /*
     * Map a shard to the set of groups whose replica sets contain the shard as
     * a leader of the group. For convenience, the data structure also contains
     * the replica set of each group.
     */
    using index_type = absl::node_hash_map<
      model::broker_shard,
      absl::btree_map<raft::group_id, std::vector<model::broker_shard>>>;

    /*
     * Represent leadership transfer for a group.
     */
    struct reassignment {
        raft::group_id group;
        model::broker_shard from;
        model::broker_shard to;
    };

    /*
     * Leaders per shard.
     */
    struct shard_load {
        model::broker_shard shard;
        size_t leaders;
    };

    /*
     * Compute error for the current leadership configuration.
     *
     * target = num_groups / num_shards
     * error = sum((shard.load - target)^2 for each shard)
     */
    virtual double error() const = 0;

    /*
     * Find a group reassignment that reduces total error.
     */
    virtual std::optional<reassignment>
    find_movement(const absl::flat_hash_set<raft::group_id>& skip) const = 0;

    /*
     * Return current strategy stats.
     */
    virtual std::vector<shard_load> stats() const = 0;
};

} // namespace cluster
