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

#include "cluster/scheduling/leader_balancer_types.h"

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
    using index_type = leader_balancer_types::index_type;

    virtual ~leader_balancer_strategy() = default;

    /*
     * Represent leadership transfer for a group.
     */
    using reassignment = leader_balancer_types::reassignment;

    /*
     * Leaders per shard.
     */
    using shard_load = leader_balancer_types::shard_load;

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
    find_movement(const leader_balancer_types::muted_groups_t& skip) = 0;

    virtual void apply_movement(const reassignment& reassignment) = 0;

    /*
     * Return current strategy stats.
     */
    virtual std::vector<shard_load> stats() const = 0;
};

} // namespace cluster
