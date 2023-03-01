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
#include "cluster/partition_balancer_types.h"
#include "cluster/scheduling/types.h"
#include "model/metadata.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

class allocation_state;
/**
 * make_soft_constraint adapts hard constraint to soft one by returning
 * max score for nodes that matches the soft constraint and 0 for
 * the ones that not
 */
soft_constraint make_soft_constraint(hard_constraint);

hard_constraint not_fully_allocated();
hard_constraint is_active();

hard_constraint on_node(model::node_id);

hard_constraint on_nodes(const std::vector<model::node_id>&);

hard_constraint distinct_from(const std::vector<model::broker_shard>&);

hard_constraint distinct_nodes();

/*
 * constraint checks that new partition won't violate max_disk_usage_ratio
 * partition_size is size of partition that is going to be allocated
 * assigned_reallocation_sizes is sizes of partitions that are going to be
 * allocated on node
 */
hard_constraint disk_not_overflowed_by_partition(
  const double max_disk_usage_ratio,
  const size_t partition_size,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports);

/*
 * scores nodes based on free overall allocation capacity left
 * returning `0` for fully allocated nodes and `max_capacity` for empty nodes
 */
soft_constraint least_allocated();

/*
 * scores nodes based on allocation capacity used by priority partitions
 * returning `0` for nodes fully allocated for priority partitions
 * and `max_capacity` for nodes without any priority partitions
 * non-priority partition allocations are ignored
 */
soft_constraint least_allocated_in_domain(partition_allocation_domain);

/*
 * constraint scores nodes on free disk space
 * assigned_reallocation_sizes is sizes of partitions that are going to be
 * allocated on node
 */
soft_constraint least_disk_filled(
  const double max_disk_usage_ratio,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports);

hard_constraint distinct_rack(const allocation_state&);

inline soft_constraint distinct_rack_preferred(const allocation_state& state) {
    return make_soft_constraint(distinct_rack(state));
}

} // namespace cluster
