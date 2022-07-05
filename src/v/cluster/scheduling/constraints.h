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

hard_constraint_evaluator not_fully_allocated();
hard_constraint_evaluator is_active();

hard_constraint_evaluator on_node(model::node_id);

hard_constraint_evaluator on_nodes(const std::vector<model::node_id>&);

hard_constraint_evaluator
distinct_from(const std::vector<model::broker_shard>&);

/*
 * constraint checks that new partition won't violate max_disk_usage_ratio
 * partition_size is size of partition that is going to be allocated
 * assigned_reallocation_sizes is sizes of partitions that are going to be
 * allocated on node
 */
hard_constraint_evaluator disk_not_overflowed_by_partition(
  const double max_disk_usage_ratio,
  const size_t partition_size,
  const absl::flat_hash_map<model::node_id, uint64_t>&
    assigned_reallocation_sizes,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports);

soft_constraint_evaluator least_allocated();

/*
 * constraint scores nodes on free disk space
 * assigned_reallocation_sizes is sizes of partitions that are going to be
 * allocated on node
 */
soft_constraint_evaluator least_disk_filled(
  const double max_disk_usage_ratio,
  const absl::flat_hash_map<model::node_id, uint64_t>&
    assigned_reallocation_sizes,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports);

soft_constraint_evaluator
distinct_rack(const std::vector<model::broker_shard>&, const allocation_state&);

} // namespace cluster
