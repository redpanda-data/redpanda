
#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"

namespace cluster {

// Calculate vector of nodes that belongs to the cluster but are not partition
// replica set members
std::vector<model::node_id> calculate_non_overlapping_nodes(
  const std::vector<model::node_id>& partition_members,
  const std::vector<model::node_id>& all_nodes);

// Returns a vector of nodes that are members of partition replica set
std::vector<model::node_id> get_partition_members(
  model::partition_id pid, const model::topic_metadata& tp_md);

} // namespace cluster