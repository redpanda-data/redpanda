// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_dissemination_utils.h"

#include "likely.h"

#include <fmt/core.h>

namespace cluster {

std::vector<model::node_id> calculate_non_overlapping_nodes(
  const std::vector<model::node_id>& partition_members,
  const std::vector<model::node_id>& all_nodes) {
    std::vector<model::node_id> non_overlapping;
    non_overlapping.reserve(all_nodes.size() - partition_members.size());
    for (auto& n : all_nodes) {
        const bool contains = std::find(
                                std::cbegin(partition_members),
                                std::cend(partition_members),
                                n)
                              != std::cend(partition_members);
        if (!contains) {
            // This node is not a partition member
            non_overlapping.push_back(n);
        }
    }
    return non_overlapping;
}

std::vector<model::node_id> get_partition_members(
  model::partition_id pid, const model::topic_metadata& tp_md) {
    std::vector<model::node_id> members;
    if (unlikely((size_t)pid() >= tp_md.partitions.size())) {
        throw std::invalid_argument(fmt::format(
          "Topic {} does not contain partion {}", tp_md.tp_ns, pid));
    }
    auto& replicas = tp_md.partitions[pid()].replicas;
    std::transform(
      std::cbegin(replicas),
      std::cend(replicas),
      std::back_inserter(members),
      [](model::broker_shard bs) { return bs.node_id; });

    return members;
}
} // namespace cluster