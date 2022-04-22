// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_dissemination_utils.h"

#include "cluster/types.h"
#include "likely.h"
#include "model/metadata.h"

#include <fmt/core.h>

namespace cluster {

std::vector<model::node_id> calculate_non_overlapping_nodes(
  const partition_assignment& partition_members,
  const std::vector<model::node_id>& all_nodes) {
    std::vector<model::node_id> non_overlapping;
    non_overlapping.reserve(
      all_nodes.size() - partition_members.replicas.size());
    for (auto& n : all_nodes) {
        const bool contains = std::find_if(
                                partition_members.replicas.begin(),
                                partition_members.replicas.end(),
                                [n](const model::broker_shard& bs) {
                                    return bs.node_id == n;
                                })
                              != partition_members.replicas.end();

        if (!contains) {
            // This node is not a partition member
            non_overlapping.push_back(n);
        }
    }
    return non_overlapping;
}

} // namespace cluster
