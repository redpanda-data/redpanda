/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/scheduling/leader_balancer_greedy.h"
#include "cluster/scheduling/leader_balancer_types.h"

namespace leader_balancer_test_utils {
/**
 * @brief Create an artificial cluster index object with the given
 * number of nodes, shards, groups and replica count.
 */
static cluster::leader_balancer_strategy::index_type make_cluster_index(
  int node_count,
  int shards_per_node,
  int groups_per_shard,
  int replica_count) {
    cluster::leader_balancer_strategy::index_type index;

    std::vector<model::broker_shard> shards;
    for (auto n = 0; n < node_count; n++) {
        for (auto s = 0U; s < shards_per_node; s++) {
            model::broker_shard shard{model::node_id(n), s};
            shards.push_back(shard);
        }
    }

    size_t replica = 0;
    for (auto shard : shards) {
        for (auto g = 0U; g < groups_per_shard; g++) {
            raft::group_id group(g);
            std::vector<model::broker_shard> replicas;
            replicas.push_back(shard); // the "leader"
            while (replicas.size() != replica_count) {
                if (shards[replica % shards.size()] != shard) {
                    replicas.push_back(shards[replica % shards.size()]);
                }
                ++replica;
            }
            index[shard][group] = std::move(replicas);
        }
    }

    return index;
}

} // namespace leader_balancer_test_utils
