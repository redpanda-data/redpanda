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

#include "cluster/scheduling/leader_balancer_strategy.h"
#include "cluster/scheduling/leader_balancer_types.h"

namespace leader_balancer_test_utils {
/**
 * @brief Create an artificial cluster index object with the given
 * number of nodes, shards, groups and replica count.
 */
inline cluster::leader_balancer_strategy::index_type make_cluster_index(
  int node_count,
  unsigned shards_per_node,
  unsigned groups_per_shard,
  unsigned replica_count) {
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

inline cluster::leader_balancer_strategy::index_type copy_cluster_index(
  const cluster::leader_balancer_strategy::index_type& c_index) {
    cluster::leader_balancer_strategy::index_type index;

    for (const auto& [bs, leaders] : c_index) {
        for (const auto& [group_id, replicas] : leaders) {
            index[bs][group_id] = replicas;
        }
    }

    return index;
}

inline cluster::leader_balancer_types::group_id_to_topic_id
make_gid_to_topic_index(
  const cluster::leader_balancer_types::index_type& index) {
    cluster::leader_balancer_types::group_id_to_topic_id ret;

    for (const auto& [bs, leaders] : index) {
        for (const auto& [group, replicas] : leaders) {
            ret.emplace(group, 0);
        }
    }

    return ret;
}

} // namespace leader_balancer_test_utils
