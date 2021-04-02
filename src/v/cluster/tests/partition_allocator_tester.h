/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/partition_allocator.h"
#include "random/fast_prng.h"
#include "random/generators.h"

namespace cluster {
struct partition_allocator_tester {
    static constexpr uint32_t max_nodes = 3;
    static constexpr uint32_t cpus_per_node = 10;

    partition_allocator_tester(
      uint32_t nodes = max_nodes, uint32_t cpus = cpus_per_node)
      : pa(raft::group_id(0)) {
        for (auto i = 0; i < nodes; ++i) {
            pa.register_node(std::make_unique<allocation_node>(
              model::node_id(i),
              cpus,
              std::unordered_map<ss::sstring, ss::sstring>()));
        }
    }
    partition_allocator::underlying_t& machines() { return pa._machines; }
    partition_allocator::cil_t& available_machines() {
        return pa._available_machines;
    }
    raft::group_id highest_group() { return pa._highest_group; }
    topic_configuration gen_topic_configuration(
      uint32_t partition_count, uint16_t replication_factor) {
        return topic_configuration(
          model::ns("test_ns"),
          model::topic("w00t"),
          partition_count,
          replication_factor);
    }
    std::vector<model::topic_metadata>
    create_topic_metadata(int topics, int partitions) {
        std::vector<model::topic_metadata> ret;
        for (int t = 0; t < topics; t++) {
            model::topic_metadata t_md(model::topic_namespace(
              model::ns("default"), model::topic(ssx::sformat("topic_{}", t))));
            for (int p = 0; p < partitions; p++) {
                std::vector<model::broker_shard> replicas;
                for (int r = 0; r < max_nodes; r++) {
                    replicas.push_back(
                      {model::node_id(r), _prng() % cpus_per_node});
                }
                model::partition_metadata p_md{model::partition_id(p)};
                p_md.replicas = std::move(replicas);
                p_md.leader_node = model::node_id(_prng() % max_nodes);
                t_md.partitions.push_back(std::move(p_md));
            }
            ret.push_back(std::move(t_md));
        }
        return ret;
    }

    void saturate_all_machines() { pa.test_only_saturate_all_machines(); }
    uint32_t cluster_partition_capacity() const {
        return pa.test_only_max_cluster_allocation_partition_capacity();
    }
    partition_allocator pa;
    fast_prng _prng;
};

} // namespace cluster
