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
            model::broker bi(
              model::node_id(_prng()),
              random_generators::gen_alphanum_string(10),
              _prng(),
              std::nullopt);
            pa.register_node(std::make_unique<allocation_node>(
              std::move(bi), cpus, std::unordered_map<sstring, sstring>()));
        }
    }
    partition_allocator::underlying_t& machines() {
        return pa._machines;
    }
    partition_allocator::cil_t& available_machines() {
        return pa._available_machines;
    }
    raft::group_id highest_group() {
        return pa._highest_group;
    }
    topic_configuration gen_topic_configuration(
      uint32_t partition_count, uint16_t replication_factor) {
        return topic_configuration(
          model::ns("test_ns"),
          model::topic("w00t"),
          partition_count,
          replication_factor);
    }
    void saturate_all_machines() {
        pa.test_only_saturate_all_machines();
    }
    uint32_t cluster_partition_capacity() const {
        return pa.test_only_max_cluster_allocation_partition_capacity();
    }
    partition_allocator pa;
    fast_prng _prng;
};

} // namespace cluster
