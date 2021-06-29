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

#include "cluster/partition_allocation_strategies.h"
#include "cluster/partition_allocator.h"
#include "random/fast_prng.h"
#include "random/generators.h"

struct partition_allocator_fixture {
    partition_allocator_fixture()
      : allocator(cluster::make_allocation_strategy<
                  cluster::round_robin_allocation_strategy>()) {
        cluster::allocation_strategy s = cluster::make_allocation_strategy<
          cluster::round_robin_allocation_strategy>();
    }

    void register_node(int id, int core_count) {
        allocator.register_node(std::make_unique<cluster::allocation_node>(
          model::node_id(id),
          core_count,
          absl::node_hash_map<ss::sstring, ss::sstring>{}));
    }

    void saturate_all_machines() {
        auto total_capacity = std::accumulate(
          allocator.state().allocation_nodes().begin(),
          allocator.state().allocation_nodes().end(),
          0,
          [](int acc, const auto& node) {
              return acc + node.second->partition_capacity();
          });

        auto units = allocator.allocate(cluster::topic_allocation_configuration{
          .partition_count = total_capacity,
          .replication_factor = 1,
        });

        for (auto pas : units.value().get_assignments()) {
            allocator.state().apply_update(pas.replicas, pas.group);
        }
    }

    uint allocated_nodes_count(
      const std::vector<cluster::partition_assignment>& allocs) {
        return std::accumulate(
          allocs.begin(),
          allocs.end(),
          0U,
          [](size_t acc, const cluster::partition_assignment& p_as) {
              return acc + p_as.replicas.size();
          });
    }

    bool all_nodes_empty() {
        return std::all_of(
          allocator.state().allocation_nodes().begin(),
          allocator.state().allocation_nodes().end(),
          [](const auto& n) { return n.second->empty(); });
    }

    int32_t max_capacity() {
        return std::accumulate(
          allocator.state().allocation_nodes().begin(),
          allocator.state().allocation_nodes().end(),
          0,
          [](int acc, auto& n) {
              return acc + n.second->partition_capacity();
          });
    }

    cluster::partition_allocator allocator;

    fast_prng prng;
};
