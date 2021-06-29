// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/partition_allocator_fixture.h"
#include "cluster/types.h"
#include "raft/types.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/perf_tests.hh>

#include <vector>

PERF_TEST_F(partition_allocator_fixture, allocation_3) {
    register_node(0, 24);
    register_node(1, 24);
    register_node(2, 24);
    auto cfg = cluster::topic_allocation_configuration{
      .partition_count = 1,
      .replication_factor = 3,
    };

    perf_tests::start_measuring_time();
    auto vals = allocator.allocate(cfg);
    perf_tests::do_not_optimize(vals);
    perf_tests::stop_measuring_time();
}
PERF_TEST_F(partition_allocator_fixture, deallocation_3) {
    register_node(0, 24);
    register_node(1, 24);
    register_node(2, 24);
    auto cfg = cluster::topic_allocation_configuration{
      .partition_count = 1,
      .replication_factor = 3,

    };
    std::vector<model::broker_shard> replicas;
    {
        auto vals = std::move(allocator.allocate(cfg).value());
        replicas = vals.get_assignments().front().replicas;
        allocator.update_allocation_state(
          vals.get_assignments().front().replicas,
          vals.get_assignments().front().group);
    }
    perf_tests::do_not_optimize(replicas);
    perf_tests::start_measuring_time();
    allocator.deallocate(replicas);
    perf_tests::stop_measuring_time();
}
PERF_TEST_F(partition_allocator_fixture, recovery) {
    register_node(0, 24);
    register_node(1, 24);
    register_node(2, 24);
    const auto node_capacity = max_capacity();

    std::vector<model::broker_shard> replicas;

    for (auto i = 0; i < node_capacity; ++i) {
        replicas.push_back(model::broker_shard{
          .node_id = model::node_id(i % 3), .shard = uint32_t(i) % 24});
    }

    perf_tests::start_measuring_time();
    allocator.update_allocation_state(
      replicas, raft::group_id(replicas.size() / 3));
    perf_tests::stop_measuring_time();
}
