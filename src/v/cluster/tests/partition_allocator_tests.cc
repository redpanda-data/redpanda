// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/tests/partition_allocator_fixture.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "raft/types.h"
#include "random/fast_prng.h"
#include "test_utils/fixture.h"

FIXTURE_TEST(register_node, partition_allocator_fixture) {
    register_node(0, 32);
    register_node(1, 64);
    register_node(2, 12);
    BOOST_REQUIRE(allocator.contains_node(model::node_id(0)));
    BOOST_REQUIRE(allocator.contains_node(model::node_id(1)));
    BOOST_REQUIRE(allocator.contains_node(model::node_id(2)));
    BOOST_REQUIRE_EQUAL(allocator.state().available_nodes(), 3);
}

FIXTURE_TEST(unregister_node, partition_allocator_fixture) {
    register_node(0, 32);
    register_node(1, 64);
    register_node(2, 12);
    allocator.unregister_node(model::node_id(1));
    BOOST_REQUIRE(allocator.contains_node(model::node_id(0)));
    BOOST_REQUIRE(!allocator.contains_node(model::node_id(1)));
    BOOST_REQUIRE(allocator.contains_node(model::node_id(2)));
    BOOST_REQUIRE_EQUAL(allocator.state().available_nodes(), 2);
}

FIXTURE_TEST(invalid_allocation_over_capacity, partition_allocator_fixture) {
    register_node(0, 8);
    register_node(1, 4);
    register_node(2, 6);
    cluster::topic_allocation_configuration cfg{
      .partition_count = 1,
      .replication_factor = 1,
    };

    saturate_all_machines();
    auto gr = allocator.state().last_group_id();
    BOOST_REQUIRE(allocator.allocate(cfg).has_error());
    // group id hasn't changed
    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id(), gr);
}

FIXTURE_TEST(max_allocation, partition_allocator_fixture) {
    register_node(0, 2);
    register_node(1, 2);
    register_node(2, 2);
    // available capacity
    // 3 * 7000 * 2 - 3*2 = 41994

    cluster::topic_allocation_configuration cfg{
      .partition_count = max_capacity() / 3,
      .replication_factor = 3,
    };
    auto units = allocator.allocate(cfg).value();

    BOOST_REQUIRE_EQUAL(units.get_assignments().size(), 13998);
    BOOST_REQUIRE_EQUAL(allocated_nodes_count(units.get_assignments()), 41994);
    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), 13998);

    // make sure there is no room left after
    cluster::topic_allocation_configuration single_partition_cfg{
      .partition_count = 1,
      .replication_factor = 1,
    };
    auto result = allocator.allocate(single_partition_cfg);
    BOOST_REQUIRE(result.has_error());
}

FIXTURE_TEST(unsatisfyable_diversity_assignment, partition_allocator_fixture) {
    register_node(0, 8);
    register_node(1, 4);
    register_node(2, 6);
    auto cfg = cluster::topic_allocation_configuration{
      .partition_count = 1,
      .replication_factor = 5,
    };
    auto allocs = allocator.allocate(cfg);
    BOOST_TEST_REQUIRE(allocs.has_error());
    BOOST_REQUIRE_EQUAL(
      cluster::errc(allocs.error().value()),
      cluster::errc::topic_invalid_replication_factor);

    // ensure rollback happened
    BOOST_REQUIRE(all_nodes_empty());

    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), 0);
}
FIXTURE_TEST(partial_assignment, partition_allocator_fixture) {
    register_node(0, 2);
    register_node(1, 2);
    register_node(2, 2);
    auto max_partitions_in_cluster = max_capacity() / 3;
    cluster::topic_allocation_configuration cfg_1{
      .partition_count = max_partitions_in_cluster - 1,
      .replication_factor = 3,
    };
    auto units_1 = allocator.allocate(cfg_1).value();
    BOOST_REQUIRE_EQUAL(
      units_1.get_assignments().size(), max_partitions_in_cluster - 1);

    // allocate 2 partitions - one should fail, returning null & deallocating
    cluster::topic_allocation_configuration cfg_2{
      .partition_count = 2,
      .replication_factor = 3,
    };

    auto units_2 = allocator.allocate(cfg_2);
    BOOST_REQUIRE(units_2.has_error());

    BOOST_REQUIRE_EQUAL(3, max_capacity());
    BOOST_REQUIRE_EQUAL(
      allocator.state().last_group_id()(), max_partitions_in_cluster - 1);
}
FIXTURE_TEST(max_deallocation, partition_allocator_fixture) {
    register_node(0, 3);
    register_node(1, 3);
    register_node(2, 3);
    // This test performs - 209994 partition assignments
    const auto max = max_capacity();

    auto cfg = cluster::topic_allocation_configuration{
      .partition_count = max / 3,
      .replication_factor = 3,
    };
    {
        cluster::allocation_units allocs = allocator.allocate(cfg).value();

        BOOST_REQUIRE_EQUAL(allocs.get_assignments().size() * 3, max);

        BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), max / 3);
    }

    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), max / 3);
    BOOST_REQUIRE_EQUAL(max_capacity(), max);
}

FIXTURE_TEST(recovery_test, partition_allocator_fixture) {
    register_node(0, 3);
    register_node(1, 3);
    register_node(2, 3);
    auto create_replicas = [this](int topics, int partitions) {
        std::vector<model::broker_shard> ret;

        for (int t = 0; t < topics; t++) {
            for (int p = 0; p < partitions; p++) {
                std::vector<model::broker_shard> replicas;
                for (int r = 0; r < 3; r++) {
                    ret.push_back(
                      model::broker_shard{model::node_id(r), prng() % 3});
                }
            }
        }
        return ret;
    };
    // 100 topics with 12 partitions each replicated on 3 nodes each
    auto replicas = create_replicas(100, 12);
    allocator.update_allocation_state(replicas, raft::group_id(0));
    // each node in the cluster holds one replica for each partition,
    // so it has to have topics * partitions shards allocated
    auto allocated_shards = 100 * 12;
    // Remaining capacity on node 0
    BOOST_REQUIRE_EQUAL(
      allocator.state()
        .allocation_nodes()
        .find(model::node_id(0))
        ->second->allocated_partitions(),
      allocated_shards);
    // Remaining capacity on node 1
    BOOST_REQUIRE_EQUAL(
      allocator.state()
        .allocation_nodes()
        .find(model::node_id(1))
        ->second->allocated_partitions(),
      allocated_shards);
    // Remaining capacity on node 2
    BOOST_REQUIRE_EQUAL(
      allocator.state()
        .allocation_nodes()
        .find(model::node_id(2))
        ->second->allocated_partitions(),
      allocated_shards);
}

FIXTURE_TEST(allocation_units_test, partition_allocator_fixture) {
    register_node(1, 10);
    register_node(2, 11);
    register_node(3, 12);
    // just fill up the cluster partially
    auto cfg1 = cluster::topic_allocation_configuration{
      .partition_count = 10,
      .replication_factor = 3,
    };
    {
        auto allocs = allocator.allocate(cfg1).value();
        BOOST_REQUIRE_EQUAL(allocs.get_assignments().size(), 10);
        BOOST_REQUIRE_EQUAL(
          allocated_nodes_count(allocs.get_assignments()), 3 * 10);
    }

    BOOST_REQUIRE(all_nodes_empty());

    // we do not decrement the highest raft group
    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), 10);
}
