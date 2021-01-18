// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition_allocator.h"
#include "cluster/tests/partition_allocator_tester.h"
#include "raft/types.h"
#include "test_utils/fixture.h"

using namespace cluster; // NOLINT

uint allocated_nodes_count(const std::vector<partition_assignment>& allocs) {
    return std::accumulate(
      allocs.begin(),
      allocs.end(),
      0U,
      [](size_t acc, const cluster::partition_assignment& p_as) {
          return acc + p_as.replicas.size();
      });
}

FIXTURE_TEST(register_node, partition_allocator_tester) {
    BOOST_REQUIRE_EQUAL(machines().size(), 3);
    BOOST_REQUIRE_EQUAL(highest_group()(), 0);
}

FIXTURE_TEST(unregister_node, partition_allocator_tester) {
    pa.unregister_node(model::node_id(1));
    BOOST_REQUIRE_EQUAL(machines().size(), 2);
    BOOST_REQUIRE_EQUAL(available_machines().size(), 2);
}

FIXTURE_TEST(decommission_node, partition_allocator_tester) {
    pa.decommission_node(model::node_id(1));
    BOOST_REQUIRE_EQUAL(machines().size(), 3);
    // only two of machines are available as one of them is decommissioned
    BOOST_REQUIRE_EQUAL(available_machines().size(), 2);
}

FIXTURE_TEST(test_decommissioned_realloc, partition_allocator_tester) {
    auto cfg = gen_topic_configuration(1, 3);
    partition_allocator::allocation_units allocs = pa.allocate(cfg).value();

    pa.decommission_node(model::node_id(2));
    BOOST_REQUIRE_EQUAL(machines().size(), 3);
    // only two of machines are available as one of them is decommissioned
    BOOST_REQUIRE_EQUAL(available_machines().size(), 2);
    auto assignment = *allocs.get_assignments().begin();

    // reallocate
    auto first_attempt = pa.reassign_decommissioned_replicas(assignment);
    // first attempt should fail, there are not enough nodes to allocate
    // replicas (requested rf = 3, while we have 2 nodes)
    BOOST_REQUIRE_EQUAL(first_attempt.has_value(), false);

    pa.register_node(std::make_unique<allocation_node>(
      model::node_id(10), 8, std::unordered_map<ss::sstring, ss::sstring>()));

    auto second_attempt = pa.reassign_decommissioned_replicas(assignment);
    //  second attempt should be successfull
    BOOST_REQUIRE_EQUAL(second_attempt.has_value(), true);
    BOOST_REQUIRE_EQUAL(second_attempt->get_assignments().size(), 1);
    BOOST_REQUIRE_EQUAL(
      second_attempt->get_assignments().begin()->replicas.size(), 3);
}

FIXTURE_TEST(
  test_decommissioned_realloc_single_replica, partition_allocator_tester) {
    auto cfg = gen_topic_configuration(1, 1);
    partition_allocator::allocation_units allocs = pa.allocate(cfg).value();

    pa.decommission_node(model::node_id(1));
    BOOST_REQUIRE_EQUAL(machines().size(), 3);
    // only two of machines are available as one of them is decommissioned
    BOOST_REQUIRE_EQUAL(available_machines().size(), 2);
    auto assignment = *allocs.get_assignments().begin();

    // reallocate

    auto reallocated = pa.reassign_decommissioned_replicas(assignment);
    // first attempt should be successful as we have 2 nodes left and requested
    // rf = 1
    BOOST_REQUIRE_EQUAL(reallocated.has_value(), true);
    BOOST_REQUIRE_EQUAL(reallocated->get_assignments().size(), 1);
    BOOST_REQUIRE_EQUAL(
      reallocated->get_assignments().begin()->replicas.size(), 1);
}

FIXTURE_TEST(invalid_allocation, partition_allocator_tester) {
    auto cfg = gen_topic_configuration(1, 1);
    saturate_all_machines();
    BOOST_REQUIRE(std::nullopt == pa.allocate(cfg));
    BOOST_REQUIRE_EQUAL(highest_group()(), 0);
}
FIXTURE_TEST(max_allocation, partition_allocator_tester) {
    // This test performs - 209994 partition assignments

    using ts = partition_allocator_tester;
    const auto max = (ts::cpus_per_node
                      * allocation_node::max_allocations_per_core)
                     - allocation_node::core0_extra_weight;
    auto cfg = gen_topic_configuration(max, ts::max_nodes);
    partition_allocator::allocation_units allocs = pa.allocate(cfg).value();

    BOOST_REQUIRE_EQUAL(max * ts::max_nodes, 209994);
    BOOST_REQUIRE_EQUAL(allocs.get_assignments().size() * 3, 209994);
    BOOST_REQUIRE_EQUAL(
      allocated_nodes_count(allocs.get_assignments()), 209994);
    BOOST_REQUIRE_EQUAL(highest_group()(), max);

    // make sure there is no room left after
    auto one_topic_cfg = gen_topic_configuration(1, 1);
    BOOST_REQUIRE(std::nullopt == pa.allocate(one_topic_cfg));
}
FIXTURE_TEST(unsatisfyable_diversity_assignment, partition_allocator_tester) {
    using ts = partition_allocator_tester;
    auto cfg = gen_topic_configuration(1, ts::max_nodes + 1);
    auto allocs = pa.allocate(cfg);
    BOOST_REQUIRE(std::nullopt == allocs);

    // ensure rollback happened
    const auto max_cluster_capacity
      = ((ts::cpus_per_node * allocation_node::max_allocations_per_core)
         - allocation_node::core0_extra_weight)
        * ts::max_nodes;

    BOOST_REQUIRE_EQUAL(max_cluster_capacity, cluster_partition_capacity());
    BOOST_REQUIRE_EQUAL(highest_group()(), 0);
}
FIXTURE_TEST(partial_assignment, partition_allocator_tester) {
    using ts = partition_allocator_tester;
    const auto max_partitions = (ts::cpus_per_node
                                 * allocation_node::max_allocations_per_core)
                                - allocation_node::core0_extra_weight;

    const auto expected_usage_capacity = (max_partitions * ts::max_nodes) - 3;

    const auto max_correct_partitions = max_partitions - 1;

    // just fill up the cluster partially
    auto cfg1 = gen_topic_configuration(max_correct_partitions, ts::max_nodes);
    auto allocs1 = pa.allocate(cfg1).value();
    BOOST_REQUIRE_EQUAL(
      allocs1.get_assignments().size() * 3, expected_usage_capacity);
    BOOST_REQUIRE_EQUAL(
      allocated_nodes_count(allocs1.get_assignments()),
      expected_usage_capacity);

    // allocate 2 partitions - one should fail, returning null & deallocating
    auto cfg = gen_topic_configuration(2, ts::max_nodes);
    auto allocs = pa.allocate(cfg);
    BOOST_REQUIRE(std::nullopt == allocs);

    BOOST_REQUIRE_EQUAL(3, cluster_partition_capacity());
    BOOST_REQUIRE_EQUAL(highest_group()(), max_correct_partitions);
}
FIXTURE_TEST(max_deallocation, partition_allocator_tester) {
    // This test performs - 209994 partition assignments
    using ts = partition_allocator_tester;
    const auto max = (ts::cpus_per_node
                      * allocation_node::max_allocations_per_core)
                     - allocation_node::core0_extra_weight;

    auto cfg = gen_topic_configuration(max, ts::max_nodes);
    partition_allocator::allocation_units allocs = pa.allocate(cfg).value();

    BOOST_REQUIRE_EQUAL(max * ts::max_nodes, 209994);
    BOOST_REQUIRE_EQUAL(
      allocs.get_assignments().size() * ts::max_nodes, 209994);
    BOOST_REQUIRE_EQUAL(
      allocated_nodes_count(allocs.get_assignments()), 209994);
    BOOST_REQUIRE_EQUAL(highest_group()(), max);
    BOOST_REQUIRE_EQUAL(available_machines().size(), 0);

    // make sure there is no room left after
    auto one_topic_cfg = gen_topic_configuration(1, 1);
    BOOST_REQUIRE(std::nullopt == pa.allocate(one_topic_cfg));

    // now deallocate them all, and we _must_ not decrease the raft count
    for (auto& as : allocs.get_assignments()) {
        for (auto& bs : as.replicas) {
            pa.deallocate(bs);
        }
    }

    BOOST_REQUIRE_EQUAL(highest_group()(), max);
    BOOST_REQUIRE_EQUAL(available_machines().size(), 3);
    BOOST_REQUIRE_EQUAL(cluster_partition_capacity(), 209994);
}

FIXTURE_TEST(recovery_test, partition_allocator_tester) {
    using ts = partition_allocator_tester;
    const auto max = (ts::cpus_per_node
                      * allocation_node::max_allocations_per_core)
                     - allocation_node::core0_extra_weight;

    // 100 topics with 12 partitions each replicated on 3 nodes each
    auto md = create_topic_metadata(100, 12);
    pa.update_allocation_state(md, raft::group_id(0));
    // each node in the cluster holds one replica for each partition,
    // so it has to have topics * partitions shards allocated
    auto allocated_shards = 100 * 12;
    // Remaining capacity on node 0
    BOOST_REQUIRE_EQUAL(
      max - machines().at(model::node_id(0))->partition_capacity(),
      allocated_shards);
    // Remaining capacity on node 1
    BOOST_REQUIRE_EQUAL(
      max - machines().at(model::node_id(1))->partition_capacity(),
      allocated_shards);
    // Remaining capacity on node 2
    BOOST_REQUIRE_EQUAL(
      max - machines().at(model::node_id(2))->partition_capacity(),
      allocated_shards);
}

BOOST_AUTO_TEST_CASE(round_robin_load) {
    partition_allocator_tester test(5, 10);
    auto cfg = test.gen_topic_configuration(100, 3);
    std::vector<partition_assignment> allocs
      = test.pa.allocate(cfg).value().get_assignments();
    std::map<model::node_id, int> node_assignment;
    for (auto& a : allocs) {
        for (auto& bs : a.replicas) {
            node_assignment[bs.node_id]++;
        }
    }
    for (auto& p : node_assignment) {
        BOOST_REQUIRE_EQUAL(p.second, 60);
    }
}

FIXTURE_TEST(allocation_units_test, partition_allocator_tester) {
    using ts = partition_allocator_tester;
    const auto partitions = 128;
    const auto max = (ts::cpus_per_node
                      * allocation_node::max_allocations_per_core)
                     - allocation_node::core0_extra_weight;
    // just fill up the cluster partially
    auto cfg1 = gen_topic_configuration(partitions, ts::max_nodes);
    {
        auto allocs = pa.allocate(cfg1).value();
        BOOST_REQUIRE_EQUAL(
          allocs.get_assignments().size() * 3, ts::max_nodes * partitions);
        BOOST_REQUIRE_EQUAL(
          allocated_nodes_count(allocs.get_assignments()),
          ts::max_nodes * partitions);
    }
    // after allocs left the scope we deallocate
    BOOST_REQUIRE_EQUAL(
      machines().at(model::node_id(0))->partition_capacity(), max);
    BOOST_REQUIRE_EQUAL(
      machines().at(model::node_id(1))->partition_capacity(), max);
    BOOST_REQUIRE_EQUAL(
      machines().at(model::node_id(2))->partition_capacity(), max);
    // we do not decrement the highest raft group
    BOOST_REQUIRE_EQUAL(highest_group()(), partitions);
}
