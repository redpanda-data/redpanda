#define BOOST_TEST_MODULE cluster

#include "cluster/tests/partition_allocator_tester.h"

#include <boost/test/unit_test.hpp>

BOOST_FIXTURE_TEST_CASE(register_node, cluster::partition_allocator_tester) {
    BOOST_REQUIRE_EQUAL(machines().size(), 3);
    BOOST_REQUIRE_EQUAL(highest_group()(), 0);
}
BOOST_FIXTURE_TEST_CASE(
  invalid_allocation, cluster::partition_allocator_tester) {
    auto cfg = gen_topic_configuration(1, 1);
    saturate_all_machines();
    BOOST_REQUIRE(std::nullopt == pa.allocate(cfg));
    BOOST_REQUIRE_EQUAL(highest_group()(), 0);
}
BOOST_FIXTURE_TEST_CASE(max_allocation, cluster::partition_allocator_tester) {
    // This test performs - 209994 partition assignments

    using ts = cluster::partition_allocator_tester;
    const auto max = (ts::cpus_per_node
                      * cluster::allocation_node::max_allocations_per_core)
                     - cluster::allocation_node::core0_extra_weight;
    auto cfg = gen_topic_configuration(max, ts::max_nodes);
    std::vector<cluster::partition_assignment> allocs = std::move(
      pa.allocate(cfg).value());

    BOOST_REQUIRE_EQUAL(max * ts::max_nodes, 209994);
    BOOST_REQUIRE_EQUAL(allocs.size(), 209994);
    BOOST_REQUIRE_EQUAL(highest_group()(), max);

    // make sure there is no room left after
    auto one_topic_cfg = gen_topic_configuration(1, 1);
    BOOST_REQUIRE(std::nullopt == pa.allocate(one_topic_cfg));
}
BOOST_FIXTURE_TEST_CASE(
  unsatisfyable_diversity_assignment, cluster::partition_allocator_tester) {
    using ts = cluster::partition_allocator_tester;
    auto cfg = gen_topic_configuration(1, ts::max_nodes + 1);
    auto allocs = pa.allocate(cfg);
    BOOST_REQUIRE(std::nullopt == allocs);

    // ensure rollback happened
    const auto max_cluster_capacity
      = ((ts::cpus_per_node
          * cluster::allocation_node::max_allocations_per_core)
         - cluster::allocation_node::core0_extra_weight)
        * ts::max_nodes;

    BOOST_REQUIRE_EQUAL(max_cluster_capacity, cluster_partition_capacity());
    BOOST_REQUIRE_EQUAL(highest_group()(), 0);
}
BOOST_FIXTURE_TEST_CASE(
  partial_assignment, cluster::partition_allocator_tester) {
    using ts = cluster::partition_allocator_tester;
    const auto max_partitions
      = (ts::cpus_per_node * cluster::allocation_node::max_allocations_per_core)
        - cluster::allocation_node::core0_extra_weight;

    const auto expected_usage_capacity = (max_partitions * ts::max_nodes) - 3;

    const auto max_correct_partitions = max_partitions - 1;

    // just fill up the cluster partially
    auto cfg1 = gen_topic_configuration(max_correct_partitions, ts::max_nodes);
    auto allocs1 = pa.allocate(cfg1);
    BOOST_REQUIRE_EQUAL(allocs1.value().size(), expected_usage_capacity);

    // allocate 2 partitions - one should fail, returning null & deallocating
    auto cfg = gen_topic_configuration(2, ts::max_nodes);
    auto allocs = pa.allocate(cfg);
    BOOST_REQUIRE(std::nullopt == allocs);

    BOOST_REQUIRE_EQUAL(3, cluster_partition_capacity());
    BOOST_REQUIRE_EQUAL(highest_group()(), max_correct_partitions);
}
BOOST_FIXTURE_TEST_CASE(max_deallocation, cluster::partition_allocator_tester) {
    // This test performs - 209994 partition assignments
    using ts = cluster::partition_allocator_tester;
    const auto max = (ts::cpus_per_node
                      * cluster::allocation_node::max_allocations_per_core)
                     - cluster::allocation_node::core0_extra_weight;

    auto cfg = gen_topic_configuration(max, ts::max_nodes);
    std::vector<cluster::partition_assignment> allocs = std::move(
      pa.allocate(cfg).value());

    BOOST_REQUIRE_EQUAL(max * ts::max_nodes, 209994);
    BOOST_REQUIRE_EQUAL(allocs.size(), 209994);
    BOOST_REQUIRE_EQUAL(highest_group()(), max);
    BOOST_REQUIRE_EQUAL(available_machines().size(), 0);

    // make sure there is no room left after
    auto one_topic_cfg = gen_topic_configuration(1, 1);
    BOOST_REQUIRE(std::nullopt == pa.allocate(one_topic_cfg));

    // now deallocate them all, and we _must_ not decrease the raft count
    for (auto& as : allocs) {
        pa.deallocate(as);
    }

    BOOST_REQUIRE_EQUAL(highest_group()(), max);
    BOOST_REQUIRE_EQUAL(available_machines().size(), 3);
    BOOST_REQUIRE_EQUAL(cluster_partition_capacity(), 209994);
}

BOOST_AUTO_TEST_CASE(round_robin_load) {
    cluster::partition_allocator_tester test(5, 10);
    auto cfg = test.gen_topic_configuration(100, 3);
    std::vector<cluster::partition_assignment> allocs = std::move(
      test.pa.allocate(cfg).value());
    std::map<model::node_id, int> node_assignment;
    for (auto& a : allocs) {
        node_assignment[a.broker.id()]++;
    }
    for (auto& p : node_assignment) {
        BOOST_REQUIRE_EQUAL(p.second, 60);
    }
}
