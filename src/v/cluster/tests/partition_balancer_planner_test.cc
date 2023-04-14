// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/partition_balancer_planner_fixture.h"
#include "vlog.h"

#include <seastar/testing/thread_test_case.hh>

static ss::logger logger("partition_balancer_planner");

// a shorthand to avoid spelling out model::node_id
static model::node_id n(int64_t id) { return model::node_id{id}; };

using namespace std::chrono_literals;

void check_violations(
  const cluster::partition_balancer_planner::plan_data& plan_data,
  const std::set<size_t>& expected_unavailable,
  const std::set<size_t>& expected_full) {
    std::vector<size_t> actual_unavailable;
    for (const auto& n : plan_data.violations.unavailable_nodes) {
        actual_unavailable.push_back(n.id());
    }
    std::sort(actual_unavailable.begin(), actual_unavailable.end());
    BOOST_REQUIRE_EQUAL(
      std::vector(expected_unavailable.begin(), expected_unavailable.end()),
      actual_unavailable);

    std::vector<size_t> actual_full;
    for (const auto& n : plan_data.violations.full_nodes) {
        actual_full.push_back(n.id);
    }
    std::sort(actual_full.begin(), actual_full.end());
    BOOST_REQUIRE_EQUAL(
      std::vector(expected_full.begin(), expected_full.end()), actual_full);
}

void check_expected_assignments(
  const std::vector<model::broker_shard>& replicas,
  const std::unordered_set<model::node_id>& expected_nodes) {
    BOOST_REQUIRE(replicas.size() == expected_nodes.size());
    for (const auto r : replicas) {
        BOOST_REQUIRE(expected_nodes.contains(r.node_id));
    }
}

/*
 * 4 nodes; 1 topic;
 * Actual
 *   node_0: partitions: 1; down: False; disk: unfilled;
 *   node_1: partitions: 1; down: False; disk: unfilled;
 *   node_2: partitions: 1; down: False; disk: unfilled;
 *   node_3: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: partitions: 1;
 *   node_1: partitions: 1;
 *   node_2: partitions: 1;
 *   node_3: partitions: 0;
 */
FIXTURE_TEST(test_stable, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_stable");
    allocator_register_nodes(4);
    create_topic("topic-1", 1, 3);

    auto hr = create_health_report();
    auto fm = create_follower_metrics();

    auto plan_data = planner.plan_reassignments(hr, fm);
    check_violations(plan_data, {}, {});
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 0);
}

/*
 * 4 nodes; 1 topic; 1 node down
 * Actual
 *   node_0: partitions: 1; down: True; disk: unfilled;
 *   node_1: partitions: 1; down: False; disk: unfilled;
 *   node_2: partitions: 1; down: False; disk: unfilled;
 *   node_3: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: partitions: 0;
 *   node_1: partitions: 1;
 *   node_2: partitions: 1;
 *   node_3: partitions: 1;
 */
FIXTURE_TEST(test_node_down, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_node_down");
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(1);

    auto hr = create_health_report();

    std::set<size_t> unavailable_nodes = {0};
    auto fm = create_follower_metrics(unavailable_nodes);

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, unavailable_nodes, {});

    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(3)});

    auto new_replicas = plan_data.reassignments.front()
                          .allocation_units.get_assignments()
                          .front()
                          .replicas;
    check_expected_assignments(new_replicas, expected_nodes);
}

/*
 * 4 nodes; 1 topic; 2 nodes down
 * Actual
 *   node_0: partitions: 1; down: True; disk: unfilled;
 *   node_1: partitions: 1; down: True; disk: unfilled;
 *   node_2: partitions: 1; down: False; disk: unfilled;
 *   node_3: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: partitions: 1;
 *   node_1: partitions: 1;
 *   node_2: partitions: 1;
 *   node_3: partitions: 0;
 */
FIXTURE_TEST(test_no_quorum_for_partition, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_no_quorum_for_partition");
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(1);

    auto hr = create_health_report();

    std::set<size_t> unavailable_nodes = {0, 1};
    auto fm = create_follower_metrics(unavailable_nodes);

    auto plan_data = planner.plan_reassignments(hr, fm);
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 0);
}

/*
 * 5 nodes; 1 topic; 1 node down; 1 node nearly full
 * Actual
 *   node_0: partitions: 1; down: True; disk: unfilled;
 *   node_1: partitions: 1; down: False; disk: unfilled;
 *   node_2: partitions: 1; down: False; disk: unfilled;
 *   node_3: partitions: 0; down: False; disk: nearly filled;
 *   node_4: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: partitions: 0;
 *   node_1: partitions: 1;
 *   node_2: partitions: 1;
 *   node_3: partitions: 0;
 *   node_4: partitions: 1;
 */
FIXTURE_TEST(
  test_node_down_and_node_is_full_but_there_is_empty,
  partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_node_down_and_node_is_full_but_there_is_empty");
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(2);

    std::set<size_t> nearly_full_nodes = {3};
    auto hr = create_health_report({}, nearly_full_nodes);

    std::set<size_t> unavailable_nodes = {0};
    auto fm = create_follower_metrics(unavailable_nodes);

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, unavailable_nodes, {});

    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(4)});

    auto new_replicas = plan_data.reassignments.front()
                          .allocation_units.get_assignments()
                          .front()
                          .replicas;
    check_expected_assignments(new_replicas, expected_nodes);
}
/*
 * 5 nodes; 1 topic; 1 node down; 1 node nearly full
 * Actual
 *   node_0: partitions: 1; down: True; disk: unfilled;
 *   node_1: partitions: 1; down: False; disk: unfilled;
 *   node_2: partitions: 1; down: False; disk: unfilled;
 *   node_3: partitions: 0; down: False; disk: nearly filled;
 * Expected
 *   node_0: partitions: 0;
 *   node_1: partitions: 1;
 *   node_2: partitions: 1;
 *   node_3: partitions: 1;
 */
FIXTURE_TEST(
  test_node_down_and_node_is_nearly_full, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_node_down_and_node_is_nearly_full");
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(1);

    std::set<size_t> nearly_full_nodes = {3};
    auto hr = create_health_report({}, nearly_full_nodes);

    std::set<size_t> unavailable_nodes = {0};
    auto fm = create_follower_metrics(unavailable_nodes);

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, unavailable_nodes, {});

    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(3)});
    auto new_replicas = plan_data.reassignments.front()
                          .allocation_units.get_assignments()
                          .front()
                          .replicas;
    check_expected_assignments(new_replicas, expected_nodes);
}

/*
 * 4 nodes; 1 topic; 1 node down; 1 node full
 * Actual
 *   node_0: partitions: 1; down: True; disk: unfilled;
 *   node_1: partitions: 1; down: False; disk: unfilled;
 *   node_2: partitions: 1; down: False; disk: unfilled;
 *   node_3: partitions: 0; down: False; disk: full;
 * Expected
 *   node_0: partitions: 1;
 *   node_1: partitions: 1;
 *   node_2: partitions: 1;
 *   node_3: partitions: 0;
 */
FIXTURE_TEST(
  test_node_down_and_node_is_full, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_node_down_and_node_is_full");
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(1);

    std::set<size_t> full_nodes = {3};
    auto hr = create_health_report(full_nodes);

    std::set<size_t> unavailable_nodes = {0};
    auto fm = create_follower_metrics(unavailable_nodes);

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, unavailable_nodes, full_nodes);

    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 0);
}

/*
 * 4 nodes; 1 topic; 1 node full
 * Actual
 *   node_0: partitions: 1; down: False; disk: full;
 *   node_1: partitions: 1; down: False; disk: unfilled;
 *   node_2: partitions: 1; down: False; disk: unfilled;
 *   node_3: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: partitions: 0;
 *   node_1: partitions: 1;
 *   node_2: partitions: 1;
 *   node_3: partitions: 1;
 */
FIXTURE_TEST(test_move_from_full_node, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_move_from_full_node");
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(1);

    std::set<size_t> full_nodes = {0};
    auto hr = create_health_report(full_nodes);

    auto fm = create_follower_metrics();

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, {}, full_nodes);

    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(3)});

    auto new_replicas = plan_data.reassignments.front()
                          .allocation_units.get_assignments()
                          .front()
                          .replicas;
    check_expected_assignments(new_replicas, expected_nodes);
}

/*
 * 4 nodes; 3 topic; 1 node down;
 * Can move only 2 topics by one operation
 * Actual
 *   node_0: partitions: 3; down: True; disk: unfilled;
 *   node_1: partitions: 3; down: False; disk: unfilled;
 *   node_2: partitions: 3; down: False; disk: unfilled;
 *   node_3: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: partitions: 1;
 *   node_1: partitions: 3;
 *   node_2: partitions: 3;
 *   node_3: partitions: 2;
 */
FIXTURE_TEST(
  test_move_multiple_partitions_batched_node_down,
  partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_move_multiple_partitions_with_batch");
    allocator_register_nodes(3);
    create_topic("topic-1", 3, 3);
    allocator_register_nodes(1);

    auto hr = create_health_report();

    std::set<size_t> unavailable_nodes = {0};
    auto fm = create_follower_metrics(unavailable_nodes);

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, unavailable_nodes, {});

    const auto& reassignments = plan_data.reassignments;
    BOOST_REQUIRE_EQUAL(reassignments.size(), 2);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(3)});

    auto new_replicas_1
      = reassignments[0].allocation_units.get_assignments().front().replicas;
    check_expected_assignments(new_replicas_1, expected_nodes);

    auto new_replicas_2
      = reassignments[1].allocation_units.get_assignments().front().replicas;
    check_expected_assignments(new_replicas_2, expected_nodes);
}

/*
 * 6 nodes; 3 topic; 3 node full;
 * Can move only 2 topics by one operation
 * Actual
 *   node_0: partitions: 3; down: False; disk: full;
 *   node_1: partitions: 3; down: False; disk: full;
 *   node_2: partitions: 3; down: False; disk: full;
 *   node_3: partitions: 0; down: False; disk: unfilled;
 *   node_4: partitions: 0; down: False; disk: unfilled;
 *   node_5: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: partitions: 1;
 *   node_1: partitions: 1;
 *   node_2: partitions: 1;
 *   node_3: partitions: 2;
 *   node_4: partitions: 2;
 *   node_5: partitions: 2;
 */
FIXTURE_TEST(
  test_move_multiple_partitions_batched_node_overfill,
  partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_move_multiple_partitions_batched_node_overfill");
    allocator_register_nodes(3);
    create_topic("topic-1", 3, 3);
    allocator_register_nodes(3);

    std::set<size_t> full_nodes = {0, 1, 2};
    auto hr = create_health_report(full_nodes);
    auto fm = create_follower_metrics();

    auto plan_data = planner.plan_reassignments(hr, fm);
    check_violations(plan_data, {}, full_nodes);

    const auto& reassignments = plan_data.reassignments;
    BOOST_REQUIRE_EQUAL(reassignments.size(), 2);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(3), model::node_id(4), model::node_id(5)});

    auto new_replicas_1
      = reassignments[0].allocation_units.get_assignments().front().replicas;
    BOOST_REQUIRE_EQUAL(new_replicas_1.size(), expected_nodes.size());
    check_expected_assignments(new_replicas_1, expected_nodes);

    auto new_replicas_2
      = reassignments[1].allocation_units.get_assignments().front().replicas;
    check_expected_assignments(new_replicas_2, expected_nodes);
}

/*
 * 5 nodes; 1 topic; 1 node down; 1 node full;
 * Move from unavailable node leaves a replica on the full node.
 * Actual
 *   node_0: partitions: 1; down: True; disk: unfilled;
 *   node_1: partitions: 1; down: False; disk: full;
 *   node_2: partitions: 1; down: False; disk: unfilled;
 *   node_3: partitions: 0; down: False; disk: unfilled;
 *   node_4: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: partitions: 0;
 *   node_1: partitions: 1;
 *   node_2: partitions: 1;
 *   node_3: partitions: 1 or 0;
 *   node_4: partitions: 0 or 1;
 */
FIXTURE_TEST(
  test_one_node_down_one_node_full, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_one_node_down_one_node_full");
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(2);

    std::set<size_t> full_nodes = {1};
    auto hr = create_health_report(full_nodes);

    std::set<size_t> unavailable_nodes = {0};
    auto fm = create_follower_metrics(unavailable_nodes);

    auto plan_data = planner.plan_reassignments(hr, fm);
    check_violations(plan_data, unavailable_nodes, full_nodes);

    const auto& reassignments = plan_data.reassignments;
    BOOST_REQUIRE_EQUAL(reassignments.size(), 1);

    auto new_replicas
      = reassignments[0].allocation_units.get_assignments().front().replicas;
    std::unordered_set<model::node_id> new_replicas_set;
    for (const auto& bs : new_replicas) {
        new_replicas_set.insert(bs.node_id);
    }

    std::unordered_set<model::node_id> expected1{
      model::node_id(1), model::node_id(2), model::node_id(3)};
    std::unordered_set<model::node_id> expected2{
      model::node_id(1), model::node_id(2), model::node_id(4)};
    BOOST_REQUIRE_MESSAGE(
      new_replicas_set == expected1 || new_replicas_set == expected2,
      "unexpected new replica set: " << new_replicas);
}

/*
 * 5 nodes; 1 topic; 3 node full;
 * Can move part of replicas
 * Actual
 *   node_0: partitions: 1; down: False; disk: full;
 *   node_1: partitions: 1; down: False; disk: full;
 *   node_2: partitions: 1; down: False; disk: full;
 *   node_3: partitions: 0; down: False; disk: unfilled;
 *   node_4: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: partitions: 1;
 *   node_1: partitions: 0;
 *   node_2: partitions: 0;
 *   node_3: partitions: 1;
 *   node_4: partitions: 1;
 */
FIXTURE_TEST(test_move_part_of_replicas, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_move_part_of_replicas");
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(2);

    std::set<size_t> full_nodes = {0, 1, 2};
    auto hr = create_health_report(full_nodes);

    auto fm = create_follower_metrics();

    // Set order of full nodes
    hr.node_reports[1].local_state.data_disk.free -= 1_MiB;
    hr.node_reports[2].local_state.data_disk.free -= 2_MiB;

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, {}, full_nodes);

    const auto& reassignments = plan_data.reassignments;

    BOOST_REQUIRE_EQUAL(reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(0), model::node_id(3), model::node_id(4)});

    auto new_replicas
      = reassignments[0].allocation_units.get_assignments().front().replicas;
    check_expected_assignments(new_replicas, expected_nodes);
}

/*
 * 3 nodes; 2 topic; 2 node full;
 * Movement should be from more filled node
 * Actual
 *   node_0: topics: topic-1; partitions: 3; down: False; disk: full;
 *   node_1: topics: topic-2; partitions: 1; down: False; disk: full;
 *   node_2: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: topics: topic-1; partitions: 1;
 *   node_1: topics: topic-2; partitions: 1;
 *   node_2: topics: topic-1; partitions: 2;
 */
FIXTURE_TEST(
  test_movement_from_more_filled_node, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_movement_from_more_filled_node");
    allocator_register_nodes(1);
    create_topic("topic-1", 3, 1);
    allocator_register_nodes(1);
    create_topic("topic-2", 1, 1);
    allocator_register_nodes(1);

    std::set<size_t> full_nodes = {0, 1};
    auto hr = create_health_report(full_nodes);
    auto fm = create_follower_metrics();

    // Set order of full nodes
    hr.node_reports[0].local_state.data_disk.free -= 1_MiB;

    // Set partition sizes
    for (auto& topic : hr.node_reports[0].topics) {
        if (topic.tp_ns.tp == "topic-1") {
            for (auto& partition : topic.partitions) {
                if (partition.id == 1) {
                    partition.size_bytes = default_partition_size - 1_KiB;
                }
                if (partition.id == 2) {
                    partition.size_bytes = default_partition_size - 2_KiB;
                }
            }
        }
    }

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, {}, full_nodes);

    const auto& reassignments = plan_data.reassignments;
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 2);
    std::unordered_set<model::node_id> expected_nodes({model::node_id(2)});

    auto new_replicas_1
      = reassignments[0].allocation_units.get_assignments().front().replicas;

    check_expected_assignments(new_replicas_1, expected_nodes);
    // First move less size node
    BOOST_REQUIRE_EQUAL(reassignments[0].ntp.tp.topic, "topic-1");
    BOOST_REQUIRE_EQUAL(reassignments[0].ntp.tp.partition, 2);

    auto new_replicas_2
      = reassignments[1].allocation_units.get_assignments().front().replicas;
    check_expected_assignments(new_replicas_2, expected_nodes);
    BOOST_REQUIRE_EQUAL(reassignments[1].ntp.tp.topic, "topic-1");
    BOOST_REQUIRE_EQUAL(reassignments[1].ntp.tp.partition, 1);
}

/*
 * 5 nodes; 1 topic; 1000 partitions; 1 node down;
 * Partition size 10_KiB
 * Batch size 19_MiB
 * Can move 1900 partitions
 * Actual
 *   node_0: partitions: 2000; down: True; disk: unfilled;
 *   node_1: partitions: 2000; down: False; disk: unfilled;
 *   node_2: partitions: 2000; down: False; disk: unfilled;
 *   node_3: partitions: 0; down: False; disk: unfilled;
 *   node_4: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: partitions: 100;
 *   node_1: partitions: 2000;
 *   node_2: partitions: 2000;
 *   node_3: partitions: 950;
 *   node_4: partitions: 950;
 */
FIXTURE_TEST(test_lot_of_partitions, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_lot_of_partitions");
    allocator_register_nodes(3);
    create_topic("topic-1", 2000, 3);
    allocator_register_nodes(2);

    uint64_t local_partition_size = 10_KiB;
    uint64_t movement_batch_partitions_amount = (reallocation_batch_size
                                                 + local_partition_size - 1)
                                                / local_partition_size;

    auto hr = create_health_report({}, {}, local_partition_size);

    std::set<size_t> unavailable_nodes = {0};
    auto fm = create_follower_metrics(unavailable_nodes);

    auto plan_data = planner.plan_reassignments(hr, fm);
    check_violations(plan_data, unavailable_nodes, {});

    const auto& reassignments = plan_data.reassignments;
    BOOST_REQUIRE_EQUAL(reassignments.size(), movement_batch_partitions_amount);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1),
       model::node_id(2),
       model::node_id(3),
       model::node_id(4)});

    size_t node_3_counter = 0;
    size_t node_4_counter = 0;

    for (auto& reassignment : reassignments) {
        auto new_replicas
          = reassignment.allocation_units.get_assignments().front().replicas;
        BOOST_REQUIRE(new_replicas.size() == 3);
        for (const auto r : new_replicas) {
            BOOST_REQUIRE(expected_nodes.contains(r.node_id));
            if (r.node_id == model::node_id(3)) {
                node_3_counter += 1;
            }
            if (r.node_id == model::node_id(4)) {
                node_4_counter += 1;
            }
        }
    }

    BOOST_REQUIRE_EQUAL(node_3_counter, node_4_counter);
    BOOST_REQUIRE_EQUAL(node_4_counter, movement_batch_partitions_amount / 2);
}

/*
 * 4 nodes; 1 topic; 1+1 node down
 * Node 3 is down after planning
 * Initial
 *   node_0: partitions: 1; down: True; disk: unfilled;
 *   node_1: partitions: 1; down: False; disk: unfilled;
 *   node_2: partitions: 1; down: False; disk: unfilled;
 *   node_3: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: partitions: 1;
 *   node_1: partitions: 1;
 *   node_2: partitions: 1;
 *   node_3: partitions: 0;
 */
FIXTURE_TEST(test_node_cancelation, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_node_cancelation");
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(1);

    auto hr = create_health_report();

    std::set<size_t> unavailable_nodes = {0};
    auto fm = create_follower_metrics(unavailable_nodes);

    auto planner_result = planner.plan_reassignments(hr, fm);

    BOOST_REQUIRE_EQUAL(planner_result.reassignments.size(), 1);

    auto ntp = planner_result.reassignments.front().ntp;

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(3)});

    auto new_replicas = planner_result.reassignments.front()
                          .allocation_units.get_assignments()
                          .front()
                          .replicas;
    check_expected_assignments(new_replicas, expected_nodes);

    for (auto& reassignment : planner_result.reassignments) {
        move_partition_replicas(reassignment);
    }

    hr = create_health_report();

    unavailable_nodes = {0, 3};
    fm = create_follower_metrics(unavailable_nodes);

    planner_result = planner.plan_reassignments(hr, fm);
    BOOST_REQUIRE(planner_result.reassignments.size() == 0);
    BOOST_REQUIRE(planner_result.cancellations.size() == 1);
    BOOST_REQUIRE(planner_result.cancellations.front() == ntp);
}

/*
 * 5 nodes; 1 topic; 1 node down;
 * Rebalaning process should select node_4 according to rack awareness
 * constraint
 * Actual
 *   node_0: partitions: 1; down: True; disk: unfilled; rack_1;
 *   node_1: partitions: 1; down: False; disk: unfilled; rack_2;
 *   node_2: partitions: 1; down: False; disk: unfilled; rack_3;
 *   node_3: partitions: 0; down: False; disk: unfilled; rack_3;
 *   node_4: partitions: 0; down: False; disk: unfilled; rack_4;
 * Expected
 *   node_0: partitions: 0;
 *   node_1: partitions: 1;
 *   node_2: partitions: 1;
 *   node_3: partitions: 0;
 *   node_4: partitions: 1;
 */
FIXTURE_TEST(test_rack_awareness, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_rack_awareness");
    allocator_register_nodes(3, {"rack_1", "rack_2", "rack_3"});
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(2, {"rack_3", "rack_4"});

    auto hr = create_health_report();
    // Make node_4 disk free size less to make partition allocator disk usage
    // constraint prefer node_3 rather than node_4
    hr.node_reports[4].local_state.data_disk.free
      = hr.node_reports[3].local_state.data_disk.free - 10_MiB;

    std::set<size_t> unavailable_nodes = {0};
    auto fm = create_follower_metrics(unavailable_nodes);

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, unavailable_nodes, {});

    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(4)});

    auto new_replicas = plan_data.reassignments.front()
                          .allocation_units.get_assignments()
                          .front()
                          .replicas;
    check_expected_assignments(new_replicas, expected_nodes);
}

/*
 * 4 nodes; 1 topic; 1 node down; 1 node in maintenance
 * Planner should stall and not schedule any movements.
 * Actual
 *   node_0: partitions: 1; down: True;
 *   node_1: partitions: 1; down: False;
 *   node_2: partitions: 1; down: False;
 *   node_3: partitions: 0; down: False; maintenance: True
 */
FIXTURE_TEST(
  test_interaction_with_maintenance, partition_balancer_planner_fixture) {
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(1);

    std::set<size_t> unavailable_nodes{0};
    auto hr = create_health_report();
    auto fm = create_follower_metrics(unavailable_nodes);

    set_maintenance_mode(model::node_id{3});

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, unavailable_nodes, {});
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 0);
    BOOST_REQUIRE(
      plan_data.status
      == cluster::partition_balancer_planner::status::
        waiting_for_maintenance_end);
}

/*
 * 4 nodes; 1 topic; 1 node down; 1 decommissioning node
 * Planner should not schedule any movements to the decommissioning node.
 *   node_0: partitions: 1; down: True;
 *   node_1: partitions: 1; down: False;
 *   node_2: partitions: 1; down: False;
 *   node_3: partitions: 0; down: False; decommissioning: True
 */
FIXTURE_TEST(
  test_interaction_with_decommission_1, partition_balancer_planner_fixture) {
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(1);

    std::set<size_t> unavailable_nodes{0};
    auto hr = create_health_report();
    auto fm = create_follower_metrics(unavailable_nodes);
    set_decommissioning(model::node_id{3});

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, unavailable_nodes, {});
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 0);
    BOOST_REQUIRE_EQUAL(plan_data.failed_reassignments_count, 1);
}

/*
 * 4 nodes; 1 topic; 1 node down; 1 decommissioning node
 * Planner should not cancel any movements from the decommissioning node.
 *   node_0: partitions: 1; down: False; decommissioning: True
 *   node_1: partitions: 1; down: False;
 *   node_2: partitions: 1; down: False;
 *   node_3: partitions: 0; down: True;
 *   movement in progress: (0, 1, 2) -> (1, 2, 3)
 */
FIXTURE_TEST(
  test_interaction_with_decommission_2, partition_balancer_planner_fixture) {
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(1);

    std::set<size_t> unavailable_nodes{3};
    auto hr = create_health_report();
    auto fm = create_follower_metrics(unavailable_nodes);

    set_decommissioning(model::node_id{0});
    move_partition_replicas(
      model::ntp(test_ns, "topic-1", 0),
      {
        model::broker_shard{model::node_id{1}, 0},
        model::broker_shard{model::node_id{2}, 0},
        model::broker_shard{model::node_id{3}, 0},
      });

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, unavailable_nodes, {});
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 0);
    BOOST_REQUIRE_EQUAL(plan_data.cancellations.size(), 0);
    BOOST_REQUIRE_EQUAL(plan_data.failed_reassignments_count, 1);
}

FIXTURE_TEST(
  test_state_ntps_with_broken_rack_constraint,
  partition_balancer_planner_fixture) {
    allocator_register_nodes(4, {"rack_A", "rack_B", "rack_B", "rack_C"});

    model::topic topic{"topic-1"};
    model::ntp ntp0{test_ns, topic, 0};
    model::ntp ntp1{test_ns, topic, 1};

    auto check_ntps = [&](absl::btree_set<model::ntp> expected) {
        const auto& ntps
          = workers.state.local().ntps_with_broken_rack_constraint();
        BOOST_REQUIRE_EQUAL(
          std::vector(ntps.begin(), ntps.end()),
          std::vector(expected.begin(), expected.end()));
    };

    create_topic(topic(), {{n(0), n(1), n(2)}, {n(0), n(1), n(3)}});
    check_ntps({ntp0});

    move_partition_replicas(ntp1, {n(0), n(1), n(2)});
    check_ntps({ntp0, ntp1});

    move_partition_replicas(ntp0, {n(0), n(3), n(2)});
    check_ntps({ntp1});

    cancel_partition_move(ntp0);
    check_ntps({ntp0, ntp1});

    finish_partition_move(ntp1);
    check_ntps({ntp0, ntp1});

    move_partition_replicas(ntp1, {n(0), n(2), n(3)});
    check_ntps({ntp0});

    delete_topic(topic);
    check_ntps({});
}

/*
 * 4 nodes; 1 topic; 2 partitions with 3 replicas in 2 racks;
 * Planner should repair the rack awareness constraint.
 *   node_0: partitions: 2; rack: rack_A;
 *   node_1: partitions: 2; rack: rack_B;
 *   node_2: partitions: 2; rack: rack_B;
 *   node_3: partitions: 0; rack: rack_C;
 */
FIXTURE_TEST(test_rack_awareness_repair, partition_balancer_planner_fixture) {
    allocator_register_nodes(3, {"rack_A", "rack_B", "rack_B"});
    // Partitions will be created with rack constraint violated (because there
    // are only 2 racks)
    create_topic("topic-1", 2, 3);
    allocator_register_nodes(1, {"rack_C"});

    auto hr = create_health_report();
    auto fm = create_follower_metrics({});

    auto plan_data = planner.plan_reassignments(hr, fm);

    check_violations(plan_data, {}, {});
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 2);
    for (const auto& ras : plan_data.reassignments) {
        const auto& new_replicas
          = ras.allocation_units.get_assignments().front().replicas;
        BOOST_REQUIRE_EQUAL(new_replicas.size(), 3);
        absl::node_hash_set<model::rack_id> racks;
        for (const auto& bs : new_replicas) {
            auto rack = workers.members.local().get_node_rack_id(bs.node_id);
            BOOST_REQUIRE(rack);
            racks.insert(*rack);
        }
        BOOST_REQUIRE_EQUAL(racks.size(), 3);
    }
    BOOST_REQUIRE_EQUAL(plan_data.cancellations.size(), 0);
    BOOST_REQUIRE_EQUAL(plan_data.failed_reassignments_count, 0);
}
