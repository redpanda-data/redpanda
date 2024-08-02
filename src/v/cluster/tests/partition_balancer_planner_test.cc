// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "cluster/controller_snapshot.h"
#include "cluster/health_monitor_types.h"
#include "cluster/tests/partition_balancer_planner_fixture.h"
#include "data_migrated_resources.h"
#include "utils/stable_iterator_adaptor.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

static ss::logger logger("pb_planner_test");

// a shorthand to avoid spelling out model::node_id
static model::node_id n(model::node_id::type id) { return model::node_id{id}; };

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
    populate_node_status_table().get();

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();
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
    populate_node_status_table(unavailable_nodes).get();

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, unavailable_nodes, {});

    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(3)});

    auto new_replicas = plan_data.reassignments.front().allocated.replicas();
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
    populate_node_status_table(unavailable_nodes).get();

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();
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
    populate_node_status_table(unavailable_nodes).get();

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, unavailable_nodes, {});

    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(4)});

    auto new_replicas = plan_data.reassignments.front().allocated.replicas();
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
    populate_node_status_table(unavailable_nodes).get();

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, unavailable_nodes, {});

    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(3)});
    auto new_replicas = plan_data.reassignments.front().allocated.replicas();
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
    populate_node_status_table(unavailable_nodes).get();

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

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

    populate_node_status_table().get();

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, {}, full_nodes);

    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(3)});

    auto new_replicas = plan_data.reassignments.front().allocated.replicas();
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
    populate_node_status_table(unavailable_nodes).get();

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, unavailable_nodes, {});

    const auto& reassignments = plan_data.reassignments;
    BOOST_REQUIRE_EQUAL(reassignments.size(), 2);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(3)});

    auto new_replicas_1 = reassignments[0].allocated.replicas();
    check_expected_assignments(new_replicas_1, expected_nodes);

    auto new_replicas_2 = reassignments[1].allocated.replicas();
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
    populate_node_status_table().get();

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();
    check_violations(plan_data, {}, full_nodes);

    const auto& reassignments = plan_data.reassignments;
    BOOST_REQUIRE_EQUAL(reassignments.size(), 2);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(3), model::node_id(4), model::node_id(5)});

    auto new_replicas_1 = reassignments[0].allocated.replicas();
    BOOST_REQUIRE_EQUAL(new_replicas_1.size(), expected_nodes.size());
    check_expected_assignments(new_replicas_1, expected_nodes);

    auto new_replicas_2 = reassignments[1].allocated.replicas();
    check_expected_assignments(new_replicas_2, expected_nodes);
}

/*
 * 5 nodes; 1 topic; 1 node down; 1 node full;
 * One replica is moved from unavailable node, another from the full one.
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
    populate_node_status_table(unavailable_nodes).get();

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();
    check_violations(plan_data, unavailable_nodes, full_nodes);

    const auto& reassignments = plan_data.reassignments;
    BOOST_REQUIRE_EQUAL(reassignments.size(), 1);

    auto new_replicas = reassignments[0].allocated.replicas();
    std::unordered_set<model::node_id> new_replicas_set;
    for (const auto& bs : new_replicas) {
        new_replicas_set.insert(bs.node_id);
    }

    std::unordered_set<model::node_id> expected{
      model::node_id(2), model::node_id(3), model::node_id(4)};
    BOOST_REQUIRE_MESSAGE(
      new_replicas_set == expected,
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

    populate_node_status_table().get();
    auto nr_1 = hr.node_reports[1]->copy();
    auto nr_2 = hr.node_reports[2]->copy();
    // Set order of full nodes

    nr_1.local_state.log_data_size.value().data_current_size += 1_MiB;
    nr_2.local_state.log_data_size.value().data_current_size += 2_MiB;

    hr.node_reports[1] = ss::make_foreign(
      ss::make_lw_shared<const cluster::node_health_report>(std::move(nr_1)));
    hr.node_reports[2] = ss::make_foreign(
      ss::make_lw_shared<const cluster::node_health_report>(std::move(nr_2)));

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, {}, full_nodes);

    const auto& reassignments = plan_data.reassignments;

    BOOST_REQUIRE_EQUAL(reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(0), model::node_id(3), model::node_id(4)});

    auto new_replicas = reassignments[0].allocated.replicas();
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
    populate_node_status_table().get();
    auto nr_0 = hr.node_reports[0]->copy();

    // Set order of full nodes
    nr_0.local_state.log_data_size.value().data_current_size += 1_MiB;

    // Set partition sizes
    for (auto& [tp_ns, partitions] : nr_0.topics) {
        if (tp_ns.tp == "topic-1") {
            for (auto& partition : partitions) {
                if (partition.id == 1) {
                    partition.size_bytes = default_partition_size - 1_KiB;
                }
                if (partition.id == 2) {
                    partition.size_bytes = default_partition_size - 2_KiB;
                }
            }
        }
    }

    hr.node_reports[0] = ss::make_foreign(
      ss::make_lw_shared<const cluster::node_health_report>(std::move(nr_0)));
    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, {}, full_nodes);

    const auto& reassignments = plan_data.reassignments;
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 2);

    std::unordered_set<model::node_id> expected_nodes({model::node_id(2)});
    for (const auto& reassignment : reassignments) {
        BOOST_REQUIRE_EQUAL(reassignment.ntp.tp.topic, "topic-1");
        check_expected_assignments(
          reassignment.allocated.replicas(), expected_nodes);
        auto partition = reassignment.ntp.tp.partition;
        BOOST_REQUIRE_MESSAGE(
          partition == 1 || partition == 2,
          "unexpected partition: " << partition);
    }
}

/*
 * 5 nodes; 1 topic; 1000 partitions; 1 node down;
 * Partition size 10_KiB
 * Batch size 19_MiB
 * Can move 50 (max_concurrent_actions) partitions
 * Actual
 *   node_0: partitions: 2000; down: True; disk: unfilled;
 *   node_1: partitions: 2000; down: False; disk: unfilled;
 *   node_2: partitions: 2000; down: False; disk: unfilled;
 *   node_3: partitions: 0; down: False; disk: unfilled;
 *   node_4: partitions: 0; down: False; disk: unfilled;
 * Expected
 *   node_0: partitions: 1950;
 *   node_1: partitions: 2000;
 *   node_2: partitions: 2000;
 *   node_3: partitions: 25;
 *   node_4: partitions: 25;
 */
FIXTURE_TEST(test_lot_of_partitions, partition_balancer_planner_fixture) {
    vlog(logger.debug, "test_lot_of_partitions");
    allocator_register_nodes(3);
    create_topic("topic-1", 2000, 3);
    allocator_register_nodes(2);

    uint64_t local_partition_size = 10_KiB;
    auto hr = create_health_report({}, {}, local_partition_size);

    std::set<size_t> unavailable_nodes = {0};
    populate_node_status_table(unavailable_nodes).get();

    const size_t max_concurrent_actions = 50;
    auto planner = make_planner(
      model::partition_autobalancing_mode::continuous, max_concurrent_actions);
    auto plan_data = planner.plan_actions(hr, as).get();
    check_violations(plan_data, unavailable_nodes, {});

    const auto& reassignments = plan_data.reassignments;
    BOOST_REQUIRE_EQUAL(reassignments.size(), max_concurrent_actions);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1),
       model::node_id(2),
       model::node_id(3),
       model::node_id(4)});

    size_t node_3_counter = 0;
    size_t node_4_counter = 0;

    for (auto& reassignment : reassignments) {
        auto new_replicas = reassignment.allocated.replicas();
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
    BOOST_REQUIRE_EQUAL(node_4_counter, max_concurrent_actions / 2);
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
    populate_node_status_table(unavailable_nodes).get();

    auto planner = make_planner();
    auto planner_result = planner.plan_actions(hr, as).get();

    BOOST_REQUIRE_EQUAL(planner_result.reassignments.size(), 1);

    auto ntp = planner_result.reassignments.front().ntp;

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(3)});

    auto new_replicas
      = planner_result.reassignments.front().allocated.replicas();
    check_expected_assignments(new_replicas, expected_nodes);

    for (auto& reassignment : planner_result.reassignments) {
        move_partition_replicas(reassignment);
    }

    hr = create_health_report();

    unavailable_nodes = {0, 3};
    populate_node_status_table(unavailable_nodes).get();

    planner_result = planner.plan_actions(hr, as).get();
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
    auto nr_4 = hr.node_reports[4]->copy();
    nr_4.local_state.log_data_size.value().data_current_size
      = hr.node_reports[3]->local_state.log_data_size.value().data_current_size
        - 10_MiB;

    hr.node_reports[4] = ss::make_foreign(
      ss::make_lw_shared<const cluster::node_health_report>(std::move(nr_4)));

    std::set<size_t> unavailable_nodes = {0};
    populate_node_status_table(unavailable_nodes).get();

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, unavailable_nodes, {});

    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(4)});

    auto new_replicas = plan_data.reassignments.front().allocated.replicas();
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
    populate_node_status_table(unavailable_nodes).get();

    set_maintenance_mode(model::node_id{3});

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, unavailable_nodes, {});
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(3)});

    auto new_replicas = plan_data.reassignments.front().allocated.replicas();
    check_expected_assignments(new_replicas, expected_nodes);
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
    populate_node_status_table(unavailable_nodes).get();
    set_decommissioning(model::node_id{3});

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, unavailable_nodes, {});
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 0);
    BOOST_REQUIRE_EQUAL(plan_data.failed_actions_count, 1);
}

/*
 * 5 nodes; 1 topic; 1 node down; 1 decommissioning node
 * Planner should schedule a movement away from the decommissioning node,
 * taking the unavailable node into account.
 *   node_0: partitions: 1; down: False; decommissioning: True
 *   node_1: partitions: 1; down: False;
 *   node_2: partitions: 1; down: False;
 *   node_3: partitions: 0; down: True;
 *   node_4: partitions: 0; down: False;
 */
FIXTURE_TEST(
  test_interaction_with_decommission_2, partition_balancer_planner_fixture) {
    allocator_register_nodes(3);
    create_topic("topic-1", 1, 3);
    allocator_register_nodes(2);

    std::set<size_t> unavailable_nodes{3};
    auto hr = create_health_report();
    populate_node_status_table(unavailable_nodes).get();

    set_decommissioning(model::node_id{0});

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, unavailable_nodes, {});
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 1);

    std::unordered_set<model::node_id> expected_nodes(
      {model::node_id(1), model::node_id(2), model::node_id(4)});
    auto new_replicas = plan_data.reassignments.front().allocated.replicas();
    check_expected_assignments(new_replicas, expected_nodes);

    BOOST_REQUIRE_EQUAL(plan_data.cancellations.size(), 0);
    BOOST_REQUIRE_EQUAL(plan_data.failed_actions_count, 0);
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
    populate_node_status_table().get();

    auto planner = make_planner();
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, {}, {});
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 2);
    for (const auto& ras : plan_data.reassignments) {
        const auto& new_replicas = ras.allocated.replicas();
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
    BOOST_REQUIRE_EQUAL(plan_data.failed_actions_count, 0);
}

FIXTURE_TEST(balancing_modes, partition_balancer_planner_fixture) {
    allocator_register_nodes(3, {"rack_A", "rack_B", "rack_B"});
    create_topic("topic-1", 2, 3);
    allocator_register_nodes(1, {"rack_C"});

    std::set<size_t> full_nodes = {2};
    auto hr = create_health_report(full_nodes);

    std::set<size_t> unavailable_nodes = {1};
    populate_node_status_table(unavailable_nodes).get();

    auto planner = make_planner(model::partition_autobalancing_mode::node_add);
    auto plan_data = planner.plan_actions(hr, as).get();

    check_violations(plan_data, {}, {});
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 0);
    BOOST_REQUIRE_EQUAL(plan_data.cancellations.size(), 0);
    BOOST_REQUIRE_EQUAL(plan_data.failed_actions_count, 0);
}

FIXTURE_TEST(
  concurrent_topic_table_updates, partition_balancer_planner_fixture) {
    // Apply lots of topic_table update commands, while concurrently invoking
    // the planner. The main goal of this test is to pass ASan checks.

    allocator_register_nodes(5);
    config::shard_local_cfg().disable_metrics.set_value(true);
    config::shard_local_cfg().disable_public_metrics.set_value(true);

    auto make_create_tp_cmd = [this](ss::sstring name, int partitions) {
        int16_t replication_factor = 3;
        cluster::topic_configuration cfg(
          test_ns, model::topic{name}, partitions, replication_factor);

        ss::chunked_fifo<cluster::partition_assignment> assignments;
        for (model::partition_id::type i = 0; i < partitions; ++i) {
            std::vector<model::broker_shard> replicas;
            for (int r = 0; r < replication_factor; ++r) {
                replicas.push_back(model::broker_shard{
                  model::node_id{r},
                  random_generators::get_int<uint32_t>(0, 3)});
            }
            std::shuffle(
              replicas.begin(),
              replicas.end(),
              random_generators::internal::gen);

            assignments.push_back(cluster::partition_assignment{
              raft::group_id{1}, model::partition_id{i}, replicas});
        }
        return cluster::create_topic_cmd{
          make_tp_ns(name),
          cluster::topic_configuration_assignment(cfg, std::move(assignments))};
    };

    size_t successes = 0;
    size_t failures = 0;
    size_t reassignments = 0;
    bool should_stop = false;
    ss::future<> planning_fiber = ss::async([&] {
        while (!should_stop) {
            vlog(logger.trace, "planning fiber: invoking...");
            auto hr = create_health_report();
            auto planner = make_planner(
              model::partition_autobalancing_mode::node_add, 50, true);

            try {
                auto plan_data = planner.plan_actions(hr, as).get();
                successes += 1;
                reassignments += plan_data.reassignments.size();
            } catch (concurrent_modification_error&) {
                failures += 1;
            }
            vlog(logger.trace, "planning fiber: iteration done");
        }
    });
    auto deferred = ss::defer([&] {
        if (!should_stop) {
            should_stop = true;
            planning_fiber.get();
        }
    });
    cluster::data_migrations::migrated_resources migrated_resources;
    cluster::topic_table other_tt(migrated_resources);
    model::offset controller_offset{0};
    std::set<ss::sstring> cur_topics;
    bool node_isolated = false;

    for (size_t iter = 0; iter < 1'000; ++iter) {
        int random_val = random_generators::get_int(0, 10);
        if (random_val == 10) {
            // allow the planner to make some progress
            ss::sleep(50ms).get();
            continue;
        }

        // randomly create and delete topics
        auto topic = ssx::sformat("topic_{}", random_val);
        if (!cur_topics.contains(topic)) {
            vlog(
              logger.trace,
              "modifying fiber: creating topic {} (isolated: {})",
              topic,
              node_isolated);
            auto cmd = make_create_tp_cmd(
              topic, random_generators::get_int(1, 20));
            other_tt.apply(cmd, controller_offset).get();
            if (!node_isolated) {
                workers.dispatch_topic_command(cmd);
            }
            cur_topics.insert(topic);
        } else {
            vlog(
              logger.trace,
              "modifying fiber: deleting topic {} (isolated: {})",
              topic,
              node_isolated);
            cluster::delete_topic_cmd cmd{make_tp_ns(topic), make_tp_ns(topic)};
            other_tt.apply(cmd, controller_offset).get();
            if (!node_isolated) {
                workers.dispatch_topic_command(cmd);
            }
            cur_topics.erase(topic);
        }

        if (random_generators::get_int(5) == 0) {
            // flip node_isolated flag

            if (node_isolated) {
                // simulate node coming back from isolation and recovering
                // current controller state from a snapshot.
                vlog(logger.trace, "modifying fiber: applying snapshot");
                node_isolated = false;
                cluster::controller_snapshot snap;
                other_tt.fill_snapshot(snap).get();
                workers.members.local().fill_snapshot(snap);
                workers.dispatcher.apply_snapshot(controller_offset, snap)
                  .get();
            } else {
                node_isolated = true;
            }
        }

        controller_offset += 1;

        vlog(logger.trace, "modifying fiber: iteration done");
    }

    should_stop = true;
    planning_fiber.get();

    // sanity-check that planning made some progress.
    BOOST_REQUIRE(successes > 0);
    BOOST_REQUIRE(failures > 0);
    BOOST_REQUIRE(reassignments > 0);
}
