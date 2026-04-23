// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/scheduling/constraints.h"
#include "cluster/tests/partition_balancer_planner_fixture.h"
#include "config/replicas_preference.h"
#include "test_utils/boost_fixture.h"

#include <seastar/testing/thread_test_case.hh>

/*
 * Constraint scoring tests for replica_pinning_preferred.
 *
 * These verify the soft constraint evaluator produces the correct scores:
 *   group 0 = max_score, group k = max_score/(k+1),
 *   rackless = 0, unpreferred = 0.
 */
FIXTURE_TEST(
  test_pinning_constraint_group_zero_scores_max,
  partition_balancer_planner_fixture) {
    allocator_register_nodes(3, {"A", "B", "C"});

    // Preference: A (group 0)
    auto pref = config::replicas_preference::parse("racks: A");

    auto constraint = cluster::replica_pinning_preferred(
      pref, workers.members.local());

    // Create a partition to get a valid allocated_partition reference.
    create_topic("dummy", 1, 1);
    auto ap = workers.allocator.local().make_allocated_partition(
      model::ntp(test_ns, model::topic("dummy"), model::partition_id(0)),
      {model::broker_shard{model::node_id(0), 0}});

    auto evaluator = constraint.make_evaluator(ap, std::nullopt);

    const auto& nodes = workers.allocator.local().state().allocation_nodes();

    // node 0 is on rack A = group 0 => max_score
    auto score_node0 = evaluator(*nodes.at(model::node_id(0)));
    BOOST_REQUIRE_EQUAL(score_node0, cluster::soft_constraint::max_score);
}

FIXTURE_TEST(
  test_pinning_constraint_group_k_scoring, partition_balancer_planner_fixture) {
    allocator_register_nodes(4, {"A", "B", "C", "D"});

    // Preference: A (group 0), {B, C} (group 1), D (group 2)
    auto pref = config::replicas_preference::parse("racks: A, {B, C}, D");

    auto constraint = cluster::replica_pinning_preferred(
      pref, workers.members.local());

    create_topic("dummy", 1, 1);
    auto ap = workers.allocator.local().make_allocated_partition(
      model::ntp(test_ns, model::topic("dummy"), model::partition_id(0)),
      {model::broker_shard{model::node_id(0), 0}});

    auto evaluator = constraint.make_evaluator(ap, std::nullopt);
    const auto& nodes = workers.allocator.local().state().allocation_nodes();

    constexpr auto max_score = cluster::soft_constraint::max_score;

    // group 0 (A) => max_score / 1 = 10,000,000
    auto score_a = evaluator(*nodes.at(model::node_id(0)));
    BOOST_REQUIRE_EQUAL(score_a, max_score);

    // group 1 (B) => max_score / 2 = 5,000,000
    auto score_b = evaluator(*nodes.at(model::node_id(1)));
    BOOST_REQUIRE_EQUAL(score_b, max_score / 2);

    // group 1 (C) => max_score / 2 = 5,000,000 (same group as B)
    auto score_c = evaluator(*nodes.at(model::node_id(2)));
    BOOST_REQUIRE_EQUAL(score_c, max_score / 2);

    // group 2 (D) => max_score / 3 = 3,333,333
    auto score_d = evaluator(*nodes.at(model::node_id(3)));
    BOOST_REQUIRE_EQUAL(score_d, max_score / 3);
}

FIXTURE_TEST(
  test_pinning_constraint_same_group_same_score,
  partition_balancer_planner_fixture) {
    allocator_register_nodes(3, {"B", "C", "A"});

    // Preference: A (group 0), {B, C} (group 1)
    auto pref = config::replicas_preference::parse("racks: A, {B, C}");

    auto constraint = cluster::replica_pinning_preferred(
      pref, workers.members.local());

    create_topic("dummy", 1, 1);
    auto ap = workers.allocator.local().make_allocated_partition(
      model::ntp(test_ns, model::topic("dummy"), model::partition_id(0)),
      {model::broker_shard{model::node_id(0), 0}});

    auto evaluator = constraint.make_evaluator(ap, std::nullopt);
    const auto& nodes = workers.allocator.local().state().allocation_nodes();

    // node 0 = B (group 1), node 1 = C (group 1)
    auto score_b = evaluator(*nodes.at(model::node_id(0)));
    auto score_c = evaluator(*nodes.at(model::node_id(1)));
    BOOST_REQUIRE_EQUAL(score_b, score_c);
}

FIXTURE_TEST(
  test_pinning_constraint_unpreferred_scores_zero,
  partition_balancer_planner_fixture) {
    allocator_register_nodes(3, {"A", "B", "X"});

    auto pref = config::replicas_preference::parse("racks: A, B");

    auto constraint = cluster::replica_pinning_preferred(
      pref, workers.members.local());

    create_topic("dummy", 1, 1);
    auto ap = workers.allocator.local().make_allocated_partition(
      model::ntp(test_ns, model::topic("dummy"), model::partition_id(0)),
      {model::broker_shard{model::node_id(0), 0}});

    auto evaluator = constraint.make_evaluator(ap, std::nullopt);
    const auto& nodes = workers.allocator.local().state().allocation_nodes();

    // node 2 = X, not in preference => 0
    auto score_x = evaluator(*nodes.at(model::node_id(2)));
    BOOST_REQUIRE_EQUAL(score_x, uint64_t{0});

    // node 0 = A (group 0) should still be max_score
    auto score_a = evaluator(*nodes.at(model::node_id(0)));
    BOOST_REQUIRE_EQUAL(score_a, cluster::soft_constraint::max_score);
}

FIXTURE_TEST(
  test_pinning_constraint_rackless_scores_zero,
  partition_balancer_planner_fixture) {
    // Register 2 nodes with racks and 1 without.
    // allocator_register_nodes doesn't support per-node optional racks,
    // so register manually.
    auto& members_table = workers.members.local();

    // node 0: rack_A
    workers.allocator.local().register_node(
      create_allocation_node(model::node_id(0), 4));
    // node 1: no rack
    workers.allocator.local().register_node(
      create_allocation_node(model::node_id(1), 4));

    std::vector<model::broker> brokers;
    brokers.emplace_back(
      model::node_id(0),
      net::unresolved_address{},
      net::unresolved_address{},
      model::rack_id{"A"},
      model::broker_properties{.cores = 4});
    brokers.emplace_back(
      model::node_id(1),
      net::unresolved_address{},
      net::unresolved_address{},
      std::nullopt,
      model::broker_properties{.cores = 4});
    members_table.set_initial_brokers(std::move(brokers));
    last_node_idx = 2;

    auto pref = config::replicas_preference::parse("racks: A");

    auto constraint = cluster::replica_pinning_preferred(
      pref, workers.members.local());

    create_topic("dummy", 1, 1);
    auto ap = workers.allocator.local().make_allocated_partition(
      model::ntp(test_ns, model::topic("dummy"), model::partition_id(0)),
      {model::broker_shard{model::node_id(0), 0}});

    auto evaluator = constraint.make_evaluator(ap, std::nullopt);
    const auto& nodes = workers.allocator.local().state().allocation_nodes();

    // node 0 = A (group 0) => max_score
    auto score_a = evaluator(*nodes.at(model::node_id(0)));
    BOOST_REQUIRE_EQUAL(score_a, cluster::soft_constraint::max_score);

    // node 1 = rackless => 0
    auto score_none = evaluator(*nodes.at(model::node_id(1)));
    BOOST_REQUIRE_EQUAL(score_none, uint64_t{0});
}
