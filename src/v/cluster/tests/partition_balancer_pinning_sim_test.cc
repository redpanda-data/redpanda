// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/commands.h"
#include "cluster/tests/partition_balancer_sim_fixture.h"
#include "config/replicas_preference.h"
#include "test_utils/boost_fixture.h"

#include <seastar/testing/thread_test_case.hh>

struct partition_balancer_pinning_sim_no_rack_fixture
  : partition_balancer_sim_fixture {
    partition_balancer_pinning_sim_no_rack_fixture()
      : partition_balancer_sim_fixture(false) {}
};

// Rack awareness ON: 6 nodes across 4 racks. Preference "racks: A".
// With rack awareness, each partition picks 3 of 4 racks. Some partitions
// will miss rack A entirely, creating pinning violations.
// Expected: balancer repairs all violations by moving replicas to rack A.
FIXTURE_TEST(
  test_replica_pinning_repair_rack_on, partition_balancer_sim_fixture) {
    add_node(model::node_id{0}, 100_GiB, 4, model::rack_id{"A"});
    add_node(model::node_id{1}, 100_GiB, 4, model::rack_id{"B"});
    add_node(model::node_id{2}, 100_GiB, 4, model::rack_id{"B"});
    add_node(model::node_id{3}, 100_GiB, 4, model::rack_id{"C"});
    add_node(model::node_id{4}, 100_GiB, 4, model::rack_id{"C"});
    add_node(model::node_id{5}, 100_GiB, 4, model::rack_id{"D"});

    add_topic("pinned_topic", 30, 3, 10_MiB);

    auto tp_ns = model::topic_namespace(test_ns, model::topic("pinned_topic"));
    cluster::incremental_topic_updates updates;
    updates.replicas_preference.op = cluster::incremental_update_operation::set;
    updates.replicas_preference.value = config::replicas_preference::parse(
      "racks: A");
    dispatch_topic_command(
      cluster::update_topic_properties_cmd{tp_ns, std::move(updates)});

    do_run_balancer();
    auto initial_violations = last_pinning_violations_count();
    logger.info("initial pinning violations: {}", initial_violations);

    BOOST_REQUIRE_GT(initial_violations, 0);

    BOOST_REQUIRE(run_to_completion(500));

    auto final_violations = last_pinning_violations_count();
    logger.info("final pinning violations: {}", final_violations);

    BOOST_REQUIRE_EQUAL(final_violations, 0);
}

// Rack awareness OFF: 4 nodes in rack A, 1 in B, 1 in C.
// Preference "racks: A". With rack awareness OFF, capacity of A = 4 nodes,
// which can hold all RF=3 replicas. Ideal = {A, A, A}.
// Some partitions will have replicas on B/C, creating violations.
// Expected: balancer repairs all violations by moving replicas to A nodes.
FIXTURE_TEST(
  test_replica_pinning_repair_rack_off,
  partition_balancer_pinning_sim_no_rack_fixture) {
    add_node(model::node_id{0}, 100_GiB, 4, model::rack_id{"A"});
    add_node(model::node_id{1}, 100_GiB, 4, model::rack_id{"A"});
    add_node(model::node_id{2}, 100_GiB, 4, model::rack_id{"A"});
    add_node(model::node_id{3}, 100_GiB, 4, model::rack_id{"A"});
    add_node(model::node_id{4}, 100_GiB, 4, model::rack_id{"B"});
    add_node(model::node_id{5}, 100_GiB, 4, model::rack_id{"C"});

    add_topic("pinned_topic", 30, 3, 10_MiB);

    auto tp_ns = model::topic_namespace(test_ns, model::topic("pinned_topic"));
    cluster::incremental_topic_updates updates;
    updates.replicas_preference.op = cluster::incremental_update_operation::set;
    updates.replicas_preference.value = config::replicas_preference::parse(
      "racks: A");
    dispatch_topic_command(
      cluster::update_topic_properties_cmd{tp_ns, std::move(updates)});

    do_run_balancer();
    auto initial_violations = last_pinning_violations_count();
    logger.info("initial pinning violations: {}", initial_violations);

    BOOST_REQUIRE_GT(initial_violations, 0);

    BOOST_REQUIRE(run_to_completion(500));

    auto final_violations = last_pinning_violations_count();
    logger.info("final pinning violations: {}", final_violations);

    BOOST_REQUIRE_EQUAL(final_violations, 0);
}

// Structurally under-capacity: pref `racks: A` with rack-aware ON and only
// one A rack means capacity[0] = 1 while RF = 3. Ideal = [0, unpref, unpref].
// Every initial placement already has an A replica (rack-aware forces 3
// distinct racks and A is the only one available), so actual matches ideal
// on day 1 and no repair moves should be generated. Also exercises the
// "pinning cannot be fully satisfied" warning log path.
FIXTURE_TEST(
  test_replica_pinning_no_spurious_moves_when_under_capacity,
  partition_balancer_sim_fixture) {
    add_node(model::node_id{0}, 100_GiB, 4, model::rack_id{"A"});
    add_node(model::node_id{1}, 100_GiB, 4, model::rack_id{"B"});
    add_node(model::node_id{2}, 100_GiB, 4, model::rack_id{"C"});

    add_topic("pinned_topic", 30, 3, 10_MiB);

    auto tp_ns = model::topic_namespace(test_ns, model::topic("pinned_topic"));
    cluster::incremental_topic_updates updates;
    updates.replicas_preference.op = cluster::incremental_update_operation::set;
    updates.replicas_preference.value = config::replicas_preference::parse(
      "racks: A");
    dispatch_topic_command(
      cluster::update_topic_properties_cmd{tp_ns, std::move(updates)});

    auto plan_data = do_run_balancer();
    BOOST_REQUIRE_EQUAL(plan_data.last_pinning_violations_count, 0);
    BOOST_REQUIRE_EQUAL(plan_data.reassignments.size(), 0);

    // Run a few more ticks to ensure steady-state stays quiet.
    BOOST_REQUIRE(run_to_completion(50));
    BOOST_REQUIRE_EQUAL(last_pinning_violations_count(), 0);
}

// Anti-spurious-move guard: preferred-group capacity is 2 (rack A has 2
// nodes, rack-aware OFF) with RF=3, so ideal = [0, 0, unpref]. Node 1 (the
// second rack-A node) is marked disk-full so the allocator rejects it. For
// partitions that reach a state with exactly one A replica and are asked
// to move a B replica, no strictly-better destination exists -- the other
// A node is either already a replica (distinct-nodes excluded) or
// disk-full (hard-excluded) -- so the allocator would pick a random
// same-rack B peer. The guard should revert that would-be move, leaving
// the partition at its current (still-violating) placement without
// churning replicas.
FIXTURE_TEST(
  test_replica_pinning_guard_blocks_spurious_moves,
  partition_balancer_pinning_sim_no_rack_fixture) {
    add_node(model::node_id{0}, 100_GiB, 4, model::rack_id{"A"});
    add_node(model::node_id{1}, 100_GiB, 4, model::rack_id{"A"});
    add_node(model::node_id{2}, 100_GiB, 4, model::rack_id{"B"});
    add_node(model::node_id{3}, 100_GiB, 4, model::rack_id{"B"});
    add_node(model::node_id{4}, 100_GiB, 4, model::rack_id{"B"});

    add_topic("pinned_topic", 30, 3, 10_MiB);

    auto tp_ns = model::topic_namespace(test_ns, model::topic("pinned_topic"));
    cluster::incremental_topic_updates updates;
    updates.replicas_preference.op = cluster::incremental_update_operation::set;
    updates.replicas_preference.value = config::replicas_preference::parse(
      "racks: A");
    dispatch_topic_command(
      cluster::update_topic_properties_cmd{tp_ns, std::move(updates)});

    // Wedge the second A node: any pinning move toward group 0 must now
    // either land back on the already-present A node (distinct-nodes
    // excluded) or on the disk-full A node (hard-excluded).
    set_node_disk_fill(model::node_id{1}, 0.95);

    // Run to completion. Partitions with zero A replicas can still improve
    // (move one to node 0). Partitions with exactly one A replica cannot
    // improve; the guard must catch the would-be same-rack B-to-B move.
    run_to_completion(500);

    // Steady state: at least one follow-up tick must produce no new
    // reassignments. If the guard were buggy, pinning would keep shuffling
    // B-rack replicas indefinitely and never reach a fixed point.
    auto steady = do_run_balancer();
    BOOST_REQUIRE_EQUAL(steady.reassignments.size(), 0);
    BOOST_REQUIRE_EQUAL(steady.cancellations.size(), 0);
}
