// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/partition_balancer_sim_fixture.h"
#include "test_utils/boost_fixture.h"

#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;

FIXTURE_TEST(test_decommission, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 4; ++i) {
        add_node(model::node_id{i}, 100_GiB);
    }
    add_topic("mytopic", 100, 3, 100_MiB);
    set_decommissioning(model::node_id{0});

    BOOST_REQUIRE(run_to_completion(100));
    BOOST_REQUIRE_EQUAL(
      allocation_nodes().at(model::node_id{0})->allocated_partitions()(), 0);
}

FIXTURE_TEST(test_two_decommissions, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 5; ++i) {
        add_node(model::node_id{i}, 100_GiB);
    }
    add_topic("mytopic", 200, 3, 100_MiB);
    set_decommissioning(model::node_id{0});

    size_t start_node_0_replicas
      = nodes().at(model::node_id{0}).replicas.size();
    auto tick_cb = [&] {
        if (
          !allocation_nodes().at(model::node_id{1})->is_decommissioned()
          && allocation_nodes().at(model::node_id{0})->final_partitions()()
               < start_node_0_replicas / 2) {
            logger.info(
              "start decommissioning node 1 after {} ticks", cur_tick());
            set_decommissioning(model::node_id{1});
            print_state();
        }
    };

    BOOST_REQUIRE(run_to_completion(500, tick_cb));
    BOOST_REQUIRE_EQUAL(
      allocation_nodes().at(model::node_id{0})->allocated_partitions()(), 0);
    BOOST_REQUIRE_EQUAL(
      allocation_nodes().at(model::node_id{1})->allocated_partitions()(), 0);
}

FIXTURE_TEST(test_counts_rebalancing, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 3; ++i) {
        add_node(model::node_id{i}, 1000_GiB, 4);
    }

    for (int i = 0; i < 10; ++i) {
        add_topic(
          ssx::sformat("topic_{}", i),
          random_generators::get_int(20, 100),
          3,
          100_MiB);
    }

    add_node(model::node_id{3}, 1000_GiB, 4);
    add_node_to_rebalance(model::node_id{3});
    add_node(model::node_id{4}, 1000_GiB, 8);
    add_node_to_rebalance(model::node_id{4});

    BOOST_REQUIRE(run_to_completion(2000));
    validate_even_replica_distribution();
    validate_even_topic_distribution();
}

FIXTURE_TEST(
  test_heterogeneous_racks_full_disk, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 3; ++i) {
        add_node(
          model::node_id{i},
          1000_GiB,
          4,
          model::rack_id{ssx::sformat("rack_{}", i)});
    }
    add_node(model::node_id{3}, 1000_GiB, 4, model::rack_id{"rack_0"});

    add_topic("topic_1", 50, 3, 18_GiB);

    // Nodes 0 and 3 are in rack_0 and 1 and 2 are each in racks of their own.
    // We expect 1 and 2 to go over 80% disk limit and the balancer to fix this
    // (even though some rack constraint violations are introduced).

    BOOST_REQUIRE(run_to_completion(100));
    for (const auto& [id, node] : nodes()) {
        BOOST_REQUIRE(double(node.used) / node.total < 0.8);
    }
}

FIXTURE_TEST(test_smol, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 4; ++i) {
        add_node(model::node_id{i}, 100_GiB);
    }

    add_topic("topic_1", 3, 3, 1_GiB, 100_MiB);
    add_topic("topic_2", 3, 3, 1_GiB, 100_MiB);

    for (model::node_id::type i = 4; i < 6; ++i) {
        add_node(model::node_id{i}, 100_GiB);
        add_node_to_rebalance(model::node_id{i});
    }

    BOOST_REQUIRE(run_to_completion(10));
}

FIXTURE_TEST(test_heterogeneous_topics, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 9; ++i) {
        add_node(model::node_id{i}, 300_GiB);
    }

    // Add 2 topics with drastically different partition sizes.
    // We expect the result to be nevertheless balanced thanks to topic-aware
    // balancing.
    add_topic("topic_1", 200, 3, 2_GiB, 200_MiB);
    add_topic("topic_2", 800, 3, 10_MiB, 1_MiB);

    for (model::node_id::type i = 9; i < 12; ++i) {
        add_node(model::node_id{i}, 300_GiB);
        add_node_to_rebalance(model::node_id{i});
    }

    BOOST_REQUIRE(run_to_completion(1000));

    validate_even_topic_distribution();
    validate_even_replica_distribution();
}

FIXTURE_TEST(test_many_topics, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 4; ++i) {
        add_node(model::node_id{i}, 100_GiB);
    }

    for (size_t i = 0; i < 100; ++i) {
        // Many topics, each with just a few partitions - this is the hard case
        // for topic-aware balancing. We expect overall replica distribution to
        // be even anyway.

        auto n_partitions = random_generators::get_int(2, 5);

        // take mean topic partition sizes from a bimodal distribution - 20% of
        // the topics will be big.
        auto partition_size = random_generators::get_int(0, 4) == 0 ? 1_GiB
                                                                    : 10_MiB;

        add_topic(
          ssx::sformat("topic_{}", i),
          n_partitions,
          3,
          partition_size,
          partition_size / 10);
    }

    for (model::node_id::type i = 4; i < 6; ++i) {
        add_node(model::node_id{i}, 100_GiB);
        add_node_to_rebalance(model::node_id{i});
    }

    BOOST_REQUIRE(run_to_completion(1000));

    validate_even_replica_distribution();
}

FIXTURE_TEST(test_replica_pair_frequency, partition_balancer_sim_fixture) {
    for (model::node_id::type i = 0; i < 3; ++i) {
        add_node(model::node_id{i}, 300_GiB);
    }
    add_topic("topic_1", 150, 3, 1_GiB);

    for (model::node_id::type i = 3; i < 6; ++i) {
        add_node(model::node_id{i}, 300_GiB);
        add_node_to_rebalance(model::node_id{i});
    }

    add_topic("topic_2", 150, 3, 1_GiB);
    logger.info("topic_2 created");
    validate_topic_replica_pair_frequencies("topic_2");

    BOOST_REQUIRE(run_to_completion(1000));
    logger.info("first rebalance finished");
    validate_even_replica_distribution();
    validate_replica_pair_frequencies();

    for (model::node_id::type i = 6; i < 9; ++i) {
        add_node(model::node_id{i}, 300_GiB);
        add_node_to_rebalance(model::node_id{i});
    }

    BOOST_REQUIRE(run_to_completion(1000));
    logger.info("second rebalance finished");
    validate_even_replica_distribution();
    validate_replica_pair_frequencies();
}

FIXTURE_TEST(test_mixed_replication_factors, partition_balancer_sim_fixture) {
    add_node(model::node_id{0}, 100_GiB, 1);
    add_node(model::node_id{1}, 400_GiB, 1);
    add_node(model::node_id{2}, 100_GiB, 1);
    /**
     * Corner case from real deployment with the following configuration of
     * topics:
     * - partitions:  1, rf: 3, topic_count:  96
     * - partitions:  5, rf: 1, topic_count: 118
     * - partitions: 16, rf: 3, topic_count:   1
     * - partitions:  5, rf: 3, topic_count:  36
     * - partitions: 25, rf: 3, topic_count:  16
     * - partitions:  3, rf: 3, topic_count:   2
     * - partitions:  1, rf: 1, topic_count:  37
     * - partitions: 10, rf: 1, topic_count:   1
     */
    for (int i = 0; i < 96; ++i) {
        add_topic(fmt::format("topic_1_3_{}", i), 1, 3, 10_MiB);
    }
    for (int i = 0; i < 118; ++i) {
        add_topic(fmt::format("topic_5_1_{}", i), 5, 1, 10_MiB);
    }

    add_topic("topic_10_1", 10, 1, 10_MiB);
    for (int i = 0; i < 36; ++i) {
        add_topic(fmt::format("topic_5_3_{}", i), 5, 3, 10_MiB);
    }
    for (int i = 0; i < 16; ++i) {
        add_topic(fmt::format("topic_25_1_{}", i), 25, 3, 10_MiB);
    }
    for (int i = 0; i < 2; ++i) {
        add_topic(fmt::format("topic_3_3_{}", i), 3, 3, 10_MiB);
    }

    for (int i = 0; i < 37; ++i) {
        add_topic(fmt::format("topic_1_1_{}", i), 3, 3, 10_MiB);
    }

    add_node(model::node_id{4}, 300_GiB, 1);
    add_node_to_rebalance(model::node_id{4});
    BOOST_REQUIRE(run_to_completion(total_replicas() * 3));
    set_decommissioning(model::node_id{4});
    BOOST_REQUIRE(run_to_completion(total_replicas() * 3));
}

// this test checks that the partition balancer planner will report
// reallocation failures when a decommissioning node's partitions have lost
// source quorum
//
// 1. create a number of RF=1 partitions
// 2. decommission a node to start partition moves
// 3. remove the decommissioned node, wait for it to be deemed unresponsive
// 4. run the planner again to check that the moving partitions have become
//    reallocation failures
FIXTURE_TEST(
  test_report_dead_decom_as_realloc_failure, partition_balancer_sim_fixture) {
    add_node(model::node_id{0}, 100_GiB, 1);
    add_node(model::node_id{1}, 100_GiB, 1);
    add_node(model::node_id{2}, 100_GiB, 1);

    model::node_id node_to_kill{1};

    // if randomly distributed, 10^-6 probability that no partition lands on
    // node_to_kill
    for (int i = 0; i < 33; ++i) {
        add_topic(fmt::format("topic_rf_1_iteration_{}", i), 1, 1, 10_MiB);
    }

    run_balancer();

    // start a decommission and run the planner
    set_decommissioning(node_to_kill);
    std::ignore = do_run_balancer();

    remove_node(node_to_kill);

    // guarantee that the node will be deemed unresponsive
    ss::sleep(node_responsiveness_timeout + 1s).get();

    auto plan = do_run_balancer();
    BOOST_REQUIRE(plan.reallocation_failures.size() > 0);
}
