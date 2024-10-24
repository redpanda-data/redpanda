// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/partition_balancer_planner_fixture.h"

#include <seastar/testing/perf_tests.hh>

PERF_TEST_C(partition_balancer_planner_fixture, unavailable_nodes) {
    static bool initialized = false;
    if (!initialized) {
        ss::thread_attributes thread_attr;
        co_await ss::async(thread_attr, [this] {
            allocator_register_nodes(3);
            create_topic("topic-1", 20000, 3);
            allocator_register_nodes(2);
        });

        initialized = true;
    }

    uint64_t local_partition_size = 10_KiB;
    auto hr = create_health_report({}, {}, local_partition_size);

    std::set<size_t> unavailable_nodes = {0};
    co_await populate_node_status_table(unavailable_nodes);

    const size_t max_concurrent_actions = 50;
    auto planner = make_planner(
      model::partition_autobalancing_mode::continuous, max_concurrent_actions);

    abort_source as;
    perf_tests::start_measuring_time();
    auto plan_data = co_await planner.plan_actions(hr, as);
    perf_tests::stop_measuring_time();

    const auto& reassignments = plan_data.reassignments;
    vassert(
      reassignments.size() == max_concurrent_actions,
      "unexpected reassignments size: {}",
      reassignments.size());
}

PERF_TEST_C(partition_balancer_planner_fixture, counts_rebalancing) {
    static bool initialized = false;
    static size_t imbalances = 0;
    if (!initialized) {
        ss::thread_attributes thread_attr;
        co_await ss::async(thread_attr, [this] {
            allocator_register_nodes(5);
            // worst case: balanced partition distribution
            create_topic(
              "really_long_topic_name_to_force_sstring_allocations", 10000, 3);
            // id doesn't matter
            workers.state.local().add_node_to_rebalance(model::node_id{123});
        });

        size_t expected_per_node = 10000 * 3 / 5;

        for (const auto& [id, n] :
             workers.allocator.local().state().allocation_nodes()) {
            size_t count = n->final_partitions();
            if (count > expected_per_node) {
                // Sometimes node replica counts after topic allocation can be
                // off by 1 due to unlucky order of replica allocation. Count
                // those imbalances to later compare with the output of the
                // balancer.
                vassert(
                  count == expected_per_node + 1,
                  "unexpected partition count on node {}: {}",
                  id,
                  count);
                imbalances += 1;
            }
        }

        initialized = true;
    }

    uint64_t local_partition_size = 10_KiB;
    auto hr = create_health_report({}, {}, local_partition_size);

    co_await populate_node_status_table();

    auto planner = make_planner();

    abort_source as;
    perf_tests::start_measuring_time();
    auto plan_data = co_await planner.plan_actions(hr, as);
    perf_tests::stop_measuring_time();

    const auto& reassignments = plan_data.reassignments;
    vassert(
      reassignments.size() == imbalances,
      "unexpected reassignments size: {} (imbalances: {})",
      reassignments.size(),
      imbalances);
}
