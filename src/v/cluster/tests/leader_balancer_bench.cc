/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/scheduling/leader_balancer_greedy.h"
#include "leader_balancer_test_utils.h"

#include <seastar/testing/perf_tests.hh>

#include <vector>

namespace {

void balancer_bench(bool measure_all) {
    constexpr int node_count = 72;
    constexpr int shards_per_node = 16;  // i.e., cores per node
    constexpr int groups_per_shard = 80; // group == partition in this context
    constexpr int replicas = 3;          // number of replicas

    cluster::leader_balancer_strategy::index_type index
      = leader_balancer_test_utils::make_cluster_index(
        node_count, shards_per_node, groups_per_shard, replicas);

    if (measure_all) {
        perf_tests::start_measuring_time();
    }

    auto greed = cluster::greedy_balanced_shards(index, {});
    vassert(greed.error() == 0, "");
    vassert(greed.error() == 0, "e > 0");

    // movement should be _from_ the overloaded shard
    if (!measure_all) {
        perf_tests::start_measuring_time();
    }
    auto movement = greed.find_movement({});
    vassert(!movement, "movemement ");
    perf_tests::do_not_optimize(movement);
    perf_tests::stop_measuring_time();
}

} // namespace

PERF_TEST(leader_balancing, bench_movement) { balancer_bench(false); }

PERF_TEST(leader_balancing, bench_all) { balancer_bench(true); }
