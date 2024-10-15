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
#include "cluster/scheduling/leader_balancer_constraints.h"
#include "cluster/scheduling/leader_balancer_random.h"
#include "cluster/scheduling/leader_balancer_types.h"
#include "leader_balancer_test_utils.h"

#include <seastar/testing/perf_tests.hh>

#include <vector>

namespace {

constexpr int node_count = 72;
constexpr int shards_per_node = 16;  // i.e., cores per node
constexpr int groups_per_shard = 80; // group == partition in this context
constexpr int replicas = 3;          // number of replicas

/*
 * Measures the time it takes to randomly generate and evaluate every possible
 * reassignment for a given cluster. The reassignments are evaluated by the
 * `even_shard_load_constraint` and the `even_topic_distributon_constraint`.
 */
template<typename random_t>
void random_search_eval_bench(bool measure_all) {
    constexpr int total_reassignments = groups_per_shard * shards_per_node
                                        * node_count * (replicas - 1);

    auto index = leader_balancer_test_utils::make_cluster_index(
      node_count, shards_per_node, groups_per_shard, replicas);

    cluster::leader_balancer_types::muted_index mi{{}, {}};
    cluster::leader_balancer_types::shard_index si(
      leader_balancer_test_utils::copy_cluster_index(index));
    auto gid_topic = leader_balancer_test_utils::make_gid_to_topic_index(
      si.shards());

    if (measure_all) {
        perf_tests::start_measuring_time();
    }

    random_t rt{index};
    cluster::leader_balancer_types::even_topic_distribution_constraint tdc(
      std::move(gid_topic), si, mi);
    cluster::leader_balancer_types::even_shard_load_constraint slc(si, mi);

    if (!measure_all) {
        perf_tests::start_measuring_time();
    }

    for (auto i = 0; i < total_reassignments; i++) {
        auto reassignment = rt.generate_reassignment();
        vassert(reassignment.has_value(), "movemement");

        double eval = tdc.evaluate(*reassignment);
        vassert(eval <= 0.001, "eval != 0");

        eval = slc.evaluate(*reassignment);
        vassert(eval <= 0.001, "eval != 0");

        perf_tests::do_not_optimize(eval);
    }

    perf_tests::stop_measuring_time();
}

/*
 * Measures the time needed to randomly generate every possible reassignment for
 * a cluster.
 */
template<typename random_t>
void random_bench() {
    constexpr int total_reassignments = groups_per_shard * shards_per_node
                                        * node_count * (replicas - 1);

    cluster::leader_balancer_strategy::index_type index
      = leader_balancer_test_utils::make_cluster_index(
        node_count, shards_per_node, groups_per_shard, replicas);

    cluster::leader_balancer_types::muted_index mi{{}, {}};

    perf_tests::start_measuring_time();
    random_t rt{index};

    perf_tests::do_not_optimize(rt);
    for (auto i = 0; i < total_reassignments; i++) {
        auto reassignment = rt.generate_reassignment();
        vassert(reassignment.has_value(), "movemement ");

        perf_tests::do_not_optimize(reassignment);
        perf_tests::do_not_optimize(i);
    }

    perf_tests::stop_measuring_time();
}

} // namespace

PERF_TEST(lb, random_eval_movement) {
    random_search_eval_bench<
      cluster::leader_balancer_types::random_reassignments>(false);
}
PERF_TEST(lb, random_eval_all) {
    random_search_eval_bench<
      cluster::leader_balancer_types::random_reassignments>(true);
}

PERF_TEST(lb, random_generator) {
    random_bench<cluster::leader_balancer_types::random_reassignments>();
}
