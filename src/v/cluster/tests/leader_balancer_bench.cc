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
#include "cluster/scheduling/leader_balancer_greedy.h"
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

cluster::leader_balancer_types::group_id_to_topic_revision_t
make_gid_to_topic_index(
  const cluster::leader_balancer_types::index_type& index) {
    cluster::leader_balancer_types::group_id_to_topic_revision_t ret;

    for (const auto& [bs, leaders] : index) {
        for (const auto& [group, replicas] : leaders) {
            ret[group] = model::revision_id(0);
        }
    }

    return ret;
}

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
    cluster::leader_balancer_types::shard_index si(index);
    auto gid_topic = make_gid_to_topic_index(si.shards());

    if (measure_all) {
        perf_tests::start_measuring_time();
    }

    random_t rt{index};
    cluster::leader_balancer_types::even_topic_distributon_constraint tdc(
      gid_topic, si, mi);
    cluster::leader_balancer_types::even_shard_load_constraint slc(
      std::move(si), mi);

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
 * Measures the time the even_topic_distributon_constraint takes to generate
 * a recommend reassignment in the worst case where it has to iterate through
 * every possible reassignment.
 */
void even_topic_c_bench(bool measure_all) {
    auto index = leader_balancer_test_utils::make_cluster_index(
      node_count, shards_per_node, groups_per_shard, replicas);

    cluster::leader_balancer_types::muted_index mi{{}, {}};
    cluster::leader_balancer_types::shard_index si(std::move(index));
    auto gid_topic = make_gid_to_topic_index(si.shards());

    if (measure_all) {
        perf_tests::start_measuring_time();
    }

    cluster::leader_balancer_types::even_topic_distributon_constraint tdc(
      gid_topic, si, mi);

    if (!measure_all) {
        perf_tests::start_measuring_time();
    }

    auto movement = tdc.recommended_reassignment();
    vassert(!movement, "movemement");

    perf_tests::do_not_optimize(movement);

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

void balancer_bench(bool measure_all) {
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

PERF_TEST(lb, even_shard_load_movement) { balancer_bench(false); }
PERF_TEST(lb, even_shard_load_all) { balancer_bench(true); }

PERF_TEST(lb, even_topic_distribution_movement) { even_topic_c_bench(false); }
PERF_TEST(lb, even_topic_distribution_all) { even_topic_c_bench(true); }

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
