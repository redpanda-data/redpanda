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
#define BOOST_TEST_MODULE leader_balancer_constraints

#include "cluster/scheduling/leader_balancer_constraints.h"
#include "cluster/scheduling/leader_balancer_types.h"
#include "leader_balancer_constraints_utils.h"
#include "leader_balancer_test_utils.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/types.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_suite.hpp>

#include <algorithm>
#include <fstream>
#include <iterator>
#include <memory>
#include <sstream>
#include <tuple>
#include <vector>

BOOST_AUTO_TEST_CASE(greedy_empty) {
    // empty spec, expect no movement
    BOOST_REQUIRE(no_movement({}));
}

BOOST_AUTO_TEST_CASE(greedy_balanced) {
    // single node is already balanced, expect no movement
    BOOST_REQUIRE(no_movement({
      {{1, 2, 3}, {}},
    }));
    BOOST_REQUIRE(no_movement({
      {{1}, {2}},
      {{2}, {1}},
    }));

    // not exactly balanced, but as balanced as possible,
    // i.e., lead loaded node has 1 less than the most loaded node
    BOOST_REQUIRE(no_movement({
      {{1, 3}, {2}},
      {{2}, {1, 3}},
    }));
}

BOOST_AUTO_TEST_CASE(greedy_movement) {
    // 10 nodes
    // 2 cores x node
    // 10 partitions per shard
    // r=3 (3 replicas)
    auto index = leader_balancer_test_utils::make_cluster_index(10, 2, 10, 3);
    auto shard_index = lbt::shard_index(index);
    auto mute_index = lbt::muted_index({}, {});

    auto greed = lbt::even_shard_load_constraint(shard_index, mute_index);
    BOOST_REQUIRE_EQUAL(greed.error(), 0);

    // new groups on shard {2, 0}
    auto shard20 = model::broker_shard{model::node_id{2}, 0};
    index[shard20][raft::group_id(20)] = index[shard20][raft::group_id(3)];
    index[shard20][raft::group_id(21)] = index[shard20][raft::group_id(3)];
    index[shard20][raft::group_id(22)] = index[shard20][raft::group_id(3)];

    auto shard_index2 = lbt::shard_index(index);
    auto greed2 = lbt::even_shard_load_constraint(shard_index2, mute_index);
    BOOST_REQUIRE_GT(greed2.error(), 0);

    // movement should be _from_ the overloaded shard
    auto movement = greed2.recommended_reassignment();
    BOOST_REQUIRE(movement);
    check_valid(shard_index2.shards(), *movement);
    BOOST_REQUIRE_EQUAL(movement->from, shard20);
}

BOOST_AUTO_TEST_CASE(greedy_obeys_replica) {
    // unbalanced, but the overloaded node has no replicas it can
    // send its groups to
    BOOST_REQUIRE(no_movement({{{1, 2, 3}, {}}, {{4}, {}}, {{}, {4}}}));
}

BOOST_AUTO_TEST_CASE(greedy_simple_movement) {
    // simple balancing from {2, 0} leader counts to {1, 1}
    BOOST_REQUIRE_EQUAL(
      expect_movement(
        {
          // clang-format off
          {{1, 2}, {}},
          {{}, {1, 2}}
          // clang-format on
        },
        re(1, 0, 1)),
      "");
}

BOOST_AUTO_TEST_CASE(greedy_highest_to_lowest) {
    // balancing should occur from the most to least loaded node if that is
    // possible
    BOOST_REQUIRE_EQUAL(
      expect_movement(
        {
          // clang-format off
            {{1, 2},    {-1}},
            {{3, 4, 5}, {-1}},                  // from 1
            {{6, 7},    {-1}},
            {{},        {1, 2, 3, 4, 5, 6, 7}}  // to 3

          // clang-format on
        },
        re(3, 1, 3)),
      "");

    // like the previous case but group 3 is not replicated on the to node, so
    // we check that group 4 goes instead
    BOOST_REQUIRE_EQUAL(
      expect_movement(
        {
          // clang-format off
          {{1, 2},    {-1}},
          {{3, 4, 5}, {-1}}, // from 1
          {{6, 7},    {-1}},
          {{},        {1, 2, 4, 5, 6, 7}} // to 3

          // clang-format on
        },
        re(4, 1, 3)),
      "");
}

BOOST_AUTO_TEST_CASE(greedy_low_to_lower) {
    // balancing can occur even from a shard with less than average load,
    // if there is a shard with even lower load and the higher loaded shards
    // cannot be rebalanced from because of a lack of replicas for their
    // groups on the lower load nodes
    BOOST_REQUIRE_EQUAL(
      expect_movement(
        {
          // clang-format off
          {{1, 2, 3, 10}, {}},
          {{4, 5, 6, 11}, {}},
          {{7, 8},        {}},  // from 2
          {{},            {8}}  // to 3

          // clang-format on
        },
        re(8, 2, 3)),
      "");
}

BOOST_AUTO_TEST_CASE(greedy_muted) {
    // base spec without high, medium and low (zero) load nodes
    auto spec = cluster_spec{
      // clang-format off
      {{1, 2, 3, 4}, {-1}},
      {{5, 6},       {-1}},
      {{},           {-1}},
      // clang-format on
    };

    // base case, move from high to low
    BOOST_REQUIRE_EQUAL(expect_movement(spec, re(1, 0, 2)), "");

    // mute from the "from" node (high), so moves from mid to low
    BOOST_REQUIRE_EQUAL(expect_movement(spec, re(5, 1, 2), {0}), "");

    // mute from the "to" node (low), so moves from high to mid
    BOOST_REQUIRE_EQUAL(expect_movement(spec, re(1, 0, 1), {2}), "");

    // mute any 2 nodes and there should be no movement
    BOOST_REQUIRE(no_movement(spec, {0, 1}));
    BOOST_REQUIRE(no_movement(spec, {0, 2}));
    BOOST_REQUIRE(no_movement(spec, {1, 2}));
}

BOOST_AUTO_TEST_CASE(greedy_skip) {
    // base spec without high, medium and low load nodes

    auto spec = cluster_spec{
      // clang-format off
      {{1, 2, 3, 4}, {-1}},
      {{5, 6},       {-1}},
      {{},           {-1}},
      // clang-format on
    };

    // base case, move from high to low
    BOOST_REQUIRE_EQUAL(expect_movement(spec, re(1, 0, 2)), "");

    // skip group 1, we move group 2 instead from same node
    BOOST_REQUIRE_EQUAL(expect_movement(spec, re(2, 0, 2), {}, {1}), "");

    // skip all groups on node 0, we move mid to low
    BOOST_REQUIRE_EQUAL(
      expect_movement(spec, re(5, 1, 2), {}, {1, 2, 3, 4}), "");

    // mute node 0 and skip all groups on node 1, no movement
    absl::flat_hash_set<raft::group_id> skip{
      raft::group_id(5), raft::group_id(6)};
    BOOST_REQUIRE(no_movement(spec, {0}, skip));
}

BOOST_AUTO_TEST_CASE(even_topic_distribution_empty) {
    auto gntp_i = group_to_topic_from_spec({});
    auto [shard_index, muted_index] = from_spec({});

    auto even_topic_con = lbt::even_topic_distributon_constraint(
      gntp_i, shard_index, muted_index);

    BOOST_REQUIRE(even_topic_con.error() == 0);
    BOOST_REQUIRE(!even_topic_con.recommended_reassignment());
}

BOOST_AUTO_TEST_CASE(even_topic_distribution_constraint_no_error) {
    // In this case leadership for the topic "test" is evenly
    // distributed on every node. Hence the even topic constraint
    // should report no error.

    auto g_id_to_t_id = group_to_topic_from_spec({
      {0, {1, 2}},
    });

    auto spec = cluster_spec{
      {{1}, {2}},
      {{2}, {1}},
    };

    auto [index_cl, gbs] = from_spec(spec);
    auto mute_i = lbt::muted_index({}, {});

    auto topic_constraint = lbt::even_topic_distributon_constraint(
      g_id_to_t_id, index_cl, mute_i);

    BOOST_REQUIRE(topic_constraint.error() == 0);
}

BOOST_AUTO_TEST_CASE(even_topic_distributon_constraint_uniform_move) {
    // In this case all leadership for the "test" topic
    // is on a single node. This test checks if the even
    // topic constraints correctly reports an error.

    auto g_id_to_t_id = group_to_topic_from_spec({
      {0, {1, 2}},
    });

    auto [index_cl, _] = from_spec({
      {{1, 2}, {}},
      {{}, {1, 2}},
    });

    auto mute_i = lbt::muted_index({}, {});

    auto topic_constraint = lbt::even_topic_distributon_constraint(
      g_id_to_t_id, index_cl, mute_i);
    auto reassignment = re(1, 0, 1);

    BOOST_REQUIRE(topic_constraint.error() != 0);
    BOOST_REQUIRE(
      topic_constraint.error() == topic_constraint.evaluate(reassignment));

    index_cl.update_index(reassignment);
    topic_constraint.update_index(reassignment);

    BOOST_REQUIRE(topic_constraint.error() == 0);
}

BOOST_AUTO_TEST_CASE(even_topic_constraint_too_many_replicas) {
    // In this case although a topic's partitions are replicated
    // on many nodes there is only 2 possible leadership assignments.
    // Hence the cluster spec should be as optimial as possible and
    // the even topic constraint shouldn't recommend any reassignments.

    auto g_id_to_t_id = group_to_topic_from_spec({
      {0, {1, 2}},
    });

    auto [index_cl, _] = from_spec({
      {{1}, {2}},
      {{2}, {1}},
      {{}, {1}},
      {{}, {2}},
      {{}, {1, 2}},
    });

    auto mute_i = lbt::muted_index({}, {});

    auto topic_constraint = lbt::even_topic_distributon_constraint(
      g_id_to_t_id, index_cl, mute_i);
}

BOOST_AUTO_TEST_CASE(even_topic_distributon_constraint_find_reassignment) {
    // In this case all leadership for the "tst1" topic
    // is on a single node. This test checks if the even
    // topic constraints correctly reports an error and
    // finds a reassignment.

    auto g_id_to_t_id = group_to_topic_from_spec({
      {0, {1, 2}},
      {1, {3, 4}},
    });

    auto [index_cl, _] = from_spec({
      {{1, 2}, {}},
      {{4}, {1, 2, 3}},
      {{3}, {4}},
    });

    auto mute_i = lbt::muted_index({}, {});

    auto topic_constraint = lbt::even_topic_distributon_constraint(
      g_id_to_t_id, index_cl, mute_i);
    auto reassignment = re(1, 0, 1);

    BOOST_REQUIRE(topic_constraint.error() != 0);
    BOOST_REQUIRE(
      topic_constraint.error() == topic_constraint.evaluate(reassignment));

    auto rreassignment = topic_constraint.recommended_reassignment();
    BOOST_REQUIRE(rreassignment.has_value());

    index_cl.update_index(rreassignment.value());
    topic_constraint.update_index(rreassignment.value());

    BOOST_REQUIRE(topic_constraint.error() == 0);
}

BOOST_AUTO_TEST_CASE(even_topic_odd_partition_cnt) {
    // In cases where a topic as a partition count that
    // is not even divisible by the nodes they are replicated on.
    // In these cases its not possible for every node to has_value
    // equal leadership for the topic.
    //
    // This tests that in those cases the even topic constraint
    // correctly recommends no further reassignments.

    auto g_id_to_t_id = group_to_topic_from_spec({
      {0, {1, 2, 3}},
    });

    auto [shard_index, muted_index] = from_spec(
      {
        {{1, 2}, {3}},
        {{3}, {1, 2}},
      },
      {});

    auto even_topic_con = lbt::even_topic_distributon_constraint(
      g_id_to_t_id, shard_index, muted_index);

    BOOST_REQUIRE(!even_topic_con.recommended_reassignment());
}

BOOST_AUTO_TEST_CASE(even_shard_no_error_even_topic_error) {
    // Here even_shard_load_constraint.error = 0, but
    // even_topic_distributon_constraint.error > 0 and any move will increase
    // the first error.
    //
    // In this case optimizing for even shard distribution first will get
    // us stuck in a local minima where any attempt to optimze for even
    // topic distribution will increase total error.

    auto g_id_to_t_id = group_to_topic_from_spec({
      {0, {1, 2}},
      {1, {3, 4}},
    });

    auto [shard_index, muted_index] = from_spec(
      {
        {{1, 2}, {3, 4}},
        {{3, 4}, {1, 2}},
      },
      {});

    auto even_shard_con = lbt::even_shard_load_constraint(
      shard_index, muted_index);
    auto even_topic_con = lbt::even_topic_distributon_constraint(
      g_id_to_t_id, shard_index, muted_index);

    BOOST_REQUIRE(even_shard_con.error() == 0);
    BOOST_REQUIRE(even_topic_con.error() > 0);

    auto rea = even_topic_con.recommended_reassignment();
    BOOST_REQUIRE(rea.has_value());

    BOOST_REQUIRE(even_shard_con.evaluate(*rea) < 0);
    BOOST_REQUIRE(even_topic_con.evaluate(*rea) > 0);
}

BOOST_AUTO_TEST_CASE(even_topic_no_error_even_shard_error) {
    // Here even_topic_distributon_constraint.error = min, but
    // even_shard_load_constraint.error > 0
    //
    // In this case optimizing for even topic distribution first will
    // result in error for the even shard being greater than zero. However,
    // unlike when optimzing for even shard constraint first we are not
    // stuck in a low minima. Hence we can optimize for the even shard
    // constraint without increasing global error.

    auto g_id_to_t_id = group_to_topic_from_spec({
      {0, {1, 2, 3}},
      {1, {4, 5, 6}},
    });

    auto [shard_index, muted_index] = from_spec(
      {
        {{6}, {4, 5}},
        {{1, 2, 4, 5}, {3, 6}},
        {{3}, {1, 2}},
      },
      {});

    auto even_shard_con = lbt::even_shard_load_constraint(
      shard_index, muted_index);
    auto even_topic_con = lbt::even_topic_distributon_constraint(
      g_id_to_t_id, shard_index, muted_index);

    BOOST_REQUIRE(even_shard_con.error() > 0);

    BOOST_REQUIRE(!even_topic_con.recommended_reassignment());
    BOOST_REQUIRE(even_shard_con.recommended_reassignment().has_value());
}

#include "cluster/scheduling/leader_balancer_random.h"

BOOST_AUTO_TEST_CASE(random_reassignments_generation) {
    // Creates a shard index with 3 nodes and 1 topic with 3 partitions.
    // Ensures that random_reassignments generates all possible reassignments.
    auto [shard_index, mi] = from_spec(
      {
        {{0}, {1, 2}},
        {{1}, {0, 2}},
        {{2}, {0, 1}},
      },
      {});

    std::vector<lbt::reassignment> all_reassignments;

    for (const auto& [bs, leaders] : shard_index.shards()) {
        for (const auto& [group, replicas] : leaders) {
            for (const auto& replica : replicas) {
                if (replica != bs) {
                    all_reassignments.emplace_back(group, bs, replica);
                }
            }
        }
    }

    lbt::random_reassignments rr(shard_index.shards());

    for (;;) {
        auto current_reassignment_opt = rr.generate_reassignment();

        if (!current_reassignment_opt) {
            break;
        }

        auto it = std::find_if(
          all_reassignments.begin(),
          all_reassignments.end(),
          [&](const auto& r) {
              return current_reassignment_opt->group == r.group
                     && current_reassignment_opt->from == r.from
                     && current_reassignment_opt->to == r.to;
          });

        BOOST_REQUIRE(it != all_reassignments.end());

        all_reassignments.erase(it);
    }

    BOOST_REQUIRE(all_reassignments.size() == 0);
}

BOOST_AUTO_TEST_CASE(topic_skew_error) {
    // This test replicates a cluster state we encountered during
    // OMB testing. It ensures that the random hill climbing strategy
    // can properly balance this state. The minimum number of reassignments
    // needed to balance is 2.
    auto g_id_to_t_id = group_to_topic_from_spec({
      {0, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
      {1, {10, 11, 12, 13, 14}},
    });

    auto [shard_index, muted_index] = from_spec(
      {
        {{0, 2, 10, 11, 12}, {13, 14, 1, 3, 4, 5, 6, 7, 8, 9}},
        {{1, 6, 8, 13, 14}, {10, 11, 12, 0, 2, 3, 4, 5, 7, 9}},
        {{3, 4, 5, 7, 9}, {10, 11, 12, 13, 14, 0, 1, 2, 6, 8}},
      },
      {});

    auto even_shard_con = lbt::even_shard_load_constraint(
      shard_index, muted_index);
    auto even_topic_con = lbt::even_topic_distributon_constraint(
      g_id_to_t_id, shard_index, muted_index);

    BOOST_REQUIRE(even_shard_con.error() == 0);
    BOOST_REQUIRE(even_topic_con.error() > 0);

    auto rhc = lbt::random_hill_climbing_strategy(
      shard_index.shards(), g_id_to_t_id, lbt::muted_index{{}, {}});

    absl::flat_hash_set<raft::group_id> muted_groups{};

    auto pre_topic_error = even_topic_con.error();
    auto pre_shard_error = even_shard_con.error();
    auto current_error = rhc.error();

    for (;;) {
        auto movement_opt = rhc.find_movement(muted_groups);
        if (!movement_opt) {
            break;
        }
        rhc.apply_movement(*movement_opt);
        even_shard_con.update_index(*movement_opt);
        even_topic_con.update_index(*movement_opt);
        muted_groups.insert(movement_opt->group);

        auto new_error = rhc.error();
        BOOST_REQUIRE(new_error <= current_error);
        current_error = new_error;
    }

    auto post_topic_error = even_topic_con.error();
    auto post_shard_error = even_shard_con.error();

    BOOST_REQUIRE(post_topic_error <= pre_topic_error);
    BOOST_REQUIRE(post_shard_error <= pre_shard_error);
}
