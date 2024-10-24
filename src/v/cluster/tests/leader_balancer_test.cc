/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/scheduling/leader_balancer_types.h"

#include <cstdint>
#define BOOST_TEST_MODULE leader_balancer

#include "absl/container/flat_hash_map.h"
#include "cluster/scheduling/leader_balancer_random.h"
#include "leader_balancer_test_utils.h"
#include "model/metadata.h"
#include "utils/to_string.h"

#include <absl/container/flat_hash_set.h>
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <iterator>
#include <tuple>
#include <vector>

using index_type = cluster::leader_balancer_strategy::index_type;
using strategy = cluster::leader_balancer_types::random_hill_climbing_strategy;
using reassignment = cluster::leader_balancer_strategy::reassignment;

/**
 * @brief Basic validity checks on a reassignment.
 */
static void check_valid(const index_type& index, const reassignment& movement) {
    // from == to is not valid (error should not decrease)
    BOOST_REQUIRE(movement.from != movement.to);

    auto& from_groups = index.at(movement.from);
    // check that the from shard was the leader
    BOOST_REQUIRE(from_groups.contains(movement.group));

    // check that the to shard is in the replica set
    auto& replicas = from_groups.at(movement.group);
    BOOST_REQUIRE_EQUAL(
      std::count(replicas.begin(), replicas.end(), movement.to), 1);
}

struct node_spec {
    std::vector<int> groups_led, groups_followed;
};

// a cluster_spec is a vector of node_specs, i.e., one element
// per node, and each node_spec is two vectors: the list of groups
// for this node is a leader, followed by those for which it is a follower
using cluster_spec = std::vector<node_spec>;

/**
 * @brief Create a balancer strategy from a cluster_spec and set of muted nodes.
 */
static auto from_spec(
  const cluster_spec& spec, const absl::flat_hash_set<int>& muted = {}) {
    index_type index;

    absl::flat_hash_map<raft::group_id, model::broker_shard> group_to_leader;

    model::node_id node_counter{0};
    for (const node_spec& node : spec) {
        model::broker_shard shard{node_counter, 0};

        index[shard]; // always create the shard entry even if it leads nothing

        for (auto& lg : node.groups_led) {
            auto leader_group = raft::group_id{lg};
            BOOST_REQUIRE_MESSAGE(
              !group_to_leader.contains(leader_group),
              "multiple leaders for same group");
            group_to_leader.insert({leader_group, shard});
            index[shard][leader_group] = {shard};
        }

        node_counter++;
    }

    node_counter = model::node_id{0};
    for (const node_spec& node : spec) {
        model::broker_shard shard{node_counter, 0};

        auto followed = node.groups_followed;

        if (followed.size() == 1 && followed.front() == -1) {
            // following only the -1 group is a special flag which means follow
            // all groups
            followed.clear();
            for (auto& gl : group_to_leader) {
                if (gl.second != shard) {
                    followed.push_back(static_cast<int>(gl.first));
                }
            }
        }

        for (auto& fg : followed) {
            auto followed = raft::group_id{fg};
            BOOST_REQUIRE(group_to_leader.contains(followed));
            auto leader = group_to_leader.at(followed);
            BOOST_REQUIRE(index[leader].contains(followed));
            index[leader][followed].push_back(shard);
        }

        node_counter++;
    }

    // transform muted from int to broker_shard (we assume the shard is always
    // zero)
    absl::flat_hash_set<model::node_id> muted_bs;
    std::transform(
      muted.begin(),
      muted.end(),
      std::inserter(muted_bs, muted_bs.begin()),
      [](auto id) { return model::node_id{id}; });

    auto index_cp = leader_balancer_test_utils::copy_cluster_index(index);
    auto g_to_topic = leader_balancer_test_utils::make_gid_to_topic_index(
      index);
    return std::make_tuple(
      std::move(index_cp),
      strategy{
        std::move(index),
        std::move(g_to_topic),
        cluster::leader_balancer_types::muted_index{muted_bs, {}},
        std::nullopt});
};

/**
 * @brief returns true iff the passed spec had no movement on balance
 */
bool no_movement(
  const cluster_spec& spec,
  const absl::flat_hash_set<int>& muted = {},
  const cluster::leader_balancer_types::muted_groups_t& skip = {}) {
    auto [_, balancer] = from_spec(spec, muted);
    return !balancer.find_movement(skip);
}

BOOST_AUTO_TEST_CASE(empty) {
    // empty spec, expect no movement
    BOOST_REQUIRE(no_movement({}));
}

BOOST_AUTO_TEST_CASE(balanced) {
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

BOOST_AUTO_TEST_CASE(obeys_replica) {
    // unbalanced, but the overloaded node has no replicas it can
    // send its groups to
    BOOST_REQUIRE(no_movement({{{1, 2, 3}, {}}, {{4}, {}}, {{}, {4}}}));
}

struct expected_reassignment {
    std::optional<model::node_id> from;
    std::optional<model::node_id> to;
    std::optional<raft::group_id> group;
};

/**
 * @brief Helper to create a reassignment from from, to and group passed as
 * integers. -1 means "any"
 */
static expected_reassignment re(int from, int to, int group = -1) {
    expected_reassignment ret;
    if (from != -1) {
        ret.from = model::node_id(from);
    }
    if (to != -1) {
        ret.to = model::node_id(to);
    }
    if (group != -1) {
        ret.group = raft::group_id(group);
    }
    return ret;
}

bool operator==(const reassignment& r, const expected_reassignment& e) {
    return (!e.from || e.from == r.from.node_id)
           && (!e.to || e.to == r.to.node_id)
           && (!e.group || e.group == r.group);
}

ss::sstring to_string(const reassignment& r) {
    return fmt::format("{{from: {} to: {} group: {}}}", r.from, r.to, r.group);
}

ss::sstring to_string(const expected_reassignment& r) {
    return fmt::format("{{from: {} to: {} group: {}}}", r.from, r.to, r.group);
}

/**
 * @brief Test helper which checks that the given spec and other parameters
 * result in the expected resassignment.
 */
ss::sstring expect_movement(
  const cluster_spec& spec,
  const expected_reassignment& expected,
  const absl::flat_hash_set<int>& muted = {},
  const absl::flat_hash_set<int>& skip = {}) {
    auto [index, balancer] = from_spec(spec, muted);

    cluster::leader_balancer_types::muted_groups_t skip_typed;
    for (auto s : skip) {
        skip_typed.add(static_cast<uint64_t>(s));
    }

    auto reassignment = balancer.find_movement(skip_typed);

    if (!reassignment) {
        return "no reassignment occurred";
    }

    check_valid(index, *reassignment);

    if (!(*reassignment == expected)) {
        return fmt::format(
          "Reassignment not as expected.\nExpected: {}\nActual:   {}\n",
          to_string(expected),
          to_string(*reassignment));
    }

    return "";
}

BOOST_AUTO_TEST_CASE(simple_movement) {
    // simple balancing from {2, 0} leader counts to {1, 1}
    BOOST_REQUIRE_EQUAL(
      expect_movement(
        {
          // clang-format off
          {{1, 2}, {}},
          {{}, {1, 2}}
          // clang-format on
        },
        re(0, 1)),
      "");

    // balancing should occur from any node to node 3
    BOOST_REQUIRE_EQUAL(
      expect_movement(
        {
          // clang-format off
            {{1, 2},    {-1}},
            {{3, 4, 5}, {-1}},
            {{6, 7},    {-1}},
            {{},        {1, 2, 3, 4, 5, 6, 7}}  // to 3

          // clang-format on
        },
        re(-1, 3)),
      "");
}

BOOST_AUTO_TEST_CASE(low_to_lower) {
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
        re(2, 3, 8)),
      "");
}

BOOST_AUTO_TEST_CASE(muted) {
    // base spec without high, medium and low (zero) load nodes
    auto spec = cluster_spec{
      // clang-format off
      {{1, 2, 3, 4}, {-1}},
      {{5, 6},       {-1}},
      {{},           {-1}},
      // clang-format on
    };

    // base case, some move expected
    BOOST_REQUIRE_EQUAL(expect_movement(spec, re(-1, -1)), "");

    // mute from the "high" node, so moves from mid to low
    BOOST_REQUIRE_EQUAL(expect_movement(spec, re(1, 2), {0}), "");

    // mute from the "low" node, so moves from high to mid
    BOOST_REQUIRE_EQUAL(expect_movement(spec, re(0, 1), {2}), "");

    // mute any 2 nodes and there should be no movement
    BOOST_REQUIRE(no_movement(spec, {0, 1}));
    BOOST_REQUIRE(no_movement(spec, {0, 2}));
    BOOST_REQUIRE(no_movement(spec, {1, 2}));
}

BOOST_AUTO_TEST_CASE(skip) {
    // base spec without high, medium and low load nodes

    auto spec = cluster_spec{
      // clang-format off
      {{1, 2, 3, 4}, {-1}},
      {{5, 6},       {-1}},
      {{},           {-1}},
      // clang-format on
    };

    // skip all groups on node 0, we move mid to low
    BOOST_REQUIRE_EQUAL(expect_movement(spec, re(1, 2), {}, {1, 2, 3, 4}), "");

    // mute node 0 and skip all groups on node 1, no movement
    cluster::leader_balancer_types::muted_groups_t skip{5, 6};
    BOOST_REQUIRE(no_movement(spec, {0}, skip));
}
