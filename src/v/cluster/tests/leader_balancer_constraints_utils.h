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

#pragma once

#include "cluster/scheduling/leader_balancer.h"
#include "cluster/scheduling/leader_balancer_constraints.h"
#include "cluster/scheduling/leader_balancer_greedy.h"
#include "cluster/scheduling/leader_balancer_types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/types.h"
#include "vassert.h"

#include <algorithm>
#include <iterator>
#include <tuple>
#include <vector>

using index_type = cluster::leader_balancer_strategy::index_type;

struct node_spec {
    std::vector<int> groups_led, groups_followed;
};

// a cluster_spec is a vector of node_specs, i.e., one element
// per node, and each node_spec is two vectors: the list of groups
// for this node is a leader, followed by those for which it is a follower
using cluster_spec = std::vector<node_spec>;

namespace lbt = cluster::leader_balancer_types;
using reassignment = lbt::reassignment;

/**
 * @brief Create a greedy balancer from a cluster_spec and set of muted nodes.
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
            vassert(
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
            vassert(group_to_leader.contains(followed), "");
            auto leader = group_to_leader.at(followed);
            vassert(index[leader].contains(followed), "");
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

    auto shard_index = lbt::shard_index(std::move(index));
    auto muted_index = lbt::muted_index(std::move(muted_bs), {});

    return std::make_tuple(std::move(shard_index), std::move(muted_index));
};

// Each element of this vector is a topic and the raft groups that are
// associated with that topic's partition. This spec can be used to generate
// a mapping of group_id to ntp.
using group_to_ntp_spec
  = std::vector<std::pair<model::revision_id::type, std::vector<int>>>;
static auto group_to_topic_from_spec(const group_to_ntp_spec& spec) {
    lbt::group_id_to_topic_revision_t group_to_topic;

    for (const auto& [topic, groups] : spec) {
        for (const auto& group : groups) {
            group_to_topic.try_emplace(raft::group_id(group), topic);
        }
    }

    return group_to_topic;
}

/**
 * @brief Basic validity checks on a reassignment.
 */
static void check_valid(const index_type& index, const reassignment& movement) {
    // from == to is not valid (error should not decrease)
    vassert(movement.from != movement.to, "");

    auto& from_groups = index.at(movement.from);
    // check that the from shard was the leader
    vassert(from_groups.contains(movement.group), "");

    // check that the to shard is in the replica set
    auto& replicas = from_groups.at(movement.group);
    vassert(std::count(replicas.begin(), replicas.end(), movement.to) == 1, "");
}

/**
 * @brief returns true iff the passed spec had no movement on balance
 */
static bool no_movement(
  const cluster_spec& spec,
  const absl::flat_hash_set<int>& muted = {},
  const absl::flat_hash_set<raft::group_id>& skip = {}) {
    auto [shard_index, muted_index] = from_spec(spec, muted);
    muted_index.update_muted_groups(skip);
    auto balancer = lbt::even_shard_load_constraint(shard_index, muted_index);

    return !balancer.recommended_reassignment();
}

static bool operator==(const lbt::reassignment& l, const lbt::reassignment& r) {
    return l.group == r.group && l.from == r.from && l.to == r.to;
}

static ss::sstring to_string(const reassignment& r) {
    return fmt::format("{{group: {} from: {} to: {}}}", r.group, r.from, r.to);
}

/**
 * @brief Test helper which checks that the given spec and other parameters
 * result in the expected resassignment.
 */
static ss::sstring expect_movement(
  const cluster_spec& spec,
  const reassignment& expected_reassignment,
  const absl::flat_hash_set<int>& muted = {},
  const absl::flat_hash_set<int>& skip = {}) {
    auto [shard_index, mute_index] = from_spec(spec, muted);

    absl::flat_hash_set<raft::group_id> skip_typed;
    std::transform(
      skip.begin(),
      skip.end(),
      std::inserter(skip_typed, skip_typed.begin()),
      [](auto groupid) { return raft::group_id{groupid}; });

    mute_index.update_muted_groups(std::move(skip_typed));
    auto balancer = lbt::even_shard_load_constraint(shard_index, mute_index);

    auto reassignment = balancer.recommended_reassignment();

    if (!reassignment) {
        return "no reassignment occurred";
    }

    check_valid(shard_index.shards(), *reassignment);

    if (!(*reassignment == expected_reassignment)) {
        return fmt::format(
          "Reassignment not as expected.\nExpected: {}\nActual:   {}\n",
          to_string(expected_reassignment),
          to_string(*reassignment));
    }

    return "";
}

/**
 * @brief Helper to create a reassignment from group, from and to passed as
 * integers.
 */
static reassignment re(unsigned group, int from, int to) {
    return {
      raft::group_id(group),
      model::broker_shard{model::node_id(from)},
      model::broker_shard{model::node_id(to)}};
}
