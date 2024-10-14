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

#include "base/vassert.h"
#include "cluster/scheduling/leader_balancer.h"
#include "cluster/scheduling/leader_balancer_constraints.h"
#include "cluster/scheduling/leader_balancer_types.h"
#include "model/metadata.h"
#include "raft/fundamental.h"

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
inline auto from_spec(
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
inline auto group_to_topic_from_spec(const group_to_ntp_spec& spec) {
    lbt::group_id_to_topic_id group_to_topic;

    for (const auto& [topic, groups] : spec) {
        for (const auto& group : groups) {
            group_to_topic.try_emplace(raft::group_id(group), topic);
        }
    }

    return group_to_topic;
}
