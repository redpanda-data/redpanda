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
#define BOOST_TEST_MODULE leader_balancer

#include "cluster/scheduling/leader_balancer_greedy.h"

#include <boost/test/unit_test.hpp>

#include <vector>

BOOST_AUTO_TEST_CASE(greedy) {
    cluster::leader_balancer_strategy::index_type index;

    // 10 nodes, 2 cores -> 20 shards
    std::vector<model::broker_shard> shards;
    for (auto n = 0U; n < 10; n++) {
        for (auto s = 0U; s < 2; s++) {
            model::broker_shard shard{model::node_id(n), s};
            shards.push_back(shard);
        }
    }

    size_t replica = 0;
    for (auto shard : shards) {
        // 10 groups per shard
        for (auto g = 0U; g < 10; g++) {
            raft::group_id group(g);
            // 3 replica per group
            std::vector<model::broker_shard> replicas;
            replicas.push_back(shard); // the "leader"
            while (replicas.size() != 3) {
                if (shards[replica % shards.size()] != shard) {
                    replicas.push_back(shards[replica % shards.size()]);
                }
                ++replica;
            }
            index[shard][group] = std::move(replicas);
        }
    }

    auto greed = cluster::greedy_balanced_shards(index, {});
    BOOST_REQUIRE_EQUAL(greed.error(), 0);

    // new groups on shard 5
    index[shards[5]][raft::group_id(20)] = index[shards[5]][raft::group_id(3)];
    index[shards[5]][raft::group_id(21)] = index[shards[5]][raft::group_id(3)];
    index[shards[5]][raft::group_id(22)] = index[shards[5]][raft::group_id(3)];

    greed = cluster::greedy_balanced_shards(index, {});
    BOOST_REQUIRE_GT(greed.error(), 0);

    // movement should be _from_ the overloaded shard
    auto movement = greed.find_movement({});
    BOOST_REQUIRE(movement);
    BOOST_REQUIRE_EQUAL(movement->from, shards[5]);
}
