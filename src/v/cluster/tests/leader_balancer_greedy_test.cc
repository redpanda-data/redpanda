/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "cluster/scheduling/leader_balancer_greedy.h"
#include "config/leaders_preference.h"
#include "leader_balancer_test_utils.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <random>
#include <ranges>
#include <tuple>
#include <vector>

using index_type = cluster::leader_balancer_strategy::index_type;
using greedy_strategy
  = cluster::leader_balancer_types::greedy_topic_aware_strategy;
using greedy_strategy_ptr = std::unique_ptr<greedy_strategy>;
using topic_leaders = std::vector<std::vector<model::broker_shard>>;
using group_locations
  = absl::flat_hash_map<raft::group_id, std::pair<size_t, size_t>>;

namespace {

struct partition_spec {
    int group;
    int topic;
    std::vector<model::broker_shard> replicas;
};

model::broker_shard bs(int node_id, uint32_t shard) {
    return model::broker_shard{
      .node_id = model::node_id{node_id}, .shard = shard};
}

std::tuple<index_type, topic_leaders, group_locations, greedy_strategy_ptr>
make_strategy_from_partitions(
  const std::vector<model::broker_shard>& broker_shards,
  const std::vector<partition_spec>& partitions,
  const absl::flat_hash_set<int>& muted_node_ids = {},
  std::optional<cluster::leader_balancer_types::preference_index> preference_idx
  = std::nullopt) {
    index_type index;
    cluster::leader_balancer_types::group_id_to_topic_id group_to_topic;
    topic_leaders current_leaders;
    group_locations locations_by_group;

    for (model::broker_shard broker_shard : broker_shards) {
        index[broker_shard];
    }

    for (const auto& partition : partitions) {
        EXPECT_FALSE(partition.replicas.empty());
        raft::group_id group_id{partition.group};
        model::broker_shard leader = partition.replicas.front();
        index[leader][group_id] = partition.replicas;
        group_to_topic.emplace(
          group_id,
          cluster::leader_balancer_types::topic_id_t{partition.topic});
        if (current_leaders.size() <= static_cast<size_t>(partition.topic)) {
            current_leaders.resize(static_cast<size_t>(partition.topic) + 1);
        }
        size_t topic_index = static_cast<size_t>(partition.topic);
        size_t partition_index = current_leaders[topic_index].size();
        current_leaders[topic_index].push_back(leader);
        locations_by_group.emplace(
          group_id, std::pair{topic_index, partition_index});
    }

    absl::flat_hash_set<model::node_id> muted_nodes;
    absl::flat_hash_set<model::node_id> nodes;
    std::ranges::transform(
      muted_node_ids,
      std::inserter(muted_nodes, muted_nodes.begin()),
      [](int node_id) { return model::node_id(node_id); });
    std::ranges::transform(
      broker_shards,
      std::inserter(nodes, nodes.begin()),
      [](model::broker_shard broker_shard) { return broker_shard.node_id; });

    auto copied_index = leader_balancer_test_utils::copy_cluster_index(index);
    auto strategy = std::make_unique<greedy_strategy>(
      nodes.size(),
      std::move(index),
      std::move(group_to_topic),
      absl::flat_hash_set<cluster::leader_balancer_types::topic_id_t>{},
      cluster::leader_balancer_types::muted_index{muted_nodes, {}},
      std::move(preference_idx));

    return std::make_tuple(
      std::move(copied_index),
      std::move(current_leaders),
      std::move(locations_by_group),
      std::move(strategy));
}

std::tuple<index_type, topic_leaders, group_locations, greedy_strategy_ptr>
make_greedy_strategy(
  int broker_count,
  int shards_per_broker,
  int topic_count,
  int partitions_per_topic,
  int replication_factor = 3) {
    std::vector<model::broker_shard> broker_shards;
    std::vector<partition_spec> partitions;
    int next_group = 1;

    for (int broker : std::views::iota(0, broker_count)) {
        for (int shard : std::views::iota(0, shards_per_broker)) {
            broker_shards.push_back(bs(broker, static_cast<uint32_t>(shard)));
        }
    }

    // Each node independently assigns partitions to shards in a
    // balanced-but-shuffled order: a permutation of [0..shards-1]
    // that repeats, so every shard gets the same number of replicas.
    // Different nodes use different permutations (seeded by node id
    // and topic) so replicas of the same partition land on different
    // shards across nodes, matching real node-local shard placement.
    auto make_shard_sequence = [&](int node, int topic) {
        std::vector<uint32_t> perm(static_cast<size_t>(shards_per_broker));
        std::iota(perm.begin(), perm.end(), uint32_t{0});
        std::mt19937 rng(static_cast<uint32_t>(node * 137 + topic * 31));
        std::ranges::shuffle(perm, rng);
        return perm;
    };

    for (int topic : std::views::iota(0, topic_count)) {
        // Build per-node shard permutations and counters.
        std::vector<std::vector<uint32_t>> node_perms;
        std::vector<size_t> node_counters(static_cast<size_t>(broker_count), 0);
        for (int n = 0; n < broker_count; ++n) {
            node_perms.push_back(make_shard_sequence(n, topic));
        }

        for (int partition : std::views::iota(0, partitions_per_topic)) {
            std::vector<model::broker_shard> replicas;
            int broker_offset = partition + topic;
            for (int replica : std::views::iota(0, replication_factor)) {
                int node = (broker_offset + replica) % broker_count;
                auto& ctr = node_counters[static_cast<size_t>(node)];
                auto& perm = node_perms[static_cast<size_t>(node)];
                auto shard = perm[ctr % perm.size()];
                ++ctr;
                replicas.push_back(bs(node, shard));
            }
            partitions.push_back(
              partition_spec{
                .group = next_group++, .topic = topic, .replicas = replicas});
        }
    }

    return make_strategy_from_partitions(broker_shards, partitions);
}

topic_leaders run_to_convergence(
  greedy_strategy& balancer,
  topic_leaders current_leaders,
  const group_locations& locations_by_group) {
    while (std::optional<cluster::leader_balancer_strategy::reassignment>
             reassignment = balancer.find_movement({})) {
        balancer.apply_movement(*reassignment);
        group_locations::const_iterator location = locations_by_group.find(
          reassignment->group);
        EXPECT_NE(location, locations_by_group.end());
        current_leaders[location->second.first][location->second.second]
          = reassignment->to;
    }
    return current_leaders;
}

void expect_move(
  const index_type& index,
  greedy_strategy& balancer,
  raft::group_id expected_group,
  model::node_id expected_from,
  model::node_id expected_to,
  cluster::leader_balancer_types::muted_groups_t skipped_groups = {}) {
    std::optional<cluster::leader_balancer_strategy::reassignment> reassignment
      = balancer.find_movement(skipped_groups);
    ASSERT_TRUE(reassignment.has_value());
    EXPECT_TRUE(index.at(reassignment->from).contains(reassignment->group));
    EXPECT_EQ(reassignment->group, expected_group);
    EXPECT_EQ(reassignment->from.node_id, expected_from);
    EXPECT_EQ(reassignment->to.node_id, expected_to);
}

void expect_no_move(
  greedy_strategy& balancer,
  cluster::leader_balancer_types::muted_groups_t skipped_groups = {}) {
    EXPECT_FALSE(balancer.find_movement(skipped_groups).has_value());
}

template<size_t broker_count, size_t shards_per_broker>
void expect_shard_counts_per_broker(
  const topic_leaders& current_leaders,
  size_t topic_index,
  const std::array<std::array<int, shards_per_broker>, broker_count>&
    expected_counts) {
    std::array<std::array<int, shards_per_broker>, broker_count>
      leaders_per_shard{};

    for (const model::broker_shard& leader : current_leaders[topic_index]) {
        leaders_per_shard[leader.node_id()][leader.shard] += 1;
    }

    for (size_t broker : std::views::iota(size_t{0}, broker_count)) {
        for (size_t shard : std::views::iota(size_t{0}, shards_per_broker)) {
            EXPECT_EQ(
              leaders_per_shard[broker][shard], expected_counts[broker][shard])
              << "broker=" << broker << " shard=" << shard
              << " topic=" << topic_index;
        }
    }
}

template<size_t broker_count, size_t shards_per_broker>
void expect_combined_shard_counts(
  const topic_leaders& current_leaders,
  const std::array<std::array<int, shards_per_broker>, broker_count>&
    expected_counts) {
    std::array<std::array<int, shards_per_broker>, broker_count>
      leaders_per_shard{};

    for (const auto& topic : current_leaders) {
        for (const model::broker_shard& leader : topic) {
            leaders_per_shard[leader.node_id()][leader.shard] += 1;
        }
    }

    for (size_t broker : std::views::iota(size_t{0}, broker_count)) {
        for (size_t shard : std::views::iota(size_t{0}, shards_per_broker)) {
            EXPECT_EQ(
              leaders_per_shard[broker][shard], expected_counts[broker][shard])
              << "broker=" << broker << " shard=" << shard;
        }
    }
}

/// Build a preference_index with unordered rack preference and a node-to-rack
/// mapping. `node_racks` maps node_id -> rack name. `preferred_racks` lists
/// the rack names that should be preferred for leadership.
cluster::leader_balancer_types::preference_index make_rack_preference(
  const std::vector<std::pair<int, std::string>>& node_racks,
  const std::vector<std::string>& preferred_racks) {
    cluster::leader_balancer_types::preference_index pref;
    pref.default_preference.type = config::leaders_preference::type_t::racks;
    for (const auto& rack : preferred_racks) {
        pref.default_preference.racks.emplace_back(rack);
    }
    for (const auto& [node, rack] : node_racks) {
        pref.node2rack[model::node_id{node}] = model::rack_id{rack};
    }
    return pref;
}

} // namespace

TEST(GreedyLeaderBalancerTest, AssignsToLowestTopicCount) {
    [[maybe_unused]] auto [index, current_leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0)},
        {{1, 0, {bs(0, 0), bs(1, 0)}}, {2, 0, {bs(0, 0), bs(1, 0)}}});
    expect_move(
      index,
      *balancer,
      raft::group_id{2},
      model::node_id{0},
      model::node_id{1});
}

TEST(GreedyLeaderBalancerTest, BreaksTiesDeterministicallyByShardOrder) {
    [[maybe_unused]] auto [index, current_leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0), bs(2, 0)},
        {{1, 0, {bs(1, 0), bs(0, 0), bs(2, 0)}},
         {2, 0, {bs(1, 0), bs(0, 0), bs(2, 0)}}});
    expect_move(
      index,
      *balancer,
      raft::group_id{1},
      model::node_id{1},
      model::node_id{0});
}

TEST(GreedyLeaderBalancerTest, BalancesEachTopicIndependently) {
    [[maybe_unused]] auto [index, current_leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0)},
        {
          {1, 0, {bs(0, 0), bs(1, 0)}},
          {2, 0, {bs(0, 0), bs(1, 0)}},
          {3, 1, {bs(0, 0), bs(1, 0)}},
          {4, 1, {bs(0, 0), bs(1, 0)}},
        });
    expect_move(
      index,
      *balancer,
      raft::group_id{2},
      model::node_id{0},
      model::node_id{1});

    cluster::leader_balancer_types::muted_groups_t skipped_groups;
    skipped_groups.add(static_cast<uint64_t>(2));
    expect_move(
      index,
      *balancer,
      raft::group_id{4},
      model::node_id{0},
      model::node_id{1},
      skipped_groups);
}

TEST(GreedyLeaderBalancerTest, RespectsMutedNodes) {
    [[maybe_unused]] auto [index, current_leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0)},
        {{1, 0, {bs(0, 0), bs(1, 0)}}, {2, 0, {bs(0, 0), bs(1, 0)}}},
        {1});
    expect_no_move(*balancer);
}

TEST(GreedyLeaderBalancerTest, RespectsSkippedGroups) {
    [[maybe_unused]] auto [index, current_leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0)},
        {{1, 0, {bs(0, 0), bs(1, 0)}}, {2, 0, {bs(0, 0), bs(1, 0)}}});
    cluster::leader_balancer_types::muted_groups_t skipped_groups;
    skipped_groups.add(static_cast<uint64_t>(2));
    expect_no_move(*balancer, skipped_groups);
}

TEST(GreedyLeaderBalancerTest, NoMovementWhenAlreadyBalanced) {
    [[maybe_unused]] auto [index, current_leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0)},
        {{1, 0, {bs(0, 0), bs(1, 0)}}, {2, 0, {bs(1, 0), bs(0, 0)}}});
    expect_no_move(*balancer);
}

TEST(GreedyLeaderBalancerTest, SinglePartitionTopicsBalanceAcrossBrokers) {
    // 6 topics with 1 partition each, 3 brokers. The global broker counts
    // steer each successive topic to the least-loaded broker.
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0), bs(2, 0)},
        {
          {1, 0, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {2, 1, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {3, 2, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {4, 3, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {5, 4, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {6, 5, {bs(0, 0), bs(1, 0), bs(2, 0)}},
        });
    topic_leaders balanced = run_to_convergence(
      *balancer, std::move(leaders), locations_by_group);
    expect_combined_shard_counts(
      balanced, std::array<std::array<int, 1>, 3>{{{{2}}, {{2}}, {{2}}}});
}

TEST(GreedyLeaderBalancerTest, MixedTopicSizes) {
    // Topics 0-2 have 1 partition each; topic 3 has 3 partitions.
    // 6 total / 3 brokers = 2 each. The single-partition topics consume the
    // global budget first, then topic 3 fills the remaining slots.
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0), bs(2, 0)},
        {
          {1, 0, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {2, 1, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {3, 2, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {4, 3, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {5, 3, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {6, 3, {bs(0, 0), bs(1, 0), bs(2, 0)}},
        });
    topic_leaders balanced = run_to_convergence(
      *balancer, std::move(leaders), locations_by_group);
    expect_shard_counts_per_broker(
      balanced, 3, std::array<std::array<int, 1>, 3>{{{{1}}, {{1}}, {{1}}}});
    expect_combined_shard_counts(
      balanced, std::array<std::array<int, 1>, 3>{{{{2}}, {{2}}, {{2}}}});
}

TEST(GreedyLeaderBalancerTest, ThreePartitionsThreeBrokers) {
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0), bs(2, 0)},
        {
          {1, 0, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {2, 0, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {3, 0, {bs(0, 0), bs(1, 0), bs(2, 0)}},
        });
    topic_leaders balanced = run_to_convergence(
      *balancer, std::move(leaders), locations_by_group);
    expect_shard_counts_per_broker(
      balanced, 0, std::array<std::array<int, 1>, 3>{{{{1}}, {{1}}, {{1}}}});
}

TEST(GreedyLeaderBalancerTest, AsymmetricReplicas) {
    // Each partition is on 2 of 3 brokers. The greedy walk achieves
    // 1 leader per broker.
    [[maybe_unused]] auto [index, current_leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0), bs(2, 0)},
        {
          {1, 0, {bs(0, 0), bs(1, 0)}},
          {2, 0, {bs(1, 0), bs(2, 0)}},
          {3, 0, {bs(0, 0), bs(2, 0)}},
        });
    expect_move(
      index,
      *balancer,
      raft::group_id{3},
      model::node_id{0},
      model::node_id{2});
}

TEST(GreedyLeaderBalancerTest, TwoTopicsFourBrokersThreeShards) {
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_greedy_strategy(4, 3, 2, 18, 3);
    topic_leaders balanced = run_to_convergence(
      *balancer, std::move(leaders), locations_by_group);
    expect_shard_counts_per_broker(
      balanced,
      0,
      std::array<std::array<int, 3>, 4>{{
        {{2, 1, 1}},
        {{1, 2, 2}},
        {{2, 2, 1}},
        {{1, 1, 2}},
      }});
    expect_shard_counts_per_broker(
      balanced,
      1,
      std::array<std::array<int, 3>, 4>{{
        {{1, 1, 2}},
        {{1, 2, 1}},
        {{2, 2, 1}},
        {{2, 2, 1}},
      }});
    expect_combined_shard_counts(
      balanced,
      std::array<std::array<int, 3>, 4>{{
        {{3, 2, 3}},
        {{2, 4, 3}},
        {{4, 4, 2}},
        {{3, 3, 3}},
      }});
}

TEST(GreedyLeaderBalancerTest, SixTopicsPerfectlyBalanced) {
    // 6 topics x 6 partitions, 3 brokers, 2 shards, RF=3. Every partition
    // has replicas on all brokers. 36 total / 6 shards = 6 per shard.
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_greedy_strategy(3, 2, 6, 6);
    topic_leaders balanced = run_to_convergence(
      *balancer, std::move(leaders), locations_by_group);
    expect_combined_shard_counts(
      balanced,
      std::array<std::array<int, 2>, 3>{{{{6, 6}}, {{6, 6}}, {{6, 6}}}});
}

TEST(GreedyLeaderBalancerTest, RemainderPartitions) {
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_greedy_strategy(3, 2, 2, 8);
    topic_leaders balanced_leaders = run_to_convergence(
      *balancer, std::move(leaders), locations_by_group);
    expect_shard_counts_per_broker(
      balanced_leaders,
      0,
      std::array<std::array<int, 2>, 3>{{{{2, 1}}, {{1, 2}}, {{1, 1}}}});
    expect_shard_counts_per_broker(
      balanced_leaders,
      1,
      std::array<std::array<int, 2>, 3>{{{{1, 2}}, {{1, 1}}, {{1, 2}}}});
    expect_combined_shard_counts(
      balanced_leaders,
      std::array<std::array<int, 2>, 3>{{{{3, 3}}, {{2, 3}}, {{2, 3}}}});
}

TEST(GreedyLeaderBalancerTest, RemainderPartitionsThreeTopics) {
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_greedy_strategy(3, 2, 3, 8);
    topic_leaders balanced_leaders = run_to_convergence(
      *balancer, std::move(leaders), locations_by_group);
    expect_shard_counts_per_broker(
      balanced_leaders,
      0,
      std::array<std::array<int, 2>, 3>{{{{2, 1}}, {{1, 2}}, {{1, 1}}}});
    expect_shard_counts_per_broker(
      balanced_leaders,
      1,
      std::array<std::array<int, 2>, 3>{{{{1, 2}}, {{1, 1}}, {{1, 2}}}});
    expect_shard_counts_per_broker(
      balanced_leaders,
      2,
      std::array<std::array<int, 2>, 3>{{{{1, 1}}, {{2, 1}}, {{2, 1}}}});
    expect_combined_shard_counts(
      balanced_leaders,
      std::array<std::array<int, 2>, 3>{{{{4, 4}}, {{4, 4}}, {{4, 4}}}});
}

TEST(GreedyLeaderBalancerTest, BalancesShardsWithinBroker) {
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_greedy_strategy(3, 4, 2, 18);
    topic_leaders balanced_leaders = run_to_convergence(
      *balancer, std::move(leaders), locations_by_group);
    expect_shard_counts_per_broker(
      balanced_leaders,
      0,
      std::array<std::array<int, 4>, 3>{{
        {{2, 1, 1, 2}},
        {{1, 1, 2, 2}},
        {{1, 2, 1, 2}},
      }});
    expect_shard_counts_per_broker(
      balanced_leaders,
      1,
      std::array<std::array<int, 4>, 3>{{
        {{1, 2, 2, 1}},
        {{2, 1, 1, 2}},
        {{2, 2, 1, 1}},
      }});
    expect_combined_shard_counts(
      balanced_leaders,
      std::array<std::array<int, 4>, 3>{{
        {{3, 3, 3, 3}},
        {{3, 2, 3, 4}},
        {{3, 4, 2, 3}},
      }});
}

TEST(GreedyLeaderBalancerTest, TwoTopicsSixBrokersTwoShardsRfThree) {
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_greedy_strategy(6, 2, 2, 18, 3);
    topic_leaders balanced_leaders = run_to_convergence(
      *balancer, std::move(leaders), locations_by_group);

    expect_shard_counts_per_broker(
      balanced_leaders,
      0,
      std::array<std::array<int, 2>, 6>{
        {{{2, 1}}, {{1, 2}}, {{2, 1}}, {{2, 1}}, {{1, 2}}, {{1, 2}}}});
    expect_shard_counts_per_broker(
      balanced_leaders,
      1,
      std::array<std::array<int, 2>, 6>{
        {{{2, 2}}, {{2, 1}}, {{1, 2}}, {{1, 1}}, {{1, 2}}, {{1, 2}}}});
    expect_combined_shard_counts(
      balanced_leaders,
      std::array<std::array<int, 2>, 6>{
        {{{4, 3}}, {{3, 3}}, {{3, 3}}, {{3, 2}}, {{2, 4}}, {{2, 4}}}});
}

TEST(GreedyLeaderBalancerTest, SkippedMoveDoesNotDesyncIndex) {
    // 4 partitions on topic 0, all led by broker 0, RF=3 across 4 brokers.
    // g4's replica set includes b3 instead of b2 so that the greedy planner
    // assigns each partition to a distinct broker and produces three moves:
    //   g2→b1 (index 0), g3→b2 (index 1), g4→b3 (index 2).
    // When the first move is skipped, apply_movement must advance
    // _next_pending past the actually-applied move so that subsequent
    // find_movement calls do not re-return already-applied moves.
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0), bs(2, 0), bs(3, 0)},
        {
          {1, 0, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {2, 0, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {3, 0, {bs(0, 0), bs(1, 0), bs(2, 0)}},
          {4, 0, {bs(0, 0), bs(1, 0), bs(3, 0)}},
        });

    cluster::leader_balancer_types::muted_groups_t skip;
    skip.add(static_cast<uint64_t>(2));

    // First call skips g2 (index 0), returns g3 (index 1).
    auto move1 = balancer->find_movement(skip);
    ASSERT_TRUE(move1.has_value());
    EXPECT_EQ(move1->group, raft::group_id{3});
    EXPECT_EQ(move1->to.node_id, model::node_id{2});
    balancer->apply_movement(*move1);

    // Second call must return g4 (index 2), NOT g3 again.
    auto move2 = balancer->find_movement(skip);
    ASSERT_TRUE(move2.has_value());
    EXPECT_EQ(move2->group, raft::group_id{4});
    EXPECT_EQ(move2->to.node_id, model::node_id{3});
    balancer->apply_movement(*move2);

    // No more moves (g2 is still skipped).
    EXPECT_FALSE(balancer->find_movement(skip).has_value());
}

TEST(GreedyLeaderBalancerTest, PinningRejectsMovesToNonPreferredRack) {
    // 2 groups on topic 0, both led by broker 0 (rack A).
    // Greedy wants to move g2 to broker 1 (rack B), but rack preference
    // pins leadership to rack A. The move should be suppressed.
    auto pref = make_rack_preference({{0, "A"}, {1, "B"}}, {"A"});
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0)},
        {{1, 0, {bs(0, 0), bs(1, 0)}}, {2, 0, {bs(0, 0), bs(1, 0)}}},
        {},
        std::move(pref));
    expect_no_move(*balancer);
}

TEST(GreedyLeaderBalancerTest, PinningAllowsMovesToPreferredRack) {
    // 2 groups on topic 0, both led by broker 0 (rack A).
    // Greedy wants to move g2 to broker 1 (rack B). Rack preference
    // includes both A and B, so the move is allowed.
    auto pref = make_rack_preference({{0, "A"}, {1, "B"}}, {"A", "B"});
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0)},
        {{1, 0, {bs(0, 0), bs(1, 0)}}, {2, 0, {bs(0, 0), bs(1, 0)}}},
        {},
        std::move(pref));
    expect_move(
      index,
      *balancer,
      raft::group_id{2},
      model::node_id{0},
      model::node_id{1});
}

TEST(GreedyLeaderBalancerTest, PinningAllowsMovesFromNonPreferredRack) {
    // 2 groups on topic 0, both led by broker 0 (rack B, non-preferred).
    // Greedy wants to move g2 to broker 1 (rack A, preferred). This
    // improves pinning compliance and should be allowed.
    auto pref = make_rack_preference({{0, "B"}, {1, "A"}}, {"A"});
    [[maybe_unused]] auto [index, leaders, locations_by_group, balancer]
      = make_strategy_from_partitions(
        {bs(0, 0), bs(1, 0)},
        {{1, 0, {bs(0, 0), bs(1, 0)}}, {2, 0, {bs(0, 0), bs(1, 0)}}},
        {},
        std::move(pref));
    expect_move(
      index,
      *balancer,
      raft::group_id{2},
      model::node_id{0},
      model::node_id{1});
}
