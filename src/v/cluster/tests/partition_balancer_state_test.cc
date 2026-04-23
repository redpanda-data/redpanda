/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/commands.h"
#include "cluster/data_migrated_resources.h"
#include "cluster/members_table.h"
#include "cluster/node_status_table.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/mock_property.h"
#include "config/replicas_preference.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "test_utils/test.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <gtest/gtest.h>

namespace {

constexpr uint32_t partitions_per_shard = 7000;
constexpr uint32_t partitions_reserve_shard0 = 2;

struct pb_state_fixture : public seastar_test {
    ss::future<> SetUpAsync() override {
        co_await migrated_resources.start();
        co_await topics.start(
          ss::sharded_parameter(
            [this] { return std::ref(migrated_resources.local()); }),
          model::node_id{1});
        co_await members.start_single();
        co_await features.start();
        co_await allocator.start_single(
          std::ref(members),
          std::ref(features),
          config::mock_binding<std::optional<int32_t>>(std::nullopt),
          config::mock_binding<uint32_t>(uint32_t{partitions_per_shard}),
          config::mock_binding<uint32_t>(uint32_t{partitions_reserve_shard0}),
          config::mock_binding<std::vector<ss::sstring>>({}),
          config::mock_binding<bool>(false));
        allocator.local().register_node(
          std::make_unique<cluster::allocation_node>(
            model::node_id{1},
            8,
            config::mock_binding<uint32_t>(uint32_t{partitions_per_shard}),
            config::mock_binding<uint32_t>(uint32_t{partitions_reserve_shard0}),
            config::mock_binding<std::vector<ss::sstring>>({})));
        co_await node_status.start_single(model::node_id{123});
        co_await pb_state.start_single(
          std::ref(topics),
          std::ref(members),
          std::ref(allocator),
          std::ref(node_status));
    }

    ss::future<> TearDownAsync() override {
        co_await pb_state.stop();
        co_await node_status.stop();
        co_await allocator.stop();
        co_await features.stop();
        co_await members.stop();
        co_await topics.stop();
        co_await migrated_resources.stop();
    }

    model::topic_namespace make_tp_ns(std::string_view tp) {
        return {model::kafka_namespace, model::topic(ss::sstring{tp})};
    }

    ss::future<> create_topic(std::string_view name, int partitions = 1) {
        cluster::topic_configuration cfg(
          model::kafka_namespace,
          model::topic(ss::sstring{name}),
          partitions,
          /*replication_factor=*/1);
        ss::chunked_fifo<cluster::partition_assignment> pas;
        for (int p = 0; p < partitions; ++p) {
            pas.push_back(
              cluster::partition_assignment(
                raft::group_id{_next_group++},
                model::partition_id(p),
                std::vector<model::broker_shard>{{model::node_id{1}, 0}}));
        }
        cluster::topic_configuration_assignment cfg_a(
          std::move(cfg), std::move(pas));
        cluster::create_topic_cmd cmd(make_tp_ns(name), std::move(cfg_a));
        auto ec = co_await topics.local().apply(
          std::move(cmd), model::offset(_next_offset++));
        ASSERT_EQ_CORO(ec, cluster::errc::success);
    }

    ss::future<>
    set_pinning(std::string_view name, std::string_view preference_spec) {
        cluster::incremental_topic_updates updates;
        updates.replicas_preference.op
          = cluster::incremental_update_operation::set;
        updates.replicas_preference.value = config::replicas_preference::parse(
          preference_spec);
        cluster::update_topic_properties_cmd cmd(
          make_tp_ns(name), std::move(updates));
        auto ec = co_await topics.local().apply(
          std::move(cmd), model::offset(_next_offset++));
        ASSERT_EQ_CORO(ec, cluster::errc::success);
    }

    ss::future<> clear_pinning(std::string_view name) {
        cluster::incremental_topic_updates updates;
        updates.replicas_preference.op
          = cluster::incremental_update_operation::remove;
        cluster::update_topic_properties_cmd cmd(
          make_tp_ns(name), std::move(updates));
        auto ec = co_await topics.local().apply(
          std::move(cmd), model::offset(_next_offset++));
        ASSERT_EQ_CORO(ec, cluster::errc::success);
    }

    ss::future<> delete_topic(std::string_view name) {
        cluster::delete_topic_cmd cmd(make_tp_ns(name), make_tp_ns(name));
        auto ec = co_await topics.local().apply(
          std::move(cmd), model::offset(_next_offset++));
        ASSERT_EQ_CORO(ec, cluster::errc::success);
    }

    ss::sharded<cluster::data_migrations::migrated_resources>
      migrated_resources;
    ss::sharded<cluster::topic_table> topics;
    ss::sharded<cluster::members_table> members;
    ss::sharded<features::feature_table> features;
    ss::sharded<cluster::partition_allocator> allocator;
    ss::sharded<cluster::node_status_table> node_status;
    ss::sharded<cluster::partition_balancer_state> pb_state;

    int64_t _next_offset{0};
    int64_t _next_group{1};
};

TEST_F_CORO(pb_state_fixture, pinning_cache_seeds_from_existing_topics) {
    co_await create_topic("pinned");
    co_await create_topic("plain");
    co_await set_pinning("pinned", "racks: rack_A, rack_B");

    auto& state = pb_state.local();

    // Before seeding: cache empty (notifications before seeding are
    // intentionally ignored; the seed reads authoritative state).
    EXPECT_TRUE(state.topics_with_replica_pinning().empty());

    state.ensure_pinning_cache_seeded();

    const auto& pinned = state.topics_with_replica_pinning();
    ASSERT_EQ_CORO(pinned.size(), 1u);
    EXPECT_TRUE(pinned.contains(make_tp_ns("pinned")));
    EXPECT_FALSE(pinned.contains(make_tp_ns("plain")));
}

TEST_F_CORO(pb_state_fixture, pinning_cache_tracks_properties_updated) {
    co_await create_topic("t1");

    auto& state = pb_state.local();
    state.ensure_pinning_cache_seeded();
    EXPECT_TRUE(state.topics_with_replica_pinning().empty());

    // Set pinning — notification must update the cache.
    co_await set_pinning("t1", "racks: rack_A");

    ASSERT_EQ_CORO(state.topics_with_replica_pinning().size(), 1u);
    EXPECT_TRUE(state.topics_with_replica_pinning().contains(make_tp_ns("t1")));

    // Clear pinning via remove op — cache must drop the topic.
    co_await clear_pinning("t1");

    EXPECT_TRUE(state.topics_with_replica_pinning().empty());
}

TEST_F_CORO(pb_state_fixture, pinning_cache_drops_topic_on_delete) {
    co_await create_topic("doomed");
    co_await set_pinning("doomed", "racks: rack_A");

    auto& state = pb_state.local();
    state.ensure_pinning_cache_seeded();
    ASSERT_EQ_CORO(state.topics_with_replica_pinning().size(), 1u);

    co_await delete_topic("doomed");

    EXPECT_TRUE(state.topics_with_replica_pinning().empty());
}

TEST_F_CORO(pb_state_fixture, pinning_cache_reset_forces_reseed) {
    co_await create_topic("t1");
    co_await set_pinning("t1", "racks: rack_A");

    auto& state = pb_state.local();
    state.ensure_pinning_cache_seeded();
    ASSERT_EQ_CORO(state.topics_with_replica_pinning().size(), 1u);

    // Reset clears the cache so callers don't observe stale contents in
    // the window before ensure_pinning_cache_seeded() re-scans.
    state.reset_pinning_cache();
    EXPECT_TRUE(state.topics_with_replica_pinning().empty());

    // Re-seed rebuilds from authoritative topic_table state.
    state.ensure_pinning_cache_seeded();
    ASSERT_EQ_CORO(state.topics_with_replica_pinning().size(), 1u);
    EXPECT_TRUE(state.topics_with_replica_pinning().contains(make_tp_ns("t1")));
}

} // namespace
