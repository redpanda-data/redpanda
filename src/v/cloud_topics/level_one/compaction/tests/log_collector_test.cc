/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/log_collector.h"
#include "cluster/commands.h"
#include "cluster/data_migrated_resources.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/node_config.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace cloud_topics::l1 {

struct log_collector_test_fixture : public seastar_test {
    ss::future<> SetUpAsync() override {
        co_await _as.start();
        co_await _migrated_resources.start();
        co_await _topics.start(
          ss::sharded_parameter(
            [this] { return std::ref(_migrated_resources.local()); }),
          model::node_id{1});
        co_await _leaders.start_single(std::ref(_topics), std::ref(_as));
    }

    ss::future<> TearDownAsync() override {
        _as.local().request_abort();
        co_await _leaders.stop();
        co_await _topics.stop();
        co_await _migrated_resources.stop();
        co_await _as.stop();
    }

    /// Create a cloud topic that is NOT compacted initially.
    ss::future<> add_non_compacted_cloud_topic(
      model::topic tp, int partition_count, model::topic_id tp_id) {
        cluster::topic_configuration_assignment tca;
        tca.cfg = cluster::topic_configuration(
          model::kafka_namespace, tp, partition_count, 1, tp_id);
        tca.cfg.properties.cloud_topic_enabled = true;
        // Explicitly NOT setting compaction - topic is not compacted.

        for (auto p = 0; p < partition_count; ++p) {
            tca.assignments.push_back(
              cluster::partition_assignment(
                raft::group_id{_group_id},
                model::partition_id{p},
                {model::broker_shard(model::node_id(0), ss::shard_id(0))}));
            ++_group_id;
        }
        cluster::create_topic_cmd cmd(
          model::topic_namespace(model::kafka_namespace, tp), tca);
        co_await _topics.local().apply(std::move(cmd), model::offset{0});
    }

    /// Enable compaction on the topic via update_topic_properties_cmd.
    /// This triggers ntp_delta notifications with properties_updated type.
    ss::future<> enable_compaction_on_topic(model::topic tp) {
        cluster::incremental_topic_updates updates;
        updates.cleanup_policy_bitflags.op
          = cluster::incremental_update_operation::set;
        updates.cleanup_policy_bitflags.value
          = model::cleanup_policy_bitflags::compaction;

        cluster::update_topic_properties_cmd cmd(
          model::topic_namespace(model::kafka_namespace, tp),
          std::move(updates));
        co_await _topics.local().apply(std::move(cmd), model::offset{1});
    }

    model::ntp make_ntp(model::topic tp, model::partition_id pid) {
        return model::ntp(model::kafka_namespace, tp, pid);
    }

    int _group_id{0};
    model::node_id _self{1};
    ss::sharded<cluster::topic_table> _topics;
    ss::sharded<cluster::partition_leaders_table> _leaders;
    ss::sharded<ss::abort_source> _as;
    ss::sharded<cluster::data_migrations::migrated_resources>
      _migrated_resources;
};

/// Test that a properties_updated delta for a compacted cloud topic
/// does NOT manage the partition when the current node is NOT the leader.
TEST_F_CORO(
  log_collector_test_fixture,
  test_properties_update_non_leader_does_not_manage) {
    auto tp = model::topic("tapioca");
    auto tp_id = model::topic_id::create();
    co_await add_non_compacted_cloud_topic(tp, 1, tp_id);
    auto ntp = make_ntp(tp, model::partition_id{0});

    // Set a different node as leader (not _self).
    auto other_leader = model::node_id{99};
    co_await _leaders.local().update_partition_leader(
      ntp, model::revision_id{0}, model::term_id{1}, other_leader);

    // Track whether manage was called.
    bool manage_called = false;

    partition_leader_log_collector collector(
      [&](
        const model::ntp&, const model::topic_id_partition&, std::string_view) {
          manage_called = true;
      },
      [&](model::ntp, std::string_view) {},
      [&](const model::ntp&) { return false; },
      _self,
      &_leaders,
      &_topics);

    co_await collector.start();

    // Enable compaction on the topic. This triggers a properties_updated delta.
    co_await enable_compaction_on_topic(tp);

    // manage_cb should NOT have been called since we are not the leader.
    EXPECT_FALSE(manage_called);

    co_await collector.stop();
}

/// Test that a properties_updated delta for a compacted cloud topic
/// DOES manage the partition when the current node IS the leader.
TEST_F_CORO(log_collector_test_fixture, test_properties_update_leader_manages) {
    auto tp = model::topic("tapioca");
    auto tp_id = model::topic_id::create();
    co_await add_non_compacted_cloud_topic(tp, 1, tp_id);
    auto ntp = make_ntp(tp, model::partition_id{0});

    // Set _self as the leader.
    co_await _leaders.local().update_partition_leader(
      ntp, model::revision_id{0}, model::term_id{1}, _self);

    // Track whether manage was called.
    bool manage_called = false;
    model::ntp managed_ntp;
    model::topic_id_partition managed_tidp;

    partition_leader_log_collector collector(
      [&](
        const model::ntp& n,
        const model::topic_id_partition& tidp,
        std::string_view) {
          manage_called = true;
          managed_ntp = n;
          managed_tidp = tidp;
      },
      [&](model::ntp, std::string_view) {},
      [&](const model::ntp&) { return false; },
      _self,
      &_leaders,
      &_topics);

    co_await collector.start();

    // Enable compaction on the topic. This triggers a properties_updated delta.
    co_await enable_compaction_on_topic(tp);

    // manage_cb SHOULD have been called since we ARE the leader.
    EXPECT_TRUE(manage_called);
    EXPECT_EQ(managed_ntp, ntp);
    EXPECT_EQ(managed_tidp.topic_id, tp_id);
    EXPECT_EQ(managed_tidp.partition, model::partition_id{0});

    co_await collector.stop();
}

/// Test that leadership change manages the partition correctly.
TEST_F_CORO(
  log_collector_test_fixture, test_on_leadership_change_manages_partition) {
    auto tp = model::topic("test_topic_3");
    auto tp_id = model::topic_id::create();
    // Create a compacted cloud topic directly.
    cluster::topic_configuration_assignment tca;
    tca.cfg = cluster::topic_configuration(
      model::kafka_namespace, tp, 1, 1, tp_id);
    tca.cfg.properties.cloud_topic_enabled = true;
    tca.cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    tca.assignments.push_back(
      cluster::partition_assignment(
        raft::group_id{_group_id++},
        model::partition_id{0},
        {model::broker_shard(model::node_id(0), ss::shard_id(0))}));
    cluster::create_topic_cmd cmd(
      model::topic_namespace(model::kafka_namespace, tp), tca);
    co_await _topics.local().apply(std::move(cmd), model::offset{0});

    auto ntp = make_ntp(tp, model::partition_id{0});

    // Track whether manage was called.
    bool manage_called = false;
    model::ntp managed_ntp;

    partition_leader_log_collector collector(
      [&](
        const model::ntp& n,
        const model::topic_id_partition&,
        std::string_view) {
          manage_called = true;
          managed_ntp = n;
      },
      [&](model::ntp, std::string_view) {},
      [&](const model::ntp&) { return false; },
      _self,
      &_leaders,
      &_topics);

    co_await collector.start();

    // Update leader to _self, which should trigger the leadership notification.
    co_await _leaders.local().update_partition_leader(
      ntp, model::revision_id{0}, model::term_id{1}, _self);

    // manage_cb SHOULD have been called due to leadership notification.
    EXPECT_TRUE(manage_called);
    EXPECT_EQ(managed_ntp, ntp);

    co_await collector.stop();
}

/// Test that disabling compaction on a managed partition triggers unmanage.
TEST_F_CORO(
  log_collector_test_fixture, test_disable_compaction_unmanages_partition) {
    auto tp = model::topic("test_topic_4");
    auto tp_id = model::topic_id::create();
    // Create a compacted cloud topic directly.
    cluster::topic_configuration_assignment tca;
    tca.cfg = cluster::topic_configuration(
      model::kafka_namespace, tp, 1, 1, tp_id);
    tca.cfg.properties.cloud_topic_enabled = true;
    tca.cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    tca.assignments.push_back(
      cluster::partition_assignment(
        raft::group_id{_group_id++},
        model::partition_id{0},
        {model::broker_shard(model::node_id(0), ss::shard_id(0))}));
    cluster::create_topic_cmd cmd(
      model::topic_namespace(model::kafka_namespace, tp), tca);
    co_await _topics.local().apply(std::move(cmd), model::offset{0});

    auto ntp = make_ntp(tp, model::partition_id{0});

    // Set _self as the leader.
    co_await _leaders.local().update_partition_leader(
      ntp, model::revision_id{0}, model::term_id{1}, _self);

    // Track unmanage calls.
    bool unmanage_called = false;
    model::ntp unmanaged_ntp;

    // Track if the partition is managed.
    bool is_managed = true;

    partition_leader_log_collector collector(
      [&](
        const model::ntp&, const model::topic_id_partition&, std::string_view) {
      },
      [&](model::ntp n, std::string_view) {
          unmanage_called = true;
          unmanaged_ntp = n;
          is_managed = false;
      },
      [&](const model::ntp&) { return is_managed; },
      _self,
      &_leaders,
      &_topics);

    co_await collector.start();

    // Disable compaction on the topic.
    cluster::incremental_topic_updates updates;
    updates.cleanup_policy_bitflags.op
      = cluster::incremental_update_operation::set;
    updates.cleanup_policy_bitflags.value
      = model::cleanup_policy_bitflags::deletion;

    cluster::update_topic_properties_cmd update_cmd(
      model::topic_namespace(model::kafka_namespace, tp), std::move(updates));
    co_await _topics.local().apply(std::move(update_cmd), model::offset{1});

    // unmanage_cb SHOULD have been called since compaction was disabled.
    EXPECT_TRUE(unmanage_called);
    EXPECT_EQ(unmanaged_ntp, ntp);

    co_await collector.stop();
}

} // namespace cloud_topics::l1
