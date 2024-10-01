/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/commands.h"
#include "cluster/data_migrated_resources.h"
#include "cluster/logger.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/producer_state.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "raft/fundamental.h"
#include "random/generators.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>
#include <fmt/core.h>
#include <gtest/gtest.h>

namespace cluster {
struct test_fixture : public seastar_test {
    test_fixture()
      : leaders(topics) {}

    ss::future<> SetUpAsync() {
        co_await migrated_resources.start();
        co_await topics.start(ss::sharded_parameter(
          [this] { return std::ref(migrated_resources.local()); }));
    }
    ss::future<> TearDownAsync() {
        co_await leaders.stop();
        co_await topics.stop();
        co_await migrated_resources.stop();
    }

    ss::future<size_t> count_leaderless_partitions() {
        size_t leaderless_cnt = 0;
        co_await leaders.for_each_leader([&leaderless_cnt](
                                           model::topic_namespace_view,
                                           model::partition_id,
                                           std::optional<model::node_id> leader,
                                           model::term_id) {
            if (!leader) {
                leaderless_cnt++;
            }
        });
        co_return leaderless_cnt;
    }

    ss::future<> add_topic(model::topic tp, int partition_count) {
        topic_configuration_assignment tca;
        tca.cfg = topic_configuration(
          model::kafka_namespace, tp, partition_count, 1);
        for (auto p = 0; p < partition_count; ++p) {
            tca.assignments.push_back(partition_assignment(
              raft::group_id{_group_id},
              model::partition_id{p},
              {model::broker_shard(model::node_id(0), ss::shard_id(0))}));
        }
        create_topic_cmd cmd(
          model::topic_namespace(model::kafka_namespace, tp), tca);
        co_await topics.local().apply(std::move(cmd), model::offset{0});
    }

    model::timeout_clock::time_point default_deadline() {
        return model::timeout_clock::now() + 2s;
    }
    raft::group_id _group_id{0};
    ss::sharded<topic_table> topics;
    partition_leaders_table leaders;
    ss::sharded<data_migrations::migrated_resources> migrated_resources;
};

TEST_F_CORO(test_fixture, test_counting_leaderless_partitions) {
    auto topic_count = 50;
    auto partitions_per_topic = 10;

    /**
     * We initialize topics table as the leader table verifies the topic
     * existence
     */
    for (int i = 0; i < topic_count; ++i) {
        model::topic tp(fmt::format("t-{}", i));
        co_await add_topic(tp, partitions_per_topic);
    }

    for (int i = 0; i < 10000; ++i) {
        model::ntp ntp(
          model::kafka_namespace,
          model::topic(
            fmt::format("t-{}", random_generators::get_int(topic_count))),
          model::partition_id(
            random_generators::get_int(partitions_per_topic)));

        if (tests::random_bool()) {
            leaders.update_partition_leader(
              ntp, model::term_id(i), tests::random_optional([] {
                  return model::node_id(0);
              }));
        } else {
            leaders.remove_leader(ntp, model::revision_id{0});
        }

        auto expected = co_await count_leaderless_partitions();
        ASSERT_EQ_CORO(expected, leaders.leaderless_partition_count());
    }
}

TEST_F_CORO(test_fixture, test_waiting_for_leader) {
    model::topic tp("test_topic");
    co_await add_topic(tp, 3);
    model::ntp p_0(
      model::kafka_namespace,
      model::topic_partition(tp, model::partition_id(0)));
    model::ntp p_1(
      model::kafka_namespace,
      model::topic_partition(tp, model::partition_id(1)));
    model::ntp p_2(
      model::kafka_namespace,
      model::topic_partition(tp, model::partition_id(2)));

    auto f_1 = leaders.wait_for_leader(p_0, default_deadline(), std::nullopt);

    /**
     * Wait for the same partition leader to see if we can have multiple
     * notifications
     */
    auto f_2 = leaders.wait_for_leader(p_0, default_deadline(), std::nullopt);

    ss::abort_source as;
    auto f_3 = leaders.wait_for_leader(p_1, default_deadline(), as);

    // no leader information is available yet;
    ASSERT_FALSE_CORO(f_1.available());
    ASSERT_FALSE_CORO(f_2.available());
    ASSERT_FALSE_CORO(f_3.available());

    leaders.update_partition_leader(p_0, model::term_id(0), model::node_id(20));
    // after leadership was set futures should be available
    // ASSERT_TRUE_CORO(f_1.available());
    // ASSERT_TRUE_CORO(f_2.available());
    // as f_3 points to different partition it should still be unavailable
    ASSERT_FALSE_CORO(f_3.available());

    auto f_4 = leaders.wait_for_leader(p_0, default_deadline(), std::nullopt);
    ASSERT_TRUE_CORO(f_4.available());

    ASSERT_EQ_CORO(co_await std::move(f_1), model::node_id(20));
    ASSERT_EQ_CORO(co_await std::move(f_2), model::node_id(20));
    ASSERT_EQ_CORO(co_await std::move(f_4), model::node_id(20));
    as.request_abort();

    //  f3 should be aborted
    ASSERT_THROW_CORO(co_await std::move(f_3), ss::abort_requested_exception);
}
using namespace testing;

TEST_F_CORO(test_fixture, test_leadership_notification) {
    model::topic tp("test_topic");
    co_await add_topic(tp, 3);

    model::ntp p_0(
      model::kafka_namespace,
      model::topic_partition(tp, model::partition_id(0)));
    model::ntp p_1(
      model::kafka_namespace,
      model::topic_partition(tp, model::partition_id(1)));
    model::ntp p_2(
      model::kafka_namespace,
      model::topic_partition(tp, model::partition_id(2)));

    absl::flat_hash_map<model::ntp, int> notifies{{p_0, 0}, {p_1, 0}, {p_2, 0}};

    leaders.register_leadership_change_notification(
      [&](const model::ntp& ntp, model::term_id, model::node_id) {
          notifies[ntp] += 1;
      });

    // notification should not be triggered when leader_id is set to nullopt
    leaders.update_partition_leader(p_0, model::term_id{1}, std::nullopt);

    EXPECT_THAT(notifies, Each(Pair(An<model::ntp>(), Eq(0))));

    // notification should be triggered as we have new leader
    leaders.update_partition_leader(
      p_0, model::revision_id{10}, model::term_id{1}, model::node_id(10));
    EXPECT_EQ(notifies[p_0], 1);
    EXPECT_EQ(notifies[p_1], 0);
    EXPECT_EQ(notifies[p_2], 0);
    // notifications for each partition should be independent
    leaders.update_partition_leader(
      p_1, model::revision_id{10}, model::term_id{1}, model::node_id(10));
    EXPECT_EQ(notifies[p_0], 1);
    EXPECT_EQ(notifies[p_1], 1);
    EXPECT_EQ(notifies[p_2], 0);

    // notifications should not be triggered if update was ignored due to
    // out of date revision revision change
    leaders.update_partition_leader(
      p_1, model::revision_id{8}, model::term_id{2}, model::node_id(2));
    EXPECT_EQ(notifies[p_0], 1);
    EXPECT_EQ(notifies[p_1], 1);
    EXPECT_EQ(notifies[p_2], 0);

    // notifications should be triggered when leadership changes 10 -> 5
    leaders.update_partition_leader(
      p_0, model::revision_id{10}, model::term_id{2}, model::node_id(5));
    EXPECT_EQ(notifies[p_0], 2);

    // notifications should be triggered when leader does not change but
    // term does
    leaders.update_partition_leader(
      p_0, model::revision_id{10}, model::term_id{3}, model::node_id(5));
    EXPECT_EQ(notifies[p_0], 3);

    // notifications should be triggered when leader and term does not change
    // but revision does
    leaders.update_partition_leader(
      p_0, model::revision_id{11}, model::term_id{3}, model::node_id(5));
    EXPECT_EQ(notifies[p_0], 4);
    EXPECT_EQ(notifies[p_1], 1);
    EXPECT_EQ(notifies[p_2], 0);
}
} // namespace cluster
