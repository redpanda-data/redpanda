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
#include "cluster/logger.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/producer_state.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "raft/types.h"
#include "random/generators.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>

#include <fmt/core.h>
#include <gtest/gtest.h>

namespace cluster {
struct test_fixture : public seastar_test {
    test_fixture()
      : leaders(topics) {}

    ss::future<> SetUpAsync() { co_await topics.start(); }
    ss::future<> TearDownAsync() {
        co_await leaders.stop();
        co_await topics.stop();
    }

    ss::future<size_t> count_leaderless_partitions() {
        size_t leaderless_cnt = 0;
        co_await leaders.for_each_leader([&leaderless_cnt](
                                           model::topic_namespace_view tp_ns,
                                           model::partition_id pid,
                                           std::optional<model::node_id> leader,
                                           model::term_id term) {
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

    raft::group_id _group_id{0};
    ss::sharded<topic_table> topics;
    partition_leaders_table leaders;
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

} // namespace cluster
