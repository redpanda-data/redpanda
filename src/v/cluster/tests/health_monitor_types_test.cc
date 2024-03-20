
// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf_parser.h"
#include "cluster/health_monitor_types.h"
#include "cluster/tests/health_monitor_test_utils.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "random/generators.h"
#include "serde/async.h"
#include "serde/envelope.h"
#include "serde/rw/rw.h"
#include "test_utils/test.h"
#include "utils/human.h"

#include <fmt/ostream.h>
#include <gtest/gtest.h>

#include <tuple>
#include <utility>
#include <vector>

using namespace cluster;
chunked_vector<topic_status> collect_statuses(const topics_store& store) {
    chunked_vector<topic_status> ret;
    for (const auto& tp : store) {
        chunked_vector<partition_status> partitions;
        for (auto& p : tp.partitions) {
            partitions.push_back(p);
        }

        ret.emplace_back(
          model::topic_namespace(tp.tp_ns), std::move(partitions));
    }
    return ret;
}

TEST(health_monitor_types, test_columnar_format) {
    chunked_vector<topic_status> original;
    original.reserve(10);
    for (int i = 0; i < 10; ++i) {
        original.emplace_back(make_tp_ns(), make_partition_statues(10));
    }

    columnar_node_health_report report;
    for (const auto& topic : original) {
        report.topics.append(topic.tp_ns, topic.partitions);
    }

    auto collected = collect_statuses(report.topics);
    for (auto i = 0; i < original.size(); ++i) {
        for (auto p = 0; p < collected[i].partitions.size(); ++p) {
            ASSERT_EQ(
              collected[i].partitions[p].id, original[i].partitions[p].id);
            ASSERT_EQ(
              collected[i].partitions[p].leader_id,
              original[i].partitions[p].leader_id);
            ASSERT_EQ(
              collected[i].partitions[p].revision_id,
              original[i].partitions[p].revision_id);
            ASSERT_EQ(
              collected[i].partitions[p].term, original[i].partitions[p].term);
            ASSERT_EQ(
              collected[i].partitions[p].under_replicated_replicas,
              original[i].partitions[p].under_replicated_replicas.value_or(0));
            ASSERT_EQ(
              collected[i].partitions[p].size_bytes,
              original[i].partitions[p].size_bytes);
            ASSERT_EQ(
              collected[i].partitions[p].shard,
              original[i].partitions[p].shard);
            ASSERT_EQ(
              collected[i].partitions[p].reclaimable_size_bytes,
              original[i].partitions[p].reclaimable_size_bytes);
        }
    }
}

TEST(health_monitor_types, basic_iteration) {
    auto report = make_columnar_node_health_report(10, 10);
    size_t topics = 0;
    size_t partitions = 0;
    for (auto& tp : report.topics) {
        for (auto& p : tp.partitions) {
            ++partitions;
            (void)(p);
        }
        ++topics;
    }

    EXPECT_EQ(topics, 10);
    EXPECT_EQ(partitions, 100);
}

TEST(health_monitor_types, test_conversion_to_legacy_node_report) {
    auto report = make_columnar_node_health_report(50, 50);
    auto legacy_report = report.materialize_legacy_report();
    auto collected = collect_statuses(report.topics);

    EXPECT_EQ(collected, legacy_report.topics);
}

TEST_CORO(health_monitor_types, serde_roundtrip) {
    std::vector<std::pair<int, int>> params{
      std::make_pair(50000, 3),
      std::make_pair(100, 1500),
      std::make_pair(100, 200)};
    for (auto& [topics, partitions] : params) {
        auto report = make_columnar_node_health_report(topics, partitions);
        auto expected_view = collect_statuses(report.topics);
        iobuf columnar_buffer;
        iobuf legacy_buffer;

        co_await serde::write_async(
          legacy_buffer, report.materialize_legacy_report());
        co_await serde::write_async(columnar_buffer, std::move(report));

        fmt::print(
          "topics: {}, partitions per topic: {}, columnar serialized size: {} "
          "legacy serialized size: {}\n",
          topics,
          partitions,
          human::bytes(columnar_buffer.size_bytes()),
          human::bytes(legacy_buffer.size_bytes()));
        iobuf_parser parser(std::move(columnar_buffer));
        auto deserialized
          = co_await serde::read_async<columnar_node_health_report>(parser);

        ASSERT_EQ_CORO(expected_view, collect_statuses(deserialized.topics));
    }
}

template<typename ColT>
void check_size(ColT& c) {
    iobuf buffer;
    serde::write(buffer, std::move(c));
    fmt::print("column size: {}\n", human::bytes(buffer.size_bytes()));
}

TEST(health_monitor_types, serde_sizes) {
    std::vector<std::pair<size_t, size_t>> params{
      std::make_pair(50000, 3),
      std::make_pair(100, 1500),
      std::make_pair(100, 200)};
    for (auto& [topics, partitions] : params) {
        auto report = make_columnar_node_health_report(topics, partitions);
        auto expected_view = collect_statuses(report.topics);

        iobuf columnar_buffer;
        iobuf legacy_buffer;
        fmt::print(
          "topics: {}, partitions per topic: {}\n", topics, partitions);
        auto fields = report.topics.serde_fields();
        auto buf = serde::to_iobuf(std::get<0>(fields));
        fmt::print("namespaces: {}\n", human::bytes(buf.size_bytes()));
        auto buf_2 = serde::to_iobuf(std::move(std::get<1>(fields)));
        fmt::print("topics: {}\n", human::bytes(buf_2.size_bytes()));
        auto& p_cstore = std::get<2>(fields);
        std::apply(
          [](auto&&... col) { (check_size(col), ...); },
          p_cstore.serde_fields());
    }
}

TEST(health_monitor_types, iteration_types) {
    auto report = make_columnar_node_health_report(5, 5);

    for (auto& tp : report.topics) {
        for (auto& p : tp.partitions) {
            (void)(p);
        }
    }
}

TEST(health_monitor_types, print) {
    auto report = make_columnar_node_health_report(5, 5);

    fmt::print("{}", report);
}
