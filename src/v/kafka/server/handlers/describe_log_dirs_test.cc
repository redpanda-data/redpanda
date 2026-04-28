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
#include "kafka/data/partition_proxy.h"
#include "kafka/data/tests/fake_partition_proxy_impl.h"
#include "kafka/data/tests/fake_partition_proxy_source.h"
#include "kafka/server/handlers/describe_log_dirs.h"
#include "test_utils/test.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using tests::fake_partition_proxy_impl;
using tests::fake_partition_proxy_source;

kafka::partition_proxy make_proxy(
  model::ntp ntp,
  size_t local_size,
  model::offset offset_lag,
  std::optional<size_t> cloud_size) {
    return kafka::partition_proxy(
      std::make_unique<fake_partition_proxy_impl>(
        std::move(ntp), local_size, offset_lag, cloud_size));
}

model::ntp make_ntp(int32_t partition) {
    return model::ntp(
      model::ns("kafka"),
      model::topic("test-topic"),
      model::partition_id(partition));
}

model::ktp make_ktp(std::string_view topic, int32_t partition) {
    return model::ktp(
      model::topic(ss::sstring(topic)), model::partition_id(partition));
}

using kafka::describe_log_dirs::detail::log_partition_data;
using kafka::describe_log_dirs::detail::partition_dir_set;

// Tag a log_partition_data with a recognizable partition_index so merge
// tests can identify which entry came from which input set.
log_partition_data tagged(int32_t partition_index) {
    return log_partition_data{
      .local = kafka::describe_log_dirs_partition{
        .partition_index = partition_index,
      }};
}

std::vector<int32_t> indexes(const chunked_vector<log_partition_data>& v) {
    std::vector<int32_t> out;
    out.reserve(v.size());
    for (const auto& e : v) {
        out.push_back(e.local.partition_index);
    }
    return out;
}

} // namespace

using kafka::describe_log_dirs::detail::describe_partition;

TEST_CORO(DescribePartition, LocalOnlyNoCloudData) {
    auto proxy = make_proxy(make_ntp(7), 1024, model::offset(3), std::nullopt);
    auto data = co_await describe_partition(proxy, /*include_remote=*/true);

    EXPECT_EQ(data.local.partition_index, 7);
    EXPECT_EQ(data.local.partition_size, 1024);
    EXPECT_EQ(data.local.offset_lag, 3);
    EXPECT_FALSE(data.local.is_future_key);
    EXPECT_FALSE(data.remote.has_value());
}

TEST_CORO(DescribePartition, CloudDataButRemoteDisabled) {
    auto proxy = make_proxy(
      make_ntp(0), 100, model::offset(0), std::optional<size_t>(2048));
    auto data = co_await describe_partition(proxy, /*include_remote=*/false);

    EXPECT_EQ(data.local.partition_size, 100);
    EXPECT_FALSE(data.remote.has_value());
}

TEST_CORO(DescribePartition, CloudDataAndRemoteEnabled) {
    auto proxy = make_proxy(
      make_ntp(2), 100, model::offset(5), std::optional<size_t>(2048));
    auto data = co_await describe_partition(proxy, /*include_remote=*/true);

    EXPECT_EQ(data.local.partition_index, 2);
    EXPECT_EQ(data.local.partition_size, 100);
    EXPECT_EQ(data.local.offset_lag, 5);

    ASSERT_TRUE_CORO(data.remote.has_value());
    EXPECT_EQ(data.remote->partition_index, 2);
    EXPECT_EQ(data.remote->partition_size, 2048);
    EXPECT_EQ(data.remote->offset_lag, 5);
    EXPECT_FALSE(data.remote->is_future_key);
}

TEST_CORO(DescribePartition, NegativeOffsetLagClampsAtZero) {
    auto proxy = make_proxy(make_ntp(0), 0, model::offset(-7), std::nullopt);
    auto data = co_await describe_partition(proxy, /*include_remote=*/false);

    EXPECT_EQ(data.local.offset_lag, 0);
}

using kafka::describe_log_dirs::detail::merge_partition_dir_sets;

TEST(MergePartitionDirSets, BothEmpty) {
    auto out = merge_partition_dir_sets({}, {});
    EXPECT_TRUE(out.empty());
}

TEST(MergePartitionDirSets, EmptyAccCarriesUpdateThrough) {
    partition_dir_set update;
    update[model::topic("t1")].push_back(tagged(0));
    update[model::topic("t1")].push_back(tagged(1));

    auto out = merge_partition_dir_sets({}, update);

    ASSERT_EQ(out.size(), 1);
    EXPECT_THAT(indexes(out[model::topic("t1")]), ::testing::ElementsAre(0, 1));
}

TEST(MergePartitionDirSets, EmptyUpdateLeavesAccUnchanged) {
    partition_dir_set acc;
    acc[model::topic("t1")].push_back(tagged(7));

    auto out = merge_partition_dir_sets(std::move(acc), {});

    ASSERT_EQ(out.size(), 1);
    EXPECT_THAT(indexes(out[model::topic("t1")]), ::testing::ElementsAre(7));
}

TEST(MergePartitionDirSets, DisjointTopicsAreUnioned) {
    partition_dir_set acc;
    acc[model::topic("t1")].push_back(tagged(1));

    partition_dir_set update;
    update[model::topic("t2")].push_back(tagged(2));

    auto out = merge_partition_dir_sets(std::move(acc), update);

    ASSERT_EQ(out.size(), 2);
    EXPECT_THAT(indexes(out[model::topic("t1")]), ::testing::ElementsAre(1));
    EXPECT_THAT(indexes(out[model::topic("t2")]), ::testing::ElementsAre(2));
}

TEST(MergePartitionDirSets, OverlappingTopicAppendsUpdateAfterAcc) {
    partition_dir_set acc;
    acc[model::topic("t1")].push_back(tagged(10));
    acc[model::topic("t1")].push_back(tagged(11));

    partition_dir_set update;
    update[model::topic("t1")].push_back(tagged(20));
    update[model::topic("t1")].push_back(tagged(21));

    auto out = merge_partition_dir_sets(std::move(acc), update);

    ASSERT_EQ(out.size(), 1);
    EXPECT_THAT(
      indexes(out[model::topic("t1")]), ::testing::ElementsAre(10, 11, 20, 21));
}

using kafka::describable_log_dir_topic;
using kafka::describe_log_dirs::detail::collect_mapper;

TEST_CORO(CollectMapper, NullFilterEnumeratesAllSourcePartitions) {
    fake_partition_proxy_source source;
    source.add(make_ktp("t1", 0), {.local_size = 100})
      .add(make_ktp("t1", 1), {.local_size = 200})
      .add(make_ktp("t2", 0), {.local_size = 300});

    auto result = co_await collect_mapper(
      source, /*topics=*/nullptr, /*include_remote=*/false);

    ASSERT_EQ_CORO(result.size(), 2);
    EXPECT_THAT(
      indexes(result[model::topic("t1")]), ::testing::ElementsAre(0, 1));
    EXPECT_THAT(indexes(result[model::topic("t2")]), ::testing::ElementsAre(0));
}

TEST_CORO(CollectMapper, EmptyFilterReturnsNothing) {
    fake_partition_proxy_source source;
    source.add(make_ktp("t1", 0), {});

    chunked_vector<describable_log_dir_topic> filter;
    auto result = co_await collect_mapper(
      source, &filter, /*include_remote=*/false);

    EXPECT_TRUE(result.empty());
}

TEST_CORO(CollectMapper, FilterMatchesSubsetOfPartitions) {
    fake_partition_proxy_source source;
    source.add(make_ktp("t1", 0), {.local_size = 100})
      .add(make_ktp("t1", 1), {.local_size = 200})
      .add(make_ktp("t1", 2), {.local_size = 300});

    chunked_vector<describable_log_dir_topic> filter;
    std::vector<int32_t> ids = {0, 2};
    filter.push_back(
      describable_log_dir_topic{
        .topic = model::topic("t1"),
        .partition_index = std::move(ids),
      });

    auto result = co_await collect_mapper(
      source, &filter, /*include_remote=*/false);

    ASSERT_EQ_CORO(result.size(), 1);
    EXPECT_THAT(
      indexes(result[model::topic("t1")]), ::testing::ElementsAre(0, 2));
}

TEST_CORO(CollectMapper, FilterTopicNotInSourceIsSkipped) {
    fake_partition_proxy_source source;
    source.add(make_ktp("t1", 0), {});

    chunked_vector<describable_log_dir_topic> filter;
    std::vector<int32_t> ids = {0};
    filter.push_back(
      describable_log_dir_topic{
        .topic = model::topic("missing"),
        .partition_index = std::move(ids),
      });

    auto result = co_await collect_mapper(
      source, &filter, /*include_remote=*/false);

    EXPECT_TRUE(result.empty());
}

TEST_CORO(CollectMapper, FilterPartitionNotInSourceIsSkipped) {
    fake_partition_proxy_source source;
    source.add(make_ktp("t1", 0), {});

    chunked_vector<describable_log_dir_topic> filter;
    std::vector<int32_t> ids = {0, 5};
    filter.push_back(
      describable_log_dir_topic{
        .topic = model::topic("t1"),
        .partition_index = std::move(ids),
      });

    auto result = co_await collect_mapper(
      source, &filter, /*include_remote=*/false);

    ASSERT_EQ_CORO(result.size(), 1);
    EXPECT_THAT(indexes(result[model::topic("t1")]), ::testing::ElementsAre(0));
}

TEST_CORO(CollectMapper, IncludeRemoteFlagPropagatesToDescribePartition) {
    fake_partition_proxy_source source;
    source.add(
      make_ktp("t1", 0),
      {.local_size = 100, .cloud_size = std::optional<size_t>(2048)});

    auto with = co_await collect_mapper(
      source, /*topics=*/nullptr, /*include_remote=*/true);
    auto without = co_await collect_mapper(
      source, /*topics=*/nullptr, /*include_remote=*/false);

    ASSERT_EQ_CORO(with.size(), 1);
    auto& w = with[model::topic("t1")];
    ASSERT_EQ_CORO(w.size(), 1);
    EXPECT_TRUE(w[0].remote.has_value());

    ASSERT_EQ_CORO(without.size(), 1);
    auto& wo = without[model::topic("t1")];
    ASSERT_EQ_CORO(wo.size(), 1);
    EXPECT_FALSE(wo[0].remote.has_value());
}
