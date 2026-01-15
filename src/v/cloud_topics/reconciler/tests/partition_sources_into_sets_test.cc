/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/reconciler/reconciler.h"
#include "cloud_topics/reconciler/tests/test_utils.h"
#include "model/fundamental.h"
#include "model/tests/randoms.h"
#include "utils/tristate.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace cloud_topics;
using namespace std::chrono_literals;
using cloud_topics::reconciler::test::fake_l0_source;

class PartitionSourcesIntoSetsTest : public testing::Test {
public:
    /// Create a source with cleanup_policy=compaction and max_compaction_lag_ms
    ss::shared_ptr<fake_l0_source> make_compact_source(
      std::chrono::milliseconds max_compaction_lag,
      std::optional<model::topic_id> tid = std::nullopt) {
        if (!tid.has_value()) {
            tid = model::create_topic_id();
        }
        auto ntp = model::random_ntp();
        auto tidp = model::topic_id_partition(tid.value(), ntp.tp.partition);

        auto overrides
          = std::make_unique<storage::ntp_config::default_overrides>();
        overrides->cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        overrides->max_compaction_lag_ms = max_compaction_lag;

        storage::ntp_config cfg(ntp, "/fake", std::move(overrides));
        return ss::make_shared<fake_l0_source>(ntp, tidp, std::move(cfg));
    }

    /// Create a source with cleanup_policy=deletion and retention_time
    ss::shared_ptr<fake_l0_source> make_delete_source(
      std::chrono::milliseconds retention,
      std::optional<model::topic_id> tid = std::nullopt) {
        if (!tid.has_value()) {
            tid = model::create_topic_id();
        }
        auto ntp = model::random_ntp();
        auto tidp = model::topic_id_partition(tid.value(), ntp.tp.partition);

        auto overrides
          = std::make_unique<storage::ntp_config::default_overrides>();
        overrides->cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::deletion;
        overrides->retention_time = tristate<std::chrono::milliseconds>(
          retention);

        storage::ntp_config cfg(ntp, "/fake", std::move(overrides));
        return ss::make_shared<fake_l0_source>(ntp, tidp, std::move(cfg));
    }

    /// Create a source with cleanup_policy=compact,delete
    ss::shared_ptr<fake_l0_source> make_compact_delete_source(
      std::chrono::milliseconds max_compaction_lag,
      std::chrono::milliseconds retention,
      std::optional<model::topic_id> tid = std::nullopt) {
        if (!tid.has_value()) {
            tid = model::create_topic_id();
        }
        auto ntp = model::random_ntp();
        auto tidp = model::topic_id_partition(tid.value(), ntp.tp.partition);

        auto overrides
          = std::make_unique<storage::ntp_config::default_overrides>();
        overrides->cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction
            | model::cleanup_policy_bitflags::deletion;
        overrides->max_compaction_lag_ms = max_compaction_lag;
        overrides->retention_time = tristate<std::chrono::milliseconds>(
          retention);

        storage::ntp_config cfg(ntp, "/fake", std::move(overrides));
        return ss::make_shared<fake_l0_source>(ntp, tidp, std::move(cfg));
    }
};

TEST_F(PartitionSourcesIntoSetsTest, EmptySources) {
    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    auto result = reconciler::partition_sources_into_sets(std::move(sources));
    EXPECT_TRUE(result.empty());
}

TEST_F(PartitionSourcesIntoSetsTest, SingleCompactSource) {
    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    sources.push_back(make_compact_source(1h));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].size(), 1);
}

TEST_F(PartitionSourcesIntoSetsTest, SingleDeleteSource) {
    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    sources.push_back(make_delete_source(1h));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].size(), 1);
}

TEST_F(PartitionSourcesIntoSetsTest, SimilarRetentionGroupedTogether) {
    // Sources with the same retention value should be grouped together
    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    sources.push_back(make_delete_source(1h));
    sources.push_back(make_compact_source(1h));
    sources.push_back(make_delete_source(1h));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // All three should be in the same bucket
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].size(), 3);
}

TEST_F(PartitionSourcesIntoSetsTest, DifferentRetentionSeparated) {
    // Sources with very different retention values should be in different
    // buckets
    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    sources.push_back(make_compact_source(1h));
    sources.push_back(make_delete_source(24h)); // 24x different

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // Should be in different buckets
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0].size(), 1);
    EXPECT_EQ(result[1].size(), 1);
}

TEST_F(PartitionSourcesIntoSetsTest, DifferentTopicsWithSameRetentionGrouped) {
    auto tid1 = model::create_topic_id();
    auto tid2 = model::create_topic_id();

    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    // Sources from different topics but same retention
    sources.push_back(make_compact_source(1h, tid1));
    sources.push_back(make_delete_source(1h, tid2));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // Should be in the same bucket (retention-based, not topic-based)
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].size(), 2);
}

TEST_F(PartitionSourcesIntoSetsTest, LogarithmicBucketBoundaries) {
    // Test bucket boundaries with bucket_width=0.5 (~1.65x per bucket)
    // e^0.5 ~ 1.649
    chunked_vector<ss::shared_ptr<reconciler::source>> sources;

    // These should all be in different buckets due to log scaling
    // 1h, ~2.7h (1h * e), ~7.4h (1h * e^2), ~20h (1h * e^3)
    sources.push_back(make_delete_source(1h));
    sources.push_back(make_compact_source(
      std::chrono::milliseconds(
        static_cast<int64_t>(std::chrono::milliseconds(1h).count() * 2.72))));
    sources.push_back(make_delete_source(
      std::chrono::milliseconds(
        static_cast<int64_t>(std::chrono::milliseconds(1h).count() * 7.39))));
    sources.push_back(make_compact_source(
      std::chrono::milliseconds(
        static_cast<int64_t>(std::chrono::milliseconds(1h).count() * 20.09))));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // With bucket_width=0.5, these span multiple buckets
    // Each e^1 jump is 2 buckets (since bucket_width=0.5)
    EXPECT_GE(result.size(), 3);
}

TEST_F(PartitionSourcesIntoSetsTest, CompactAndDeleteTopicsWithSameLifetime) {
    // A compact topic with max_compaction_lag_ms=1day and
    // a delete topic with retention_ms=1day. Both should be grouped together.
    auto compact_topic_id = model::create_topic_id();
    auto delete_topic_id = model::create_topic_id();

    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    sources.push_back(make_compact_source(24h, compact_topic_id));
    sources.push_back(make_delete_source(24h, delete_topic_id));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // Both should be in the same bucket since they have the same lifetime
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].size(), 2);
}

TEST_F(
  PartitionSourcesIntoSetsTest, CompactAndDeleteTopicsWithDifferentLifetimes) {
    // A compact topic with max_compaction_lag_ms=1day and
    // a delete topic with retention_ms=7days. They should be in different
    // buckets.
    auto compact_topic_id = model::create_topic_id();
    auto delete_topic_id = model::create_topic_id();

    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    sources.push_back(make_compact_source(24h, compact_topic_id));
    sources.push_back(make_delete_source(24h * 7, delete_topic_id));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // Should be in different buckets (7x difference = ~4 buckets apart)
    ASSERT_EQ(result.size(), 2);
}

TEST_F(PartitionSourcesIntoSetsTest, MultipleCompactAndDeleteTopicsMixed) {
    // Multiple compact and delete topics with similar lifetimes.
    // All should be grouped together.
    auto compact_topic1 = model::create_topic_id();
    auto compact_topic2 = model::create_topic_id();
    auto delete_topic1 = model::create_topic_id();
    auto delete_topic2 = model::create_topic_id();

    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    // Compact topics with ~1 day max_compaction_lag_ms
    sources.push_back(make_compact_source(24h, compact_topic1));
    sources.push_back(make_compact_source(24h, compact_topic1));
    sources.push_back(make_compact_source(24h, compact_topic2));
    // Delete topics with ~1 day retention_ms
    sources.push_back(make_delete_source(24h, delete_topic1));
    sources.push_back(make_delete_source(24h, delete_topic2));
    sources.push_back(make_delete_source(24h, delete_topic2));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // All 6 partitions should be in the same bucket
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].size(), 6);
}

TEST_F(PartitionSourcesIntoSetsTest, ZeroRetentionFromDifferentTopics) {
    // Sources with 0ms effective_retention (immediate compaction/deletion)
    // from different topics should be grouped together
    auto tid1 = model::create_topic_id();
    auto tid2 = model::create_topic_id();
    auto tid3 = model::create_topic_id();

    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    sources.push_back(make_compact_source(0ms, tid1));
    sources.push_back(make_delete_source(0ms, tid2));
    sources.push_back(make_compact_source(0ms, tid3));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // All should be in the same bucket (bucket 0)
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].size(), 3);
}

TEST_F(PartitionSourcesIntoSetsTest, ZeroRetentionSeparateFromLargeRetention) {
    // 0ms retention should be separate from topics with large retention
    auto zero_tid = model::create_topic_id();
    auto large_tid = model::create_topic_id();

    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    // 0ms retention (immediate)
    sources.push_back(make_compact_source(0ms, zero_tid));
    sources.push_back(make_delete_source(0ms, zero_tid));
    // 1 day retention
    sources.push_back(make_delete_source(24h, large_tid));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // Should be in different buckets
    ASSERT_EQ(result.size(), 2);
}

TEST_F(PartitionSourcesIntoSetsTest, MixOfZeroAndSmallRetention) {
    // 0ms and 1ms should both end up in bucket 0 (log(1) = 0)
    auto tid1 = model::create_topic_id();
    auto tid2 = model::create_topic_id();

    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    sources.push_back(make_compact_source(0ms, tid1));
    sources.push_back(make_delete_source(1ms, tid2));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // Both should be in bucket 0
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].size(), 2);
}

TEST_F(PartitionSourcesIntoSetsTest, RealisticMixOfTopicTypes) {
    // A realistic scenario with multiple topic types:
    // - Short-lived compact topics (1 hour max_compaction_lag)
    // - Medium-lived delete topics (1 day retention)
    // - Long-lived compact topics (7 day max_compaction_lag)
    auto short_compact_tid = model::create_topic_id();
    auto medium_delete_tid = model::create_topic_id();
    auto long_compact_tid = model::create_topic_id();

    chunked_vector<ss::shared_ptr<reconciler::source>> sources;

    // Short-lived compact (1 hour max_compaction_lag_ms)
    sources.push_back(make_compact_source(1h, short_compact_tid));
    sources.push_back(make_compact_source(1h, short_compact_tid));

    // Medium-lived delete (1 day retention_ms)
    sources.push_back(make_delete_source(24h, medium_delete_tid));
    sources.push_back(make_delete_source(24h, medium_delete_tid));

    // Long-lived compact (7 days max_compaction_lag_ms)
    sources.push_back(make_compact_source(24h * 7, long_compact_tid));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // Should have 3 groups:
    // 1. Short-lived (1h) - 2 partitions
    // 2. Medium-lived (1d) - 2 partitions
    // 3. Long-lived (7d) - 1 partition
    ASSERT_EQ(result.size(), 3);

    // Verify total partitions
    size_t total = 0;
    for (const auto& group : result) {
        total += group.size();
    }
    EXPECT_EQ(total, 5);
}

TEST_F(PartitionSourcesIntoSetsTest, CompactDeleteTopicUsesMinimum) {
    // A compact,delete topic should use min(max_compaction_lag, retention)
    // Here compaction_lag=1h, retention=1d, so effective=1h
    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    sources.push_back(make_compact_delete_source(1h, 24h));
    // This compact-only source with 1h should be in the same bucket
    sources.push_back(make_compact_source(1h));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // Both should be in the same bucket (effective retention = 1h)
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].size(), 2);
}

TEST_F(PartitionSourcesIntoSetsTest, CompactDeleteTopicUsesMinimumRetention) {
    // A compact,delete topic where retention is shorter than compaction_lag
    // Here compaction_lag=7d, retention=1d, so effective=1d
    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    sources.push_back(make_compact_delete_source(24h * 7, 24h));
    // This delete-only source with 1d should be in the same bucket
    sources.push_back(make_delete_source(24h));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // Both should be in the same bucket (effective retention = 1d)
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].size(), 2);
}

TEST_F(
  PartitionSourcesIntoSetsTest, CompactDeleteSeparatedByEffectiveLifetime) {
    // Two compact,delete topics with different effective lifetimes
    chunked_vector<ss::shared_ptr<reconciler::source>> sources;
    // effective = min(1h, 7d) = 1h
    sources.push_back(make_compact_delete_source(1h, 24h * 7));
    // effective = min(7d, 1d) = 1d
    sources.push_back(make_compact_delete_source(24h * 7, 24h));

    auto result = reconciler::partition_sources_into_sets(std::move(sources));

    // Should be in different buckets (1h vs 1d)
    ASSERT_EQ(result.size(), 2);
}
