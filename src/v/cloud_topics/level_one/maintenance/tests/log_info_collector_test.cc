/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "cloud_topics/level_one/maintenance/log_info_collector.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cluster/topic_configuration.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/tests/random_batch.h"
#include "utils/tristate.h"

#include <gtest/gtest.h>

#include <chrono>
#include <optional>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace {
cluster::topic_properties
props_with_delete_retention(tristate<std::chrono::milliseconds> dr) {
    cluster::topic_properties p;
    p.delete_retention_ms = dr;
    return p;
}
} // namespace

// While a partition is migrating, tombstones are never removable regardless of
// delete.retention.ms (mirrors the tiered-storage rule).
TEST(TombstoneRemovalUpperBound, MigratingNeverRemoves) {
    const auto now = model::timestamp{1'000'000};
    auto props = props_with_delete_retention(
      tristate<std::chrono::milliseconds>(std::make_optional(1000ms)));
    EXPECT_EQ(
      l1::tombstone_removal_upper_bound(
        props, /*cluster_default=*/5000ms, now, /*migrating=*/true),
      model::timestamp::min());
}

TEST(TombstoneRemovalUpperBound, DisabledRetentionNeverRemoves) {
    const auto now = model::timestamp{1'000'000};
    // Default tristate is disabled.
    auto props = props_with_delete_retention(
      tristate<std::chrono::milliseconds>{});
    EXPECT_EQ(
      l1::tombstone_removal_upper_bound(
        props, 5000ms, now, /*migrating=*/false),
      model::timestamp::min());
}

TEST(TombstoneRemovalUpperBound, TopicOverrideUsed) {
    const auto now = model::timestamp{1'000'000};
    auto props = props_with_delete_retention(
      tristate<std::chrono::milliseconds>(std::make_optional(1000ms)));
    EXPECT_EQ(
      l1::tombstone_removal_upper_bound(
        props, 5000ms, now, /*migrating=*/false),
      now - model::timestamp(1000));
}

TEST(TombstoneRemovalUpperBound, ClusterDefaultUsedWhenUnset) {
    const auto now = model::timestamp{1'000'000};
    // not-set tristate -> fall back to the cluster default.
    auto props = props_with_delete_retention(
      tristate<std::chrono::milliseconds>(
        std::optional<std::chrono::milliseconds>{}));
    EXPECT_EQ(
      l1::tombstone_removal_upper_bound(
        props, 5000ms, now, /*migrating=*/false),
      now - model::timestamp(5000));
    // Cluster default also disabled -> nothing removable.
    EXPECT_EQ(
      l1::tombstone_removal_upper_bound(
        props, std::nullopt, now, /*migrating=*/false),
      model::timestamp::min());
}

class LogInfoCollectorTestFixture : public l1::l1_reader_fixture {};

// A fake topic config provider which always returns a value.
class fake_cfg_provider : public l1::topic_cfg_provider {
public:
    std::optional<std::reference_wrapper<const cluster::topic_configuration>>
    get_topic_cfg(model::topic_namespace_view) const final {
        return _cfg;
    }

private:
    cluster::topic_configuration _cfg{};
};

// A fake offset provider which always returns kafka::offset::max().
class fake_offset_provider : public l1::max_compactible_offset_provider {
public:
    ss::future<> fill_max_compactible_offsets(
      chunked_hash_map<model::ntp, kafka::offset>&) const final {
        co_return;
    }
    ss::future<>
    fill_migrating_ntps(chunked_hash_map<model::ntp, bool>&) const final {
        co_return;
    }
};

TEST_F(LogInfoCollectorTestFixture, TestInfoCollector) {
    auto cfg_provider = std::make_unique<fake_cfg_provider>();
    auto offset_provider = std::make_unique<fake_offset_provider>();
    l1::log_info_collector log_info_collector(
      &_metastore, std::move(cfg_provider), std::move(offset_provider));
    std::vector<std::pair<model::ntp, model::topic_id_partition>> ntidps;
    const auto topic_names = {"topic_a", "topic_b", "topic_c"};
    const auto num_topics = topic_names.size();
    for (const auto& topic : topic_names) {
        ntidps.push_back(make_ntidp(topic));
    }

    std::vector<tidp_batches_t> tidp_batches;
    l1::log_set_t logs;
    l1::log_compaction_queue cached_metadata(
      [](
        const l1::log_compaction_meta_ptr& a,
        const l1::log_compaction_meta_ptr& b) { return a->ntp < b->ntp; });
    l1::log_list_t logs_list;
    for (const auto& [ntp, tidp] : ntidps) {
        auto [it, success] = logs.emplace(
          ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp));
        logs_list.push_back(*it->get());
        auto batches
          = model::test::make_random_batches(model::offset{0}, 10).get();
        tidp_batches.emplace_back(tidp, std::move(batches));
    }

    make_l1_objects(std::move(tidp_batches)).get();
    log_info_collector.collect_info_for_logs(logs, logs_list, cached_metadata)
      .get();
    ASSERT_EQ(cached_metadata.size(), num_topics);
    while (!cached_metadata.empty()) {
        auto sample = cached_metadata.top();
        cached_metadata.pop();
        ASSERT_TRUE(sample->compaction_info_and_ts.has_value());
        ASSERT_FLOAT_EQ(sample->compaction_info_and_ts->info.dirty_ratio, 1.0);
        ASSERT_TRUE(
          sample->compaction_info_and_ts->info.earliest_dirty_ts.has_value());
    }
}

TEST_F(LogInfoCollectorTestFixture, TestSampleLevelingInfo) {
    auto cfg_provider = std::make_unique<fake_cfg_provider>();
    auto offset_provider = std::make_unique<fake_offset_provider>();
    l1::log_info_collector log_info_collector(
      &_metastore, std::move(cfg_provider), std::move(offset_provider));

    auto [ntp, tidp] = make_ntidp("leveling_topic");
    auto log_ptr = ss::make_lw_shared<l1::log_compaction_meta>(tidp, ntp);

    // Seed the metastore with two small objects for this partition. With the
    // default config (max_object_size=80MiB, threshold=0.5 =>
    // min_acceptable=40MiB), each object is undersized. The leveling range
    // builder only emits a range when it sees a run of *two or more*
    // consecutive undersized extents (singletons can't reduce extent count),
    // so we need at least two objects to produce a non-empty range.
    model::offset o{0};
    {
        auto batches = model::test::make_random_batches(o, 10).get();
        o = model::next_offset(batches.back().last_offset());
        std::vector<tidp_batches_t> bs;
        bs.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(bs)).get();
    }

    {
        auto batches = model::test::make_random_batches(o, 10).get();
        std::vector<tidp_batches_t> bs;
        bs.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(bs)).get();
    }

    chunked_vector<l1::log_compaction_meta_ptr> logs;
    logs.push_back(log_ptr);

    log_info_collector.collect_leveling_info(std::move(logs)).get();

    ASSERT_TRUE(log_ptr->leveling_info_and_ts.has_value());
    const auto& ranges = log_ptr->leveling_info_and_ts->info.ranges;
    ASSERT_FALSE(ranges.empty());
    size_t total_size_bytes = 0;
    for (const auto& r : ranges) {
        total_size_bytes += r.size_bytes;
    }
    ASSERT_GT(total_size_bytes, 0u);
}
