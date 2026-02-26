/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "cloud_topics/level_one/maintenance/leveling/leveling_sink.h"
#include "cloud_topics/level_one/maintenance/leveling/leveling_source.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/worker_probe.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "compaction/reducer.h"
#include "config/property.h"
#include "container/chunked_vector.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "test_utils/async.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <numeric>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace {

ss::future<> do_level(
  model::ntp ntp,
  model::topic_id_partition tidp,
  chunked_vector<l1::offset_interval_set::interval> leveling_ranges,
  l1::metastore* metastore,
  l1::io* io) {
    ss::abort_source as;
    auto state = l1::maintenance_job_state::running;
    l1::maintenance_worker_probe probe;

    auto src = std::make_unique<l1::leveling_source>(
      ntp, tidp, std::move(leveling_ranges), metastore, io, as, state, probe);
    auto sink = std::make_unique<l1::leveling_sink>(
      tidp, io, metastore, as, config::mock_binding<size_t>(128_MiB));

    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    co_await std::move(reducer).run();
}

} // namespace

class LevelingReducerTestFixture : public l1::l1_reader_fixture {
protected:
    struct batch_stats {
        size_t batch_count{0};
        size_t record_count{0};
    };

    batch_stats count_batches_and_records(
      const model::ntp& ntp, const model::topic_id_partition& tidp) {
        auto reader = make_reader(ntp, tidp);
        auto batches = read_all(std::move(reader));
        batch_stats stats;
        stats.batch_count = batches.size();
        stats.record_count = std::accumulate(
          batches.begin(),
          batches.end(),
          size_t{0},
          [](size_t acc, model::record_batch& b) {
              return acc + b.record_count();
          });
        return stats;
    }

    // Count the number of extents in the metastore for the given tidp.
    size_t count_extents(const model::topic_id_partition& tidp) {
        size_t count = 0;
        auto resp
          = _metastore
              .get_extent_metadata_forwards(
                tidp,
                kafka::offset{0},
                kafka::offset::max(),
                std::numeric_limits<size_t>::max(),
                cloud_topics::l1::metastore::include_object_metadata::no)
              .get();
        if (resp.has_value()) {
            count = resp->extents.size();
        }
        return count;
    }
};

TEST_F(LevelingReducerTestFixture, LevelingMultipleUndersizedExtents) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    int num_batches_per_extent = 5;
    int num_extents = 4;
    // Each batch has a random number of records; use a fixed records_per_batch
    // to get predictable offsets.
    int records_per_batch = 10;
    int records_per_extent = num_batches_per_extent * records_per_batch;

    for (int i = 0; i < num_extents; ++i) {
        auto start = model::offset{i * records_per_extent};
        auto batches = model::test::make_random_batches(
                         start,
                         num_batches_per_extent,
                         false,
                         std::nullopt,
                         records_per_batch)
                         .get();
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto before = count_batches_and_records(ntp, tidp);
    ASSERT_EQ(
      before.batch_count,
      static_cast<size_t>(num_batches_per_extent * num_extents));
    auto extents_before = count_extents(tidp);
    ASSERT_EQ(extents_before, static_cast<size_t>(num_extents));

    auto info_spec = l1::metastore::leveling_info_spec{
      .tidp = tidp,
      .min_acceptable_object_size = std::numeric_limits<size_t>::max(),
      .removed_data_threshold = 1.0};
    auto leveling_info = _metastore.get_leveling_info(info_spec).get();
    ASSERT_TRUE(leveling_info.has_value());
    ASSERT_FALSE(leveling_info->leveling_ranges.empty());

    do_level(
      ntp, tidp, leveling_info->leveling_ranges.to_vec(), &_metastore, &_io)
      .get();

    auto after = count_batches_and_records(ntp, tidp);
    ASSERT_EQ(after.record_count, before.record_count);
    ASSERT_EQ(after.batch_count, before.batch_count);

    // Multiple undersized extents should be consolidated into fewer extents.
    auto extents_after = count_extents(tidp);
    ASSERT_LT(extents_after, extents_before);
}

TEST_F(LevelingReducerTestFixture, LevelingPartialRanges) {
    // Create 4 extents: 2 with many batches (large), 2 with few batches
    // (small). Only the small ones should appear in leveling ranges when
    // we set the threshold between the two sizes.
    auto [ntp, tidp] = make_ntidp("test_topic");
    int large_batches = 50;
    int small_batches = 2;
    int records_per_batch = 10;
    int next_offset = 0;

    auto make_extent = [&](int count) {
        auto start = model::offset{next_offset};
        auto batches = model::test::make_random_batches(
                         start, count, false, std::nullopt, records_per_batch)
                         .get();
        next_offset += count * records_per_batch;
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    };

    make_extent(large_batches);
    make_extent(small_batches);
    make_extent(large_batches);
    make_extent(small_batches);

    auto before = count_batches_and_records(ntp, tidp);
    auto total_batches = 2 * large_batches + 2 * small_batches;
    ASSERT_EQ(before.batch_count, static_cast<size_t>(total_batches));
    // 4 extents: large, small, large, small
    auto extents_before = count_extents(tidp);
    ASSERT_EQ(extents_before, 4);

    auto info_spec = l1::metastore::leveling_info_spec{
      .tidp = tidp,
      .min_acceptable_object_size = std::numeric_limits<size_t>::max(),
      .removed_data_threshold = 1.0};
    auto leveling_info = _metastore.get_leveling_info(info_spec).get();
    ASSERT_TRUE(leveling_info.has_value());
    ASSERT_FALSE(leveling_info->leveling_ranges.empty());

    do_level(
      ntp, tidp, leveling_info->leveling_ranges.to_vec(), &_metastore, &_io)
      .get();

    auto after = count_batches_and_records(ntp, tidp);
    ASSERT_EQ(after.batch_count, before.batch_count);
    ASSERT_EQ(after.record_count, before.record_count);

    // Extents should be consolidated after leveling.
    auto extents_after = count_extents(tidp);
    ASSERT_LT(extents_after, extents_before);
}

TEST_F(LevelingReducerTestFixture, LevelingNoRanges) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    int num_batches = 10;

    auto batches = model::test::make_random_batches(
                     model::offset{0}, num_batches, false)
                     .get();
    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // Use min_acceptable_object_size=0 so no objects are undersized, and
    // removed_data_threshold=1.0 so no objects are fragmented.
    auto info_spec = l1::metastore::leveling_info_spec{
      .tidp = tidp,
      .min_acceptable_object_size = 0,
      .removed_data_threshold = 1.0};
    auto leveling_info = _metastore.get_leveling_info(info_spec).get();
    ASSERT_TRUE(leveling_info.has_value());
    ASSERT_TRUE(leveling_info->leveling_ranges.empty());

    // With empty ranges, do_level should complete immediately with no work.
    do_level(
      ntp, tidp, leveling_info->leveling_ranges.to_vec(), &_metastore, &_io)
      .get();

    // Data should be untouched.
    auto stats = count_batches_and_records(ntp, tidp);
    ASSERT_EQ(stats.batch_count, static_cast<size_t>(num_batches));
    ASSERT_GT(stats.record_count, 0);
}

TEST_F(LevelingReducerTestFixture, LevelingPreservesCompressedData) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    int num_batches = 10;

    auto batches = model::test::make_random_batches(
                     model::offset{0}, num_batches, true)
                     .get();
    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    auto before = count_batches_and_records(ntp, tidp);
    ASSERT_EQ(before.batch_count, num_batches);
    ASSERT_GT(before.record_count, 0);

    auto info_spec = l1::metastore::leveling_info_spec{
      .tidp = tidp,
      .min_acceptable_object_size = std::numeric_limits<size_t>::max(),
      .removed_data_threshold = 1.0};
    auto leveling_info = _metastore.get_leveling_info(info_spec).get();
    ASSERT_TRUE(leveling_info.has_value());
    ASSERT_FALSE(leveling_info->leveling_ranges.empty());

    do_level(
      ntp, tidp, leveling_info->leveling_ranges.to_vec(), &_metastore, &_io)
      .get();

    auto after = count_batches_and_records(ntp, tidp);
    ASSERT_EQ(after.batch_count, before.batch_count);
    ASSERT_EQ(after.record_count, before.record_count);
}
