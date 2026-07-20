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
#include "compaction/reducer.h"
#include "config/property.h"
#include "container/chunked_vector.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "test_utils/async.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/util/log.hh>

#include <gtest/gtest.h>

#include <limits>
#include <numeric>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace {

static ss::logger test_log("leveling_reducer_test");
static prefix_logger test_ctxlog(test_log, "leveling_reducer_test");

// Default upload part size for tests.
static constexpr size_t test_upload_part_size = 64_KiB;

size_t
count_extents(l1::metastore& metastore, const model::topic_id_partition& tidp) {
    auto resp = metastore
                  .get_extent_metadata_forwards(
                    tidp,
                    kafka::offset{0},
                    kafka::offset::max(),
                    std::numeric_limits<size_t>::max(),
                    l1::metastore::include_object_metadata::no)
                  .get();
    return resp.has_value() ? resp->extents.size() : 0;
}

ss::future<> do_level(
  model::ntp ntp,
  model::topic_id_partition tidp,
  chunked_vector<l1::levelable_range> leveling_ranges,
  l1::metastore::compaction_epoch epoch,
  l1::metastore* metastore,
  l1::io* io,
  size_t max_object_size = 128_MiB) {
    ss::abort_source as;
    auto state = l1::compaction_job_state::running;
    l1::compaction_worker_probe probe;

    auto src = std::make_unique<l1::leveling_source>(
      ntp,
      tidp,
      std::move(leveling_ranges),
      metastore,
      io,
      as,
      state,
      test_ctxlog);
    auto sink = std::make_unique<l1::leveling_sink>(
      tidp,
      epoch,
      io,
      metastore,
      as,
      config::mock_binding<size_t>(max_object_size),
      test_upload_part_size,
      probe,
      test_ctxlog);

    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    co_await std::move(reducer).run();
}

} // namespace

class LevelingReducerTest : public l1::l1_reader_fixture {
protected:
    struct batch_stats {
        size_t batch_count{0};
        size_t record_count{0};

        bool operator==(const batch_stats&) const = default;
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

    size_t count_extents(const model::topic_id_partition& tidp) {
        size_t count = 0;
        auto resp = _metastore
                      .get_extent_metadata_forwards(
                        tidp,
                        kafka::offset{0},
                        kafka::offset::max(),
                        std::numeric_limits<size_t>::max(),
                        l1::metastore::include_object_metadata::no)
                      .get();
        if (resp.has_value()) {
            count = resp->extents.size();
        }
        return count;
    }

    // Returns leveling info for `tidp`.
    l1::metastore::leveling_info_response get_all_leveling_info(
      const model::topic_id_partition& tidp, size_t min_acceptable_bytes) {
        chunked_vector<l1::metastore::leveling_info_spec> specs;
        specs.push_back(
          {.tidp = tidp, .min_acceptable_extent_bytes = min_acceptable_bytes});
        auto result = _metastore.get_leveling_infos(specs).get();
        EXPECT_TRUE(result.has_value()) << "get_leveling_infos failed";
        auto it = result->find(tidp);
        EXPECT_NE(it, result->end());
        EXPECT_TRUE(it->second.has_value());
        return std::move(it->second.value());
    }

    // Produces `num_batches` batches of `records_per_batch` fixed-size
    // (1 KiB) records and uploads them as a single L1 object for `tidp`,
    // starting at `base_offset`.
    void upload_batches(
      const model::topic_id_partition& tidp,
      model::offset base_offset,
      int num_batches,
      int records_per_batch = 10) {
        chunked_circular_buffer<model::record_batch> batches;
        auto offset = base_offset;
        for (int i = 0; i < num_batches; ++i) {
            model::test::record_batch_spec spec;
            spec.offset = offset;
            spec.allow_compression = false;
            spec.count = records_per_batch;
            spec.record_sizes = std::vector<size_t>(records_per_batch, 1024);
            auto b = model::test::make_random_batch(spec);
            offset = model::next_offset(b.last_offset());
            batches.push_back(std::move(b));
        }
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }
};

// A run of undersized extents should be consolidated into fewer extents
// after leveling.
TEST_F(LevelingReducerTest, RewritesLevelableRange) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    const int records_per_batch = 10;
    const int batches_per_extent = 5;
    const int num_extents = 4;
    const int records_per_extent = batches_per_extent * records_per_batch;

    for (int i = 0; i < num_extents; ++i) {
        upload_batches(
          tidp,
          model::offset{i * records_per_extent},
          batches_per_extent,
          records_per_batch);
    }

    auto before = count_batches_and_records(ntp, tidp);
    ASSERT_EQ(
      before.batch_count,
      static_cast<size_t>(batches_per_extent * num_extents));
    auto extents_before = count_extents(tidp);
    ASSERT_EQ(extents_before, static_cast<size_t>(num_extents));

    // Each extent is ~55 KiB (50 framed 1 KiB records), ~220 KiB in total:
    // all four are undersized and the run's total clears the threshold.
    auto leveling_info = get_all_leveling_info(tidp, 100_KiB);
    ASSERT_FALSE(leveling_info.ranges.empty());

    do_level(
      ntp,
      tidp,
      std::move(leveling_info.ranges),
      leveling_info.epoch,
      &_metastore,
      &_io)
      .get();

    auto after = count_batches_and_records(ntp, tidp);
    ASSERT_EQ(after, before);

    // Multiple undersized extents should be consolidated.
    auto extents_after = count_extents(tidp);
    ASSERT_LT(extents_after, extents_before);
}

// Every input record must be byte-for-byte present in the output after
// leveling.
TEST_F(LevelingReducerTest, PreservesBatchData) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    const int num_batches = 10;
    const int records_per_batch = 5;

    // Upload multiple small objects so there are leveling ranges.
    for (int i = 0; i < 3; ++i) {
        upload_batches(
          tidp,
          model::offset{i * num_batches * records_per_batch},
          num_batches,
          records_per_batch);
    }

    // Read original records.
    auto orig_reader = make_reader(ntp, tidp);
    auto orig_batches = read_all(std::move(orig_reader));
    ASSERT_GT(orig_batches.size(), 0);

    // Each extent is ~56 KiB (50 framed 1 KiB records), ~170 KiB in total:
    // all three are undersized and the run's total clears the threshold.
    auto leveling_info = get_all_leveling_info(tidp, 100_KiB);
    ASSERT_FALSE(leveling_info.ranges.empty());

    do_level(
      ntp,
      tidp,
      std::move(leveling_info.ranges),
      leveling_info.epoch,
      &_metastore,
      &_io)
      .get();

    // Re-read after leveling.
    auto new_reader = make_reader(ntp, tidp);
    auto new_batches = read_all(std::move(new_reader));

    ASSERT_EQ(orig_batches.size(), new_batches.size());
    for (size_t i = 0; i < orig_batches.size(); ++i) {
        EXPECT_EQ(orig_batches[i].header(), new_batches[i].header())
          << "Batch header mismatch at index " << i;
        EXPECT_EQ(orig_batches[i].data(), new_batches[i].data())
          << "Batch data mismatch at index " << i;
    }
}

// If hard_stop is requested, the source stops iteration immediately and the
// sink does not commit any replacement.
TEST_F(LevelingReducerTest, HardStopPreempts) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    const int records_per_batch = 10;
    const int batches_per_extent = 5;

    // Create two extents.
    for (int i = 0; i < 2; ++i) {
        upload_batches(
          tidp,
          model::offset{i * batches_per_extent * records_per_batch},
          batches_per_extent,
          records_per_batch);
    }

    auto extents_before = count_extents(tidp);
    ASSERT_EQ(extents_before, 2);

    // Each extent is ~55 KiB (50 framed 1 KiB records), ~110 KiB in total:
    // both are undersized and the run's total clears the threshold.
    auto leveling_info = get_all_leveling_info(tidp, 80_KiB);
    ASSERT_FALSE(leveling_info.ranges.empty());

    // Run leveling with state set to hard_stop from the start.
    ss::abort_source as;
    auto state = l1::compaction_job_state::hard_stop;
    l1::compaction_worker_probe probe;

    auto src = std::make_unique<l1::leveling_source>(
      ntp,
      tidp,
      std::move(leveling_info.ranges),
      &_metastore,
      &_io,
      as,
      state,
      test_ctxlog);
    auto sink = std::make_unique<l1::leveling_sink>(
      tidp,
      leveling_info.epoch,
      &_io,
      &_metastore,
      as,
      config::mock_binding<size_t>(128_MiB),
      test_upload_part_size,
      probe,
      test_ctxlog);

    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));
    std::move(reducer).run().get();

    // Extents should be unchanged since no iteration occurred.
    auto extents_after = count_extents(tidp);
    ASSERT_EQ(extents_after, extents_before);
}

// If leveling_ranges is empty, initialize() should return false and the
// reducer should short-circuit without doing any work.
TEST_F(LevelingReducerTest, EmptyLevelingRangesIsNoOp) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    const int num_batches = 10;

    upload_batches(tidp, model::offset{0}, num_batches);

    auto extents_before = count_extents(tidp);
    ASSERT_EQ(extents_before, 1);

    // Use min_acceptable_extent_bytes=0 so no extents are undersized and the
    // response's ranges are empty.
    chunked_vector<l1::metastore::leveling_info_spec> specs;
    specs.push_back({.tidp = tidp, .min_acceptable_extent_bytes = 0});
    auto infos = _metastore.get_leveling_infos(specs).get();
    ASSERT_TRUE(infos.has_value());
    auto leveling_it = infos->find(tidp);
    ASSERT_NE(leveling_it, infos->end());
    ASSERT_TRUE(leveling_it->second.has_value());
    auto& leveling_info = leveling_it->second.value();
    ASSERT_TRUE(leveling_info.ranges.empty());

    // Verify that initialize() returns false without modifying state.
    ss::abort_source as;
    auto state = l1::compaction_job_state::running;
    l1::compaction_worker_probe probe;

    auto src = std::make_unique<l1::leveling_source>(
      ntp,
      tidp,
      std::move(leveling_info.ranges),
      &_metastore,
      &_io,
      as,
      state,
      test_ctxlog);
    auto sink = std::make_unique<l1::leveling_sink>(
      tidp,
      leveling_info.epoch,
      &_io,
      &_metastore,
      as,
      config::mock_binding<size_t>(128_MiB),
      test_upload_part_size,
      probe,
      test_ctxlog);

    bool initialized = sink->initialize(*src).get();
    ASSERT_FALSE(initialized);

    // Extents should be unchanged.
    auto extents_after = count_extents(tidp);
    ASSERT_EQ(extents_after, extents_before);
}

namespace {

// One batch in this test holds a single ~1 KiB record. After batch framing
// plus the amortized per-object footer, each batch contributes roughly
// this much to an L1 object's reported object_size. Used to translate
// byte-valued test inputs (sizes, thresholds) into a batch count when
// uploading, since the sink rolls at batch granularity. The empirical
// value (~1.25 KiB) accounts for record/batch encoding overhead that the
// raw 1 KiB record alone wouldn't capture.
constexpr size_t approx_bytes_per_batch = 1280;

// Per-extent size tolerance is `max(absolute, relative * expected)`:
//   * `shape_tolerance_bytes` (absolute) caps the error introduced by
//     per-object footer (~600 B), per-batch framing variance (~few
//     hundred bytes per batch), and slop at sink rolling boundaries (the
//     sink rolls on file_size, which includes framing, so the data bytes
//     per output extent drift from the prediction by up to ~4 KiB).
//   * `shape_tolerance_ratio` (relative) keeps the bound useful for
//     larger extents where the absolute slop is small relative to
//     expected.
constexpr size_t shape_tolerance_bytes = 5_KiB;
constexpr double shape_tolerance_ratio = 0.05;

struct test_case {
    std::string name;
    // Approximate size in bytes of each L1 object to create, in order.
    // Uploaded as ceil(entry / approx_bytes_per_batch) batches.
    std::vector<size_t> object_sizes_bytes;
    // Below this many bytes, an L1 object is considered undersized by the
    // algorithm.
    size_t min_acceptable_bytes;
    // Target output object size in bytes. Used as the sink's
    // max_object_size, controlling rolling boundaries.
    size_t target_bytes;
    // Expected approximate byte size of each extent after the leveling
    // pipeline runs, in offset order. Each actual extent's object_size
    // must fall within +/- shape_tolerance of the corresponding entry.
    // When no leveling is expected, this should mirror object_sizes_bytes.
    std::vector<size_t> expected_shape_bytes;
};

class LevelingReducerShapeTest
  : public l1::l1_reader_fixture
  , public ::testing::WithParamInterface<test_case> {
protected:
    // Uploads one L1 object made of enough ~1 KiB batches to land near
    // `approx_bytes`. Advances `next_offset` by the batch count.
    void upload_object_of_approx_size(
      const model::topic_id_partition& tidp,
      model::offset& next_offset,
      size_t approx_bytes) {
        // At least one batch even for very small requests, so the object
        // exists in the metastore.
        const int num_batches = std::max<int>(
          1,
          static_cast<int>(
            (approx_bytes + approx_bytes_per_batch - 1)
            / approx_bytes_per_batch));
        chunked_circular_buffer<model::record_batch> batches;
        for (int i = 0; i < num_batches; ++i) {
            model::test::record_batch_spec spec;
            spec.offset = next_offset;
            spec.allow_compression = false;
            spec.count = 1;
            // Use a record size slightly under approx_bytes_per_batch so
            // overall encoded batch size stays close to the target.
            spec.record_sizes = std::vector<size_t>{1024};
            auto b = model::test::make_random_batch(spec);
            next_offset = model::next_offset(b.last_offset());
            batches.push_back(std::move(b));
        }
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, std::move(batches));
        make_l1_objects(std::move(tidp_batches)).get();
    }
};

} // namespace

TEST_P(LevelingReducerShapeTest, ShapeAfterE2ELeveling) {
    const auto& c = GetParam();
    auto [ntp, tidp] = make_ntidp("test_topic");

    model::offset next_offset{0};
    for (size_t bytes : c.object_sizes_bytes) {
        upload_object_of_approx_size(tidp, next_offset, bytes);
    }
    ASSERT_EQ(count_extents(_metastore, tidp), c.object_sizes_bytes.size());

    chunked_vector<l1::metastore::leveling_info_spec> specs;
    specs.push_back(
      {.tidp = tidp, .min_acceptable_extent_bytes = c.min_acceptable_bytes});
    auto infos = _metastore.get_leveling_infos(specs).get();
    ASSERT_TRUE(infos.has_value()) << int(infos.error());
    auto info_it = infos->find(tidp);
    ASSERT_NE(info_it, infos->end());
    ASSERT_TRUE(info_it->second.has_value()) << int(info_it->second.error());
    auto& info_res = info_it->second.value();

    // Snapshot record count before leveling so we can verify preservation.
    size_t records_before = 0;
    {
        auto reader = make_reader(ntp, tidp);
        for (auto& b : read_all(std::move(reader))) {
            records_before += b.record_count();
        }
    }

    if (!info_res.ranges.empty()) {
        do_level(
          ntp,
          tidp,
          std::move(info_res.ranges),
          info_res.epoch,
          &_metastore,
          &_io,
          c.target_bytes)
          .get();
    }

    size_t records_after = 0;
    {
        auto reader = make_reader(ntp, tidp);
        for (auto& b : read_all(std::move(reader))) {
            records_after += b.record_count();
        }
    }
    EXPECT_EQ(records_after, records_before);

    // Verify per-extent shape: actual object_size within +/- shape_tolerance
    // of the case's expected_shape_bytes.
    auto final_extents = _metastore
                           .get_extent_metadata_forwards(
                             tidp,
                             kafka::offset{0},
                             kafka::offset::max(),
                             std::numeric_limits<size_t>::max(),
                             l1::metastore::include_object_metadata::yes)
                           .get();
    ASSERT_TRUE(final_extents.has_value());

    std::vector<size_t> actual_bytes;
    actual_bytes.reserve(final_extents->extents.size());
    for (const auto& ext : final_extents->extents) {
        actual_bytes.push_back(
          ext.object_info.has_value() ? ext.object_info->object_size : 0);
    }
    vlog(
      test_log.info,
      "[{}] expected_shape={}, actual_bytes={}",
      c.name,
      c.expected_shape_bytes,
      actual_bytes);

    ASSERT_EQ(final_extents->extents.size(), c.expected_shape_bytes.size())
      << "extent count mismatch";

    for (size_t i = 0; i < c.expected_shape_bytes.size(); ++i) {
        const auto& ext = final_extents->extents[i];
        ASSERT_TRUE(ext.object_info.has_value());
        const size_t actual = ext.object_info->object_size;
        const size_t expected = c.expected_shape_bytes[i];
        const size_t slop = std::max(
          shape_tolerance_bytes,
          static_cast<size_t>(
            static_cast<double>(expected) * shape_tolerance_ratio));
        const size_t lower = expected > slop ? expected - slop : 0;
        const size_t upper = expected + slop;
        EXPECT_GE(actual, lower)
          << "extent " << i << " too small: actual=" << actual << " expected~"
          << expected << " (slop=" << slop << ")";
        EXPECT_LE(actual, upper)
          << "extent " << i << " too large: actual=" << actual << " expected~"
          << expected << " (slop=" << slop << ")";
    }
}

INSTANTIATE_TEST_SUITE_P(
  Cases,
  LevelingReducerShapeTest,
  ::testing::Values(
    // No undersized extents: algorithm returns no ranges; layout unchanged.
    test_case{
      .name = "NoUndersizedExtents",
      .object_sizes_bytes = {100_KiB, 100_KiB, 100_KiB},
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 100_KiB,
      .expected_shape_bytes = {100_KiB, 100_KiB, 100_KiB},
    },
    // All small, but the run is the partition's tail and its total (~6 KiB)
    // can't fill a healthy output object yet: held back, layout unchanged.
    test_case{
      .name = "AllSmallTailHeldBack",
      .object_sizes_bytes = {2_KiB, 2_KiB, 2_KiB},
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 100_KiB,
      .expected_shape_bytes = {2_KiB, 2_KiB, 2_KiB},
    },
    // All small and the tail run's total (~60 KiB) reaches min_acceptable:
    // consolidated into one healthy object.
    test_case{
      .name = "AllSmallTailCommits",
      .object_sizes_bytes = {20_KiB, 20_KiB, 20_KiB},
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 100_KiB,
      .expected_shape_bytes = {60_KiB},
    },
    // Isolated small with no rewrite savings: range discarded, partition
    // unchanged.
    test_case{
      .name = "IsolatedSmallNoSavings",
      .object_sizes_bytes = {100_KiB, 2_KiB, 100_KiB},
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 100_KiB,
      .expected_shape_bytes = {100_KiB, 2_KiB, 100_KiB},
    },
    // Two small extents wrapped by healthy ones, with one healthy extent
    // between them. The lone small (obj 1) sits alone in its run (K=1,
    // dropped). The {15, 2} pair (objs 3..4) forms a K=2 run consolidated
    // into a single ~17 KiB object. Objs 0, 2, and 5 kept.
    test_case{
      .name = "SmallSandwichedBetweenLarge",
      .object_sizes_bytes = {100_KiB, 2_KiB, 100_KiB, 15_KiB, 2_KiB, 100_KiB},
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 100_KiB,
      .expected_shape_bytes = {100_KiB, 2_KiB, 100_KiB, 17_KiB, 100_KiB},
    },
    // Two smalls each isolated (each is between healthy extents, so each
    // candidate run has K=1 and is dropped). No leveling happens; the
    // partition is unchanged. Guards against false-positive consolidation
    // across healthy extents.
    test_case{
      .name = "TwoSmallsSeparatedByOneLarge",
      .object_sizes_bytes = {100_KiB, 2_KiB, 100_KiB, 100_KiB, 2_KiB, 100_KiB},
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 100_KiB,
      .expected_shape_bytes
      = {100_KiB, 2_KiB, 100_KiB, 100_KiB, 2_KiB, 100_KiB},
    },
    // Two healthy leading extents and one healthy trailing extent flank a
    // K=2 run of smalls (objs 2..3, 4 KiB total). The run is consolidated
    // into a single 4 KiB object. Objs 0, 1, and 4 kept.
    test_case{
      .name = "LeadingHealthyExtentsUntouched",
      .object_sizes_bytes = {100_KiB, 100_KiB, 2_KiB, 2_KiB, 100_KiB},
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 100_KiB,
      .expected_shape_bytes = {100_KiB, 100_KiB, 4_KiB, 100_KiB},
    },
    // Several small extents in the partition, but only the trailing pair
    // of 30 KiB smalls (objs 6..7) forms a K=2 run, consolidated into a
    // single ~60 KiB object. Earlier 30 KiB smalls each sit alone between
    // healthy extents (K=1, dropped). Confirms the builder finds the one
    // valid run when earlier candidates all degenerate to K=1.
    test_case{
      .name = "OneRunAmongIsolatedSmalls",
      .object_sizes_bytes
      = {100_KiB, 30_KiB, 100_KiB, 30_KiB, 100_KiB, 100_KiB, 30_KiB, 30_KiB, 100_KiB},
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 100_KiB,
      .expected_shape_bytes
      = {100_KiB, 30_KiB, 100_KiB, 30_KiB, 100_KiB, 100_KiB, 60_KiB, 100_KiB},
    },
    // A solitary trailing small with no companion: K=1 candidate run
    // dropped by the builder. Partition unchanged.
    test_case{
      .name = "TrailingSmallAlone",
      .object_sizes_bytes = {100_KiB, 100_KiB, 2_KiB},
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 100_KiB,
      .expected_shape_bytes = {100_KiB, 100_KiB, 2_KiB},
    },
    // Long run of small extents (60 of them, 1 KiB each). Each upload
    // becomes one 1-batch L1 object. After consolidation, the new object
    // holds 60 batches — per-batch framing overhead accumulates to about
    // 250 B * 60 ~= 15 KiB on top of the ~60 KiB of record data (also
    // pushing the tail run's total past min_acceptable so it commits).
    // Verifies the sink correctly consolidates a high batch-count range.
    test_case{
      .name = "LongRunOfSmalls",
      .object_sizes_bytes = std::vector<size_t>(60, 1_KiB),
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 100_KiB,
      .expected_shape_bytes = {75_KiB},
    },
    // Three consecutive smalls of varying sizes (2K, 15K, 30K — all under
    // 50K min_acceptable) form a K=3 run, closed by the trailing healthy
    // obj 4. Consolidated into a single ~47 KiB object.
    test_case{
      .name = "VaryingSmallSizes",
      .object_sizes_bytes = {100_KiB, 2_KiB, 15_KiB, 30_KiB, 100_KiB},
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 100_KiB,
      .expected_shape_bytes = {100_KiB, 47_KiB, 100_KiB},
    },
    // Long run of small extents whose consolidated size exceeds the sink's
    // target object size, forcing the sink to roll mid-run and produce
    // multiple output extents.
    test_case{
      .name = "LongRunRollsAcrossExtents",
      .object_sizes_bytes = std::vector<size_t>(80, 1_KiB),
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 50_KiB,
      .expected_shape_bytes = {50_KiB, 48_KiB},
    },
    // Two long runs of smalls separated by a single healthy extent. Each
    // run independently exceeds the target, so the sink rolls mid-run, and
    // the gap at the healthy extent forces an additional roll on the
    // non-contiguous boundary. Verifies that rolling is range-scoped: the
    // healthy extent is left untouched and each run consolidates
    // independently.
    test_case{
      .name = "TwoLongRunsBothRoll",
      .object_sizes_bytes =
        []() {
            std::vector<size_t> v(80, 1_KiB);
            v.push_back(100_KiB);
            v.insert(v.end(), 80, 1_KiB);
            return v;
        }(),
      .min_acceptable_bytes = 50_KiB,
      .target_bytes = 50_KiB,
      .expected_shape_bytes = {50_KiB, 48_KiB, 100_KiB, 50_KiB, 48_KiB},
    }),
  [](const auto& info) { return info.param.name; });
