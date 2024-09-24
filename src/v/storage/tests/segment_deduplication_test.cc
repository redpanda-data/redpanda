// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "gmock/gmock.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "storage/chunk_cache.h"
#include "storage/disk_log_impl.h"
#include "storage/key_offset_map.h"
#include "storage/segment_deduplication_utils.h"
#include "storage/segment_utils.h"
#include "storage/tests/disk_log_builder_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/types.h"
#include "test_utils/test.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <stdexcept>

using namespace storage;
using namespace std::chrono_literals;

namespace {
ss::abort_source never_abort;
ss::sharded<features::feature_table> feature_table;
} // anonymous namespace

// Builds a segment layout:
// [0    9][10   19][20    29]...
void add_segments(
  storage::disk_log_builder& b,
  int num_segs,
  int records_per_seg = 10,
  int start_offset = 0,
  bool mark_compacted = true,
  bool may_have_tombstones = true,
  std::optional<model::timestamp> clean_compacted_ts = std::nullopt) {
    auto& disk_log = b.get_disk_log_impl();
    for (int i = 0; i < num_segs; i++) {
        auto offset = start_offset + i * records_per_seg;
        b | add_segment(offset)
          | add_random_batch(
            offset, records_per_seg, maybe_compress_batches::yes);
    }
    for (auto& seg : disk_log.segments()) {
        if (mark_compacted) {
            seg->mark_as_finished_self_compaction();
            seg->mark_as_finished_windowed_compaction();
        }

        seg->index().set_may_have_tombstone_records(may_have_tombstones);

        if (clean_compacted_ts.has_value()) {
            seg->index().maybe_set_clean_compact_timestamp(
              clean_compacted_ts.value());
        }
        if (seg->has_appender()) {
            seg->appender().close().get();
            seg->release_appender();
        }
    }
}

void build_segments(
  storage::disk_log_builder& b,
  int num_segs,
  int records_per_seg = 10,
  int start_offset = 0,
  bool mark_compacted = true,
  bool may_have_tombstones = true,
  std::optional<model::timestamp> clean_compacted_ts = std::nullopt) {
    b | start();
    add_segments(
      b,
      num_segs,
      records_per_seg,
      start_offset,
      mark_compacted,
      may_have_tombstones,
      clean_compacted_ts);
}

TEST(FindSlidingRangeTest, TestCollectSegments) {
    storage::disk_log_builder b;
    build_segments(b, 3);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    for (int start = 0; start < 30; start += 5) {
        for (int end = start; end < 30; end += 5) {
            compaction_config cfg(
              model::offset{end},
              std::nullopt,
              ss::default_priority_class(),
              never_abort);
            auto segs = disk_log.find_sliding_range(cfg, model::offset{start});
            if (end - start < 10) {
                // If the compactible range isn't a full segment, we can't
                // compact anything. We only care about full segments.
                ASSERT_EQ(segs.size(), 0);
                continue;
            }
            // We can't compact partial segments so we round the end down to
            // the nearest segment boundary.
            ASSERT_EQ((end - (end % 10) - start) / 10, segs.size())
              << ssx::sformat("{} to {}: {}", start, end, segs.size());
        }
    }
}

TEST(FindSlidingRangeTest, TestCollectExcludesPrevious) {
    storage::disk_log_builder b;
    build_segments(b, 3);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    compaction_config cfg(
      model::offset{30},
      std::nullopt,
      ss::default_priority_class(),
      never_abort);
    auto segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(3, segs.size());
    ASSERT_EQ(segs.front()->offsets().get_base_offset(), model::offset{0});

    // Let's pretend the previous compaction indexed offsets [20, 30).
    // Subsequent compaction should ignore that last segment.
    disk_log.set_last_compaction_window_start_offset(model::offset(20));
    segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(2, segs.size());
    ASSERT_EQ(segs.front()->offsets().get_base_offset(), model::offset{0});

    disk_log.set_last_compaction_window_start_offset(model::offset(10));
    segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(1, segs.size());
    ASSERT_EQ(segs.front()->offsets().get_base_offset(), model::offset{0});
}

TEST(FindSlidingRangeTest, TestCollectOneRecordSegments) {
    storage::disk_log_builder b;
    build_segments(
      b,
      /*num_segs=*/5,
      /*records_per_seg=*/1,
      /*start_offset=*/0,
      /*mark_compacted=*/false);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    compaction_config cfg(
      model::offset{30},
      std::nullopt,
      ss::default_priority_class(),
      never_abort);
    auto segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(5, segs.size());

    // These segments are considered to have compactible records.
    for (const auto& seg : segs) {
        ASSERT_TRUE(seg->may_have_compactible_records());
    }

    // Add some segments with multiple records. They should be eligible for
    // compaction and are also included in the range.
    add_segments(
      b,
      /*num_segs=*/3,
      /*records_per_seg=*/2,
      /*start_offset=*/6,
      /*mark_compacted=*/false);
    segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(8, segs.size());
    for (const auto& seg : segs) {
        ASSERT_TRUE(seg->may_have_compactible_records());
    }
}

TEST(FindSlidingRangeTest, TestPlaceholderBatchesNoCompactibleRecords) {
    storage::disk_log_builder b;
    b | start();
    const int num_placeholder_batches = 3;
    std::vector<int> offsets = {10, 17, 25};
    ASSERT_EQ(offsets.size(), num_placeholder_batches);
    for (int i = 0; i < num_placeholder_batches; ++i) {
        auto placeholder_batch = model::test::make_random_batch(
          model::offset{offsets[i]},
          2,
          false,
          model::record_batch_type::compaction_placeholder);
        b | add_segment(offsets[i]) | add_batch(std::move(placeholder_batch));
    }
    auto& disk_log = b.get_disk_log_impl();
    auto cleanup = ss::defer([&] { b.stop().get(); });
    compaction_config cfg(
      model::offset{30},
      std::nullopt,
      ss::default_priority_class(),
      never_abort);

    ASSERT_EQ(disk_log.segment_count(), num_placeholder_batches);

    // None of the segments should be included in the sliding range.
    auto segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(0, segs.size());

    for (const auto& seg : disk_log.segments()) {
        ASSERT_FALSE(seg->may_have_compactible_records());
    }
}

TEST(FindSlidingRangeTest, TestEmptySegmentNoCompactibleRecords) {
    storage::disk_log_builder b;
    b | start();
    b | add_segment(0);
    auto& disk_log = b.get_disk_log_impl();
    auto cleanup = ss::defer([&] { b.stop().get(); });
    compaction_config cfg(
      model::offset{30},
      std::nullopt,
      ss::default_priority_class(),
      never_abort);

    ASSERT_EQ(disk_log.segment_count(), 1);

    // The single closed, empty segment shouldn't be included in the sliding
    // range.
    auto segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(0, segs.size());

    for (const auto& seg : disk_log.segments()) {
        ASSERT_FALSE(seg->may_have_compactible_records());
    }
}

TEST(FindSlidingRangeTest, TestAllCleanlyCompactedSegments) {
    storage::disk_log_builder b;
    const auto num_segs = 3;
    // Mark as compacted, do not have tombstones, and cleanly compacted at a
    // previous timestamp.
    build_segments(b, num_segs, 10, 0, true, false, model::timestamp{0});
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    compaction_config cfg(
      model::offset{30}, 1ms, ss::default_priority_class(), never_abort);
    auto segs = disk_log.find_sliding_range(cfg, model::offset{0});
    // All cleanly compacted segments are still considered in the range.
    ASSERT_EQ(segs.size(), num_segs);
}

TEST(FindSlidingRangeTest, TestCompactionLastSegmentNotCompacted) {
    storage::disk_log_builder b;
    const auto num_segs = 3;
    // Mark as not compacted.
    build_segments(b, num_segs, 10, 0, false);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    compaction_config cfg(
      model::offset{30},
      std::nullopt,
      ss::default_priority_class(),
      never_abort);
    auto segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(3, segs.size());
    ASSERT_EQ(segs.front()->offsets().get_base_offset(), model::offset{0});

    // Set the last window start offset to 20. Now, even though the last segment
    // in the group is marked as not compacted, it still will not be considered
    // in the window.
    disk_log.set_last_compaction_window_start_offset(model::offset(20));
    segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(2, segs.size());

    // Reset the last window start offset, and now all segments are once again
    // considered in the window.
    disk_log.set_last_compaction_window_start_offset(std::nullopt);
    segs = disk_log.find_sliding_range(cfg);
    ASSERT_EQ(3, segs.size());
}

TEST(FindSlidingRangeTest, TestWindowWithRemovedSegments) {
    storage::disk_log_builder b;
    const auto num_segs = 3;
    // Mark as not compacted
    build_segments(b, num_segs, 10, 0, false);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();

    // Set the last compaction window start offset, then remove a segment from
    // the log such that start offset < the log's base offset.
    disk_log.set_last_compaction_window_start_offset(model::offset(5));
    disk_log.segments().pop_front();

    compaction_config cfg(
      model::offset{30}, 1ms, ss::default_priority_class(), never_abort);
    auto segs = disk_log.find_sliding_range(cfg, model::offset{0});

    // We should have reset the compaction window start offset, and had the
    // remaining two segments in the sliding range.
    ASSERT_EQ(segs.size(), 2);
    ASSERT_FALSE(
      disk_log.get_last_compaction_window_start_offset().has_value());
}

TEST(FindSlidingRangeTest, TestWindowWithTruncatedSegments) {
    storage::disk_log_builder b;
    const auto num_segs = 3;
    // Mark as not compacted
    build_segments(b, num_segs, 10, 0, false);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();

    // Set the last compaction window start offset, then prefix truncate the log
    // such that start offset < the log's base offset.
    disk_log.set_last_compaction_window_start_offset(model::offset(5));
    truncate_prefix_config trunc_cfg(
      model::offset{10}, ss::default_priority_class());
    disk_log.truncate_prefix(trunc_cfg).get();

    compaction_config cfg(
      model::offset{30}, 1ms, ss::default_priority_class(), never_abort);
    auto segs = disk_log.find_sliding_range(cfg, model::offset{0});

    // We should have reset the compaction window start offset, and had the
    // remaining two segments in the sliding range.
    ASSERT_EQ(segs.size(), 2);
    ASSERT_FALSE(
      disk_log.get_last_compaction_window_start_offset().has_value());
}

TEST(BuildOffsetMap, TestBuildSimpleMap) {
    ss::smp::invoke_on_all([] {
        config::shard_local_cfg().disable_metrics.set_value(true);
        config::shard_local_cfg().disable_public_metrics.set_value(true);
    }).get();
    auto defer_config_reset = ss::defer([] {
        ss::smp::invoke_on_all([] {
            config::shard_local_cfg().disable_metrics.reset();
            config::shard_local_cfg().disable_public_metrics.reset();
        }).get();
    });

    storage::disk_log_builder b;
    build_segments(b, 3);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    auto& segs = disk_log.segments();
    compaction_config cfg(
      model::offset{30},
      std::nullopt,
      ss::default_priority_class(),
      never_abort);
    probe pb;

    feature_table.start().get();
    feature_table
      .invoke_on_all(
        [](features::feature_table& f) { f.testing_activate_all(); })
      .get();
    auto defer = ss::defer([] { feature_table.stop().get(); });

    // Self-compact each segment so we're left with compaction indices. This is
    // a requirement to build the offset map.
    for (auto& seg : segs) {
        storage::internal::self_compact_segment(
          seg,
          disk_log.stm_manager(),
          cfg,
          pb,
          disk_log.readers(),
          disk_log.resources(),
          feature_table)
          .get();
    }

    // Build a map, configuring it to hold too little data for even a single
    // segment.
    simple_key_offset_map too_small_map(5);
    ASSERT_THAT(
      [&] {
          build_offset_map(
            cfg,
            segs,
            disk_log.stm_manager(),
            disk_log.resources(),
            disk_log.get_probe(),
            too_small_map)
            .get();
      },
      testing::ThrowsMessage<std::runtime_error>(
        testing::HasSubstr("Couldn't index")));

    // Now configure a map to index some segments.
    simple_key_offset_map partial_map(15);
    auto partial_o = build_offset_map(
                       cfg,
                       segs,
                       disk_log.stm_manager(),
                       disk_log.resources(),
                       disk_log.get_probe(),
                       partial_map)
                       .get();
    ASSERT_GT(partial_o(), 0);

    // Now make it large enough to index all segments.
    simple_key_offset_map all_segs_map(100);
    auto all_segs_o = build_offset_map(
                        cfg,
                        segs,
                        disk_log.stm_manager(),
                        disk_log.resources(),
                        disk_log.get_probe(),
                        all_segs_map)
                        .get();
    ASSERT_EQ(all_segs_o(), 0);
}

TEST(BuildOffsetMap, TestBuildMapWithMissingCompactedIndex) {
    storage::disk_log_builder b;
    build_segments(b, 3);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    auto& segs = disk_log.segments();
    compaction_config cfg(
      model::offset{30},
      std::nullopt,
      ss::default_priority_class(),
      never_abort);
    for (const auto& s : segs) {
        auto idx_path = s->path().to_compacted_index();
        ASSERT_FALSE(ss::file_exists(idx_path.string()).get());
    }

    // Proceed to window compaction without building any compacted indexes.
    // When building the map, we should attempt to rebuild the index if it
    // doesn't exist.
    simple_key_offset_map missing_index_map(100);
    auto o = build_offset_map(
               cfg,
               segs,
               disk_log.stm_manager(),
               disk_log.resources(),
               disk_log.get_probe(),
               missing_index_map)
               .get();
    ASSERT_EQ(o(), 0);
    ASSERT_EQ(missing_index_map.size(), 30);
    for (const auto& s : segs) {
        auto idx_path = s->path().to_compacted_index();
        ASSERT_TRUE(ss::file_exists(idx_path.string()).get());
    }
}

// Regression test that ensures that segment deduplication doesn't crash
// Redpanda when it hits an error on the read path.
TEST(DeduplicateSegmentsTest, TestBadReader) {
    storage::disk_log_builder b;
    build_segments(
      b,
      /*num_segs=*/5,
      /*records_per_seg=*/10,
      /*start_offset=*/0,
      /*mark_compacted=*/false);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    auto& segs = disk_log.segments();

    // Build an offset map for our log.
    compaction_config cfg(
      model::offset{0},
      std::nullopt,
      ss::default_priority_class(),
      never_abort);
    simple_key_offset_map all_segs_map(50);
    auto map_start_offset = build_offset_map(
                              cfg,
                              segs,
                              disk_log.stm_manager(),
                              disk_log.resources(),
                              disk_log.get_probe(),
                              all_segs_map)
                              .get();
    ASSERT_EQ(map_start_offset(), 0);

    // Set up an appender and index writer.
    auto first_seg = segs[0];
    const auto tmpname = first_seg->reader().path().to_compaction_staging();
    auto appender = storage::internal::make_segment_appender(
                      tmpname,
                      segment_appender::write_behind_memory
                        / storage::internal::chunks().chunk_size(),
                      std::nullopt,
                      cfg.iopc,
                      disk_log.resources(),
                      cfg.sanitizer_config)
                      .get();
    const auto cmp_idx_tmpname = tmpname.to_compacted_index();
    auto compacted_idx_writer = make_file_backed_compacted_index(
      cmp_idx_tmpname,
      cfg.iopc,
      true,
      disk_log.resources(),
      cfg.sanitizer_config);
    auto close = ss::defer([&] {
        compacted_idx_writer.close().get();
        appender->close().get();
    });

    // Test that injecting a failure only throws an exception, i.e. no crashes!
    EXPECT_THROW(
      deduplicate_segment(
        cfg,
        all_segs_map,
        first_seg,
        *appender,
        compacted_idx_writer,
        disk_log.get_probe(),
        storage::internal::should_apply_delta_time_offset(b.feature_table()),
        b.feature_table(),
        /*inject_reader_failure=*/true)
        .get(),
      std::runtime_error);
}
