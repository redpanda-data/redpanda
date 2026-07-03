// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction/key_offset_map.h"
#include "config/configuration.h"
#include "model/batch_compression.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "storage/compaction_reducers.h"
#include "storage/disk_log_impl.h"
#include "storage/segment_deduplication_utils.h"
#include "storage/segment_utils.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/types.h"

#include <seastar/core/gate.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

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
            offset,
            records_per_seg,
            maybe_compress_batches::yes,
            model::record_batch_type::raft_data,
            append_config(),
            disk_log_builder::should_flush_after::yes,
            model::timestamp::min());
    }
    for (auto& seg : disk_log.segments()) {
        if (mark_compacted) {
            seg->index().maybe_set_self_compact_timestamp(
              model::timestamp::now());
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
            // Since we're "rolling" the segment manually here (releasing the
            // appender), increment closed/dirty bytes in the log.
            if (!seg->has_clean_compact_timestamp()) {
                b.add_dirty_segment_bytes(seg->file_size());
            }
            b.add_closed_segment_bytes(seg->file_size());
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
            compaction::compaction_config cfg(
              model::offset{end},
              model::offset{end},
              model::offset{end},
              std::nullopt,
              std::nullopt,
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
    compaction::compaction_config cfg(
      model::offset{30},
      model::offset{30},
      model::offset{30},
      std::nullopt,
      std::nullopt,
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
    compaction::compaction_config cfg(
      model::offset{30},
      model::offset{30},
      model::offset{30},
      std::nullopt,
      std::nullopt,
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
    compaction::compaction_config cfg(
      model::offset{30},
      model::offset{30},
      model::offset{30},
      std::nullopt,
      std::nullopt,
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
    compaction::compaction_config cfg(
      model::offset{30},
      model::offset{30},
      model::offset{30},
      std::nullopt,
      std::nullopt,
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
    compaction::compaction_config cfg(
      model::offset{30},
      model::offset{30},
      model::offset{30},
      1ms,
      std::nullopt,
      never_abort);
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
    compaction::compaction_config cfg(
      model::offset{30},
      model::offset{30},
      model::offset{30},
      std::nullopt,
      std::nullopt,
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

    compaction::compaction_config cfg(
      model::offset{30},
      model::offset{30},
      model::offset{30},
      1ms,
      std::nullopt,
      never_abort);
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
    truncate_prefix_config trunc_cfg(model::offset{10});
    disk_log.truncate_prefix(trunc_cfg).get();

    compaction::compaction_config cfg(
      model::offset{30},
      model::offset{30},
      model::offset{30},
      1ms,
      std::nullopt,
      never_abort);
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
    compaction::compaction_config cfg(
      model::offset{30},
      model::offset{30},
      model::offset{30},
      std::nullopt,
      std::nullopt,
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
          disk_log.stm_hookset(),
          cfg,
          pb,
          disk_log.readers(),
          disk_log.resources(),
          feature_table)
          .get();
    }

    // Now configure a map to index some segments.
    compaction::simple_key_offset_map partial_map(15);
    auto partial_o = build_offset_map(
                       cfg,
                       segs,
                       disk_log.stm_hookset(),
                       disk_log.resources(),
                       disk_log.get_probe(),
                       partial_map,
                       b.feature_table())
                       .get();
    ASSERT_GT(partial_o(), 0);

    // Now make it large enough to index all segments.
    compaction::simple_key_offset_map all_segs_map(100);
    auto all_segs_o = build_offset_map(
                        cfg,
                        segs,
                        disk_log.stm_hookset(),
                        disk_log.resources(),
                        disk_log.get_probe(),
                        all_segs_map,
                        b.feature_table())
                        .get();
    ASSERT_EQ(all_segs_o(), 0);
}

TEST(BuildOffsetMap, TestBuildMapWithMissingCompactedIndex) {
    storage::disk_log_builder b;
    build_segments(b, 3);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    auto& segs = disk_log.segments();
    compaction::compaction_config cfg(
      model::offset{30},
      model::offset{30},
      model::offset{30},
      std::nullopt,
      std::nullopt,
      never_abort);
    for (const auto& s : segs) {
        auto idx_path = s->path().to_compacted_index();
        ASSERT_FALSE(ss::file_exists(idx_path.string()).get());
    }

    // Proceed to window compaction without building any compacted indexes.
    // When building the map, we should attempt to rebuild the index if it
    // doesn't exist.
    compaction::simple_key_offset_map missing_index_map(100);
    auto o = build_offset_map(
               cfg,
               segs,
               disk_log.stm_hookset(),
               disk_log.resources(),
               disk_log.get_probe(),
               missing_index_map,
               b.feature_table())
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
    compaction::compaction_config cfg(
      model::offset{0},
      model::offset{0},
      model::offset{0},
      std::nullopt,
      std::nullopt,
      never_abort);
    compaction::simple_key_offset_map all_segs_map(50);
    auto map_start_offset = build_offset_map(
                              cfg,
                              segs,
                              disk_log.stm_hookset(),
                              disk_log.resources(),
                              disk_log.get_probe(),
                              all_segs_map,
                              b.feature_table())
                              .get();
    ASSERT_EQ(map_start_offset(), 0);

    // Set up an appender and index writer.
    auto first_seg = segs[0];
    const auto tmpname = first_seg->reader().path().to_compaction_staging();
    auto appender = storage::internal::make_segment_appender(
                      tmpname,
                      std::nullopt,
                      disk_log.resources(),
                      cfg.sanitizer_config,
                      nullptr)
                      .get();
    const auto cmp_idx_tmpname = tmpname.to_compacted_index();
    auto compacted_idx_writer = make_file_backed_compacted_index(
      cmp_idx_tmpname, true, disk_log.resources(), cfg.sanitizer_config);
    auto close = ss::defer([&] {
        compacted_idx_writer->close().get();
        appender->close().get();
    });

    // Test that injecting a failure only throws an exception, i.e. no crashes!
    EXPECT_THROW(
      deduplicate_segment(
        cfg,
        all_segs_map,
        first_seg,
        *appender,
        *compacted_idx_writer,
        disk_log.stm_hookset(),
        disk_log.get_probe(),
        storage::internal::should_apply_delta_time_offset(b.feature_table()),
        b.feature_table(),
        /*inject_reader_failure=*/true)
        .get(),
      std::runtime_error);
}

TEST(DeduplicateSegmentsTest, SegmentNeedsRewriteNoCompactedIndex) {
    storage::disk_log_builder b;
    build_segments(
      b,
      /*num_segs=*/1,
      /*records_per_seg=*/10,
      /*start_offset=*/0,
      /*mark_compacted=*/false);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();

    auto& segs = disk_log.segments();
    auto& seg = segs[0];
    compaction::compaction_config cfg(
      model::offset{0},
      model::offset{0},
      model::offset{0},
      std::nullopt,
      std::nullopt,
      never_abort);
    compaction::simple_key_offset_map map(50);

    // When the .compaction_index file doesn't exist, the segment should assume
    // it needs rewriting.
    ASSERT_FALSE(
      ss::file_exists(seg->path().to_compacted_index().string()).get());

    bool needs_rewrite
      = segment_needs_rewrite_with_offset_map(cfg, seg, map).get();

    // Make sure we didn't create a new .compaction_index file by just
    // attempting to read it.
    ASSERT_FALSE(
      ss::file_exists(seg->path().to_compacted_index().string()).get());

    ASSERT_EQ(needs_rewrite, true);
}

TEST(CopyReducerCompressionReuseTest, ReusesCompressedPayloadWhenUnchanged) {
    storage::disk_log_builder b;
    build_segments(
      b,
      /*num_segs=*/2,
      /*records_per_seg=*/1,
      /*start_offset=*/0,
      /*mark_compacted=*/false);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    auto& segs = disk_log.segments();

    compaction::compaction_config cfg(
      model::offset{0},
      model::offset{0},
      model::offset{0},
      std::nullopt,
      std::nullopt,
      never_abort);
    const auto apply_offset = storage::internal::should_apply_delta_time_offset(
      b.feature_table());

    // Runs the reducer over `batch` into a fresh appender backed by `seg`'s
    // staging path, and returns the resulting reducer stats.
    auto run_one =
      [&](
        const ss::lw_shared_ptr<storage::segment>& seg,
        model::record_batch batch,
        storage::internal::copy_data_segment_reducer::filter_t keep_fn) {
          const auto tmpname = seg->reader().path().to_compaction_staging();
          auto appender = storage::internal::make_segment_appender(
                            tmpname,
                            std::nullopt,
                            disk_log.resources(),
                            cfg.sanitizer_config,
                            nullptr)
                            .get();
          auto close = ss::defer([&] { appender->close().get(); });
          const auto seg_last_offset = batch.last_offset();
          const auto base_offset = batch.base_offset();
          storage::internal::copy_data_segment_reducer reducer(
            disk_log.config().ntp(),
            std::move(keep_fn),
            appender.get(),
            /*internal_topic=*/false,
            apply_offset,
            base_offset,
            seg_last_offset,
            /*compaction_placeholder_enabled=*/false,
            /*unset_transaction_bit_enabled=*/false,
            disk_log.stm_hookset(),
            /*cidx=*/nullptr,
            /*inject_failure=*/false,
            &never_abort);
          reducer(std::move(batch)).get();
          return reducer.end_of_stream().reducer_stats;
      };

    auto keep_all =
      [](const model::record_batch&, const model::record_key_metadata&, bool) {
          return ss::make_ready_future<bool>(true);
      };

    auto make_batch = [](bool compress) {
        auto batch = model::test::make_random_batch(
          model::test::record_batch_spec{
            .offset = model::offset{0},
            .allow_compression = false,
            .count = 4});
        if (compress) {
            batch = model::compress_batch_sync(
              model::compression::zstd, std::move(batch));
        }
        return batch;
    };

    // A compressed batch with nothing removed is reused, not re-compressed.
    {
        auto batch = make_batch(/*compress=*/true);
        ASSERT_TRUE(batch.compressed());
        auto stats = run_one(segs[0], std::move(batch), keep_all);
        EXPECT_EQ(stats.records_discarded, 0);
        EXPECT_EQ(stats.compressed_batches_reused, 1);
    }

    // When a record is removed, the batch is rebuilt and re-compressed rather
    // than reused.
    {
        auto batch = make_batch(/*compress=*/true);
        ASSERT_TRUE(batch.compressed());
        bool drop = true;
        auto drop_first = [&](
                            const model::record_batch&,
                            const model::record_key_metadata&,
                            bool) mutable {
            // Keep every record except the first: the predicate returns
            // whether to keep, so negate the one-shot drop flag.
            return ss::make_ready_future<bool>(!std::exchange(drop, false));
        };
        auto stats = run_one(segs[1], std::move(batch), std::move(drop_first));
        EXPECT_EQ(stats.records_discarded, 1);
        EXPECT_EQ(stats.compressed_batches_reused, 0);
    }
}

namespace {

// Transactional stm fake whose aborted_tx_ranges() parks until the test
// unblocks it, letting the test stop the stm_hookset at a precise point
// during compaction index rebuild.
class blocking_tx_stm final : public storage::snapshotable_stm {
public:
    storage::stm_type type() override {
        return storage::stm_type::user_topic_transactional;
    }
    ss::future<> ensure_local_snapshot_exists(model::offset) override {
        return ss::now();
    }
    void write_local_snapshot_in_background() override {}
    model::offset max_removable_local_log_offset() override {
        return model::offset::max();
    }
    std::optional<kafka::offset> lowest_pinned_data_offset() const override {
        return std::nullopt;
    }
    model::offset last_locally_snapshotted_offset() const override {
        return model::offset{};
    }
    model::offset last_applied() const override { return model::offset{}; }
    const ss::sstring& name() override { return _name; }
    ss::future<chunked_vector<model::tx_range>>
    aborted_tx_ranges(model::offset, model::offset) override {
        entered = true;
        co_await _unblock.get_future();
        co_return chunked_vector<model::tx_range>{};
    }

    bool entered{false};
    ss::promise<> _unblock;

private:
    ss::sstring _name{"blocking_tx_stm"};
};

} // anonymous namespace

// Stopping the stm_hookset after aborted tx ranges are fetched but before the
// compaction index is built makes stm_hookset::transactional_stm_type() throw
// synchronously. This used to destroy the freshly-created segment reader
// while it still held a live reader, tripping the assert in ~log_reader
// (e.g. partition shutdown racing compaction during a partition move).
TEST(RebuildCompactionIndexTest, TestHooksetStoppedDuringRebuild) {
    storage::disk_log_builder b;
    build_segments(b, 1);
    auto cleanup = ss::defer([&] { b.stop().get(); });
    auto& disk_log = b.get_disk_log_impl();
    auto seg = disk_log.segments().front();

    compaction::compaction_config cfg(
      model::offset{30},
      model::offset{30},
      model::offset{30},
      std::nullopt,
      std::nullopt,
      never_abort);
    probe pb;

    auto stm = ss::make_shared<blocking_tx_stm>();
    auto hookset = ss::make_lw_shared<storage::stm_hookset>();
    hookset->add_stm(stm);
    hookset->start();

    auto fut = storage::internal::rebuild_compaction_index(
      seg, hookset, cfg, pb, disk_log.resources(), false);
    while (!stm->entered) {
        ss::yield().get();
    }
    ASSERT_FALSE(fut.available());
    hookset->stop();
    stm->_unblock.set_value();
    ASSERT_THROW(fut.get(), ss::gate_closed_exception);
}
