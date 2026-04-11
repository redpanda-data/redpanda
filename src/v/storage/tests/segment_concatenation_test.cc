// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "storage/disk_log_impl.h"
#include "storage/parser.h"
#include "storage/scoped_file_tracker.h"
#include "storage/segment.h"
#include "storage/segment_deduplication_utils.h"
#include "storage/segment_reader.h"
#include "storage/segment_utils.h"
#include "storage/tests/disk_log_builder_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/types.h"
#include "test_utils/test.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <numeric>
#include <stdexcept>

using namespace std::literals::chrono_literals;

namespace {
ss::abort_source never_abort;
ss::logger test_log("segment_concatenation_test_log");

struct read_segments_result {
    iobuf buf;
    size_t len;
    auto operator==(const read_segments_result& o) const {
        return std::tie(buf, len) == std::tie(o.buf, o.len);
    }
};

struct segment_attrs_result {
    model::offset base_offset;
    model::offset dirty_offset;
    model::offset committed_offset;
    model::offset stable_offset;

    size_t num_records{0};
    size_t size_bytes{0};

    auto operator==(const segment_attrs_result& o) const {
        return std::tie(
                 base_offset,
                 dirty_offset,
                 committed_offset,
                 stable_offset,
                 num_records,
                 size_bytes)
               == std::tie(
                 o.base_offset,
                 o.dirty_offset,
                 o.committed_offset,
                 o.stable_offset,
                 o.num_records,
                 o.size_bytes);
    }
};

struct segment_comparision_fields {
    segment_attrs_result attrs_result;
    read_segments_result read_result;

    auto operator==(const segment_comparision_fields& o) const {
        return std::tie(attrs_result, read_result)
               == std::tie(o.attrs_result, o.read_result);
    }
};

segment_comparision_fields concat_read_segments(
  std::vector<ss::lw_shared_ptr<storage::segment>> segments) {
    size_t start_pos = 0;
    size_t end_pos = segments.back()->file_size();
    storage::concat_segment_reader_view cv{segments, start_pos, end_pos};
    iobuf buf;
    segment_comparision_fields res;
    size_t num_records = 0;
    size_t size_bytes = 0;
    auto transform_stream_result
      = storage::transform_stream(
          cv.take_stream(),
          make_iobuf_ref_output_stream(buf),
          [&num_records, &size_bytes](model::record_batch_header h) {
              num_records += h.record_count;
              size_bytes += h.record_count;
              return storage::batch_consumer::consume_result::accept_batch;
          })
          .get();

    res.attrs_result = segment_attrs_result{
      .base_offset = segments.front()->offsets().get_base_offset(),
      .dirty_offset = segments.back()->offsets().get_dirty_offset(),
      .committed_offset = segments.back()->offsets().get_committed_offset(),
      .stable_offset = segments.back()->offsets().get_stable_offset(),
      .num_records = num_records,
      .size_bytes = size_bytes};
    EXPECT_TRUE(transform_stream_result.has_value());
    res.read_result = read_segments_result{
      .buf = std::move(buf), .len = transform_stream_result.value()};
    return res;
}

void concatenate_segments_from_log(
  storage::disk_log_impl& log,
  int expected_segments_to_compact,
  ss::sharded<features::feature_table>& feature_table,
  bool maybe_compress) {
    compaction::compaction_config cfg(
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      never_abort);

    const auto closed_segment_filter = [](const auto& s) -> bool {
        return !s->has_appender();
    };

    auto log_segments = log.segments()
                        | std::views::filter(closed_segment_filter);

    std::vector<ss::lw_shared_ptr<storage::segment>> segments(
      log_segments.begin(), log_segments.end());
    EXPECT_EQ(segments.size(), expected_segments_to_compact);

    // Self compaction shouldn't affect the number of records in the segment
    // Compression may have an effect on read buffer/len though.
    for (auto& seg : segments) {
        auto read_result_before = concat_read_segments({seg});
        log.segment_self_compact(cfg, seg).get();
        auto read_result_after = concat_read_segments({seg});
        if (maybe_compress) {
            EXPECT_EQ(
              read_result_before.attrs_result, read_result_after.attrs_result);
        } else {
            EXPECT_EQ(read_result_before, read_result_after);
        }
    }

    auto expected_target_file_size = std::accumulate(
      segments.begin(),
      segments.end(),
      size_t{0},
      [](size_t sum, const auto& seg) { return sum + seg->file_size(); });

    auto segments_read_result = concat_read_segments(segments);

    auto fake_segment_rewrite_lock = ssx::mutex{"fake_segment_rewrite_lock"};
    auto holder = fake_segment_rewrite_lock.get_units().get();

    // the segment which will be expanded to replace
    auto target = segments.front();

    chunked_vector<ss::lw_shared_ptr<storage::segment>> chunked_segments(
      segments.begin(), segments.end());
    [[maybe_unused]] auto ret
      = storage::internal::concatenate_and_rebuild_target_segment(
          target,
          chunked_segments,
          log.stm_hookset(),
          cfg,
          log.get_probe(),
          log.readers(),
          log.resources(),
          feature_table,
          fake_segment_rewrite_lock)
          .get();

    EXPECT_EQ(target->file_size(), expected_target_file_size);

    auto target_read_result = concat_read_segments({target});
    EXPECT_EQ(segments_read_result, target_read_result);
}

} // anonymous namespace

// Params:
// size_t number of segments to concatenate
// bool maybe compress batches or not
class MakeConcatenatedSegmentFixture
  : public testing::Test
  , public testing::WithParamInterface<std::tuple<size_t, bool>> {};

TEST_P(MakeConcatenatedSegmentFixture, ConcatenateSegments) {
    using namespace storage;
    disk_log_builder b;
    auto cleanup = ss::defer([&] { b.stop().get(); });
    b | start();
    auto& disk_log = b.get_disk_log_impl();

    auto [num_segments, maybe_compress] = GetParam();
    auto records_per_seg = random_generators::get_int(50, 200);
    model::timestamp base_ts = model::timestamp::min();
    for (size_t i = 0; i < num_segments; ++i) {
        auto offset = i * records_per_seg;
        b
          | add_random_batch(
            offset,
            records_per_seg,
            maybe_compress_batches(maybe_compress),
            model::record_batch_type::raft_data,
            append_config(),
            disk_log_builder::should_flush_after::yes,
            base_ts);
        disk_log.force_roll().get();
    }

    concatenate_segments_from_log(
      disk_log, num_segments, b.feature_table(), maybe_compress);
}

INSTANTIATE_TEST_SUITE_P(
  MakeConcatenatedSegmentTest,
  MakeConcatenatedSegmentFixture,
  testing::Combine(
    testing::Values(1, 2, 3, 4, 5, 10, 20, 100), testing::Bool()));
