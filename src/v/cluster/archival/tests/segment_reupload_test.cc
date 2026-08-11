/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/types.h"
#include "cluster/archival/adjacent_segment_merger.h"
#include "cluster/archival/archival_policy.h"
#include "cluster/archival/segment_reupload.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/offset_interval.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "storage/log_manager.h"
#include "storage/record_batch_utils.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/types.h"
#include "test_utils/archival.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/loop.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <variant>

using namespace archival;

namespace {
static const cloud_storage::remote_path_provider
  path_provider(std::nullopt, std::nullopt);

bool has_failure(const segment_collector_stream_result& res) {
    return std::holds_alternative<candidate_creation_error>(res);
}

segment_collector_stream& value(segment_collector_stream_result& res) {
    vassert(
      std::holds_alternative<segment_collector_stream>(res), "Expected value");
    return std::get<segment_collector_stream>(res);
}
} // anonymous namespace

inline ss::logger test_log("test");

static constexpr std::string_view manifest = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 1,
    "last_offset": 39,
    "segments": {
        "10-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 1024,
            "base_offset": 10,
            "committed_offset": 19
        },
        "20-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 2048,
            "base_offset": 20,
            "committed_offset": 29,
            "max_timestamp": 1234567890
        },
        "30-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 30,
            "committed_offset": 39,
            "max_timestamp": 1234567890
        }
    }
})json";

static constexpr std::string_view manifest_with_gaps = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 1,
    "last_offset": 59,
    "segments": {
        "10-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 1024,
            "base_offset": 10,
            "committed_offset": 19
        },
        "30-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 2048,
            "base_offset": 30,
            "committed_offset": 39,
            "max_timestamp": 1234567890
        },
        "50-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 50,
            "committed_offset": 59,
            "max_timestamp": 1234567890
        }
    }
})json";

static constexpr std::string_view test_manifest = R"json({
  "version": 2,
  "namespace": "test-ns",
  "topic": "test-topic",
  "partition": 1,
  "revision": 21,
  "last_offset": 211,
  "segments": {
    "0-1-v1.log": {
      "base_offset": 0,
      "committed_offset": 1,
      "is_compacted": false,
      "size_bytes": 200,
      "archiver_term": 2,
      "delta_offset": 0,
      "base_timestamp": 1686389191244,
      "max_timestamp": 1686389191244,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 1,
      "delta_offset_end": 0
    },
    "2-2-v1.log": {
      "base_offset": 2,
      "committed_offset": 103,
      "is_compacted": false,
      "size_bytes": 98014783,
      "archiver_term": 2,
      "delta_offset": 0,
      "base_timestamp": 1686389202577,
      "max_timestamp": 1686389230060,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 2,
      "delta_offset_end": 0
    },
    "104-2-v1.log": {
      "base_offset": 104,
      "committed_offset": 113,
      "is_compacted": false,
      "size_bytes": 10001460,
      "archiver_term": 2,
      "delta_offset": 0,
      "base_timestamp": 1686389230182,
      "max_timestamp": 1686389233222,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 2,
      "delta_offset_end": 0
    },
    "113-2-v1.log": {
      "base_offset": 113,
      "committed_offset": 115,
      "is_compacted": false,
      "size_bytes": 10001460,
      "archiver_term": 2,
      "delta_offset": 0,
      "base_timestamp": 1686389230182,
      "max_timestamp": 1686389233222,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 2,
      "delta_offset_end": 0
    },
    "116-2-v1.log": {
      "base_offset": 116,
      "committed_offset": 211,
      "is_compacted": false,
      "size_bytes": 10001460,
      "archiver_term": 2,
      "delta_offset": 0,
      "base_timestamp": 1686389230182,
      "max_timestamp": 1686389233222,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 2,
      "delta_offset_end": 0
    }
  }
})json";

static constexpr size_t max_upload_size{4096_KiB};
static constexpr ss::lowres_clock::duration segment_lock_timeout{60s};

TEST(SegmentReuploadUnit, test_segment_collection) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // Local disk log starts before manifest and ends after manifest. First
    // three segments are compacted.
    populate_log(
      b,
      {.segment_starts = {5, 22, 35, 50},
       .compacted_segment_indices = {0, 1, 2},
       .last_segment_num_records = 10});

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{4},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();

    // The three compacted segments are collected, with the begin and end
    // markers set to align with manifest segment.
    ASSERT_TRUE(collector.should_replace_manifest_segment());
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});
    ASSERT_EQ(collector.end_inclusive(), model::offset{39});
    ASSERT_EQ(3, collector.segments().size());
}

TEST(SegmentReuploadUnit, test_make_upload_candidate_stream) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("candidate_stream_test");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    populate_log(
      b,
      {.segment_starts = {10, 20, 30, 40},
       .compacted_segment_indices = {0, 1, 2},
       .last_segment_num_records = 10});

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{4},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();
    ASSERT_TRUE(collector.should_replace_manifest_segment());
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});
    ASSERT_EQ(collector.end_inclusive(), model::offset{39});
    ASSERT_EQ(3, collector.segments().size());

    auto result
      = collector.make_upload_candidate_stream(segment_lock_timeout).get();
    ASSERT_TRUE(!has_failure(result));
    ASSERT_TRUE(!std::holds_alternative<skip_offset_range>(result));

    auto cstream = std::move(value(result));
    ASSERT_GE(cstream.size, size_t{1});

    // Read from candidate stream
    iobuf candidate_data;
    {
        auto is = cstream.create_input_stream();
        while (!is.eof()) {
            auto buf = is.read().get();
            if (buf.empty()) {
                break;
            }
            candidate_data.append(std::move(buf));
        }
        is.close().get();
    }
    ASSERT_EQ(candidate_data.size_bytes(), cstream.size);
}

TEST(SegmentReuploadUnit, test_make_upload_candidate_skip_offsets) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("candidate_stream_test");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    populate_log(
      b,
      {.segment_starts = {15, 25, 35},
       .compacted_segment_indices = {0, 1, 2},
       .last_segment_num_records = 10});

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{4},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();
    auto result
      = collector.make_upload_candidate_stream(segment_lock_timeout).get();
    ASSERT_TRUE(!has_failure(result));
    ASSERT_TRUE(std::holds_alternative<skip_offset_range>(result));
    ASSERT_EQ(
      std::get<skip_offset_range>(result).end_offset, model::offset{39});
}

TEST(SegmentReuploadUnit, test_start_ahead_of_manifest) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    {
        // start ahead of manifest end, no collection happens.
        archival::segment_collector collector{
          segment_collector_mode::compacted_reupload,
          model::offset{400},
          m,
          b.get_disk_log_impl(),
          max_upload_size};

        collector.collect_segments();

        ASSERT_EQ(false, collector.should_replace_manifest_segment());
        auto segments = collector.segments();
        ASSERT_TRUE(segments.empty());
    }

    {
        // start at manifest end. the collector will advance it first to prevent
        // overlap. no collection happens.
        archival::segment_collector collector{
          segment_collector_mode::compacted_reupload,
          model::offset{39},
          m,
          b.get_disk_log_impl(),
          max_upload_size};

        collector.collect_segments();

        ASSERT_EQ(false, collector.should_replace_manifest_segment());
        auto segments = collector.segments();
        ASSERT_TRUE(segments.empty());
    }
}

TEST(SegmentReuploadUnit, test_empty_manifest) {
    cloud_storage::partition_manifest m;

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{2},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();

    ASSERT_EQ(false, collector.should_replace_manifest_segment());
    ASSERT_TRUE(collector.segments().empty());
}

TEST(
  SegmentReuploadUnit, test_short_compacted_segment_inside_manifest_segment) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // segment [12-14] lies inside manifest segment [10-19]. start offset 1 is
    // adjusted to start of the local log 12. Since this offset is in the middle
    // of a manifest segment, advance it again to the beginning of the next
    // manifest segment: 20. There's no local segment containing that offset, so
    // no segments are collected.
    populate_log(
      b,
      {.segment_starts = {12},
       .compacted_segment_indices = {0},
       .last_segment_num_records = 2});

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{1},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();

    ASSERT_EQ(false, collector.should_replace_manifest_segment());
    ASSERT_EQ(collector.segments().size(), 0);
}

TEST(
  SegmentReuploadUnit, test_compacted_segment_aligned_with_manifest_segment) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    populate_log(
      b,
      {.segment_starts = {10, 20, 45, 55},
       .compacted_segment_indices = {0},
       .last_segment_num_records = 10});

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{1},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();

    ASSERT_TRUE(collector.should_replace_manifest_segment());
    auto segments = collector.segments();
    ASSERT_EQ(1, segments.size());

    const auto& seg = segments.front();
    ASSERT_EQ(seg->offsets().get_base_offset(), model::offset{10});
    ASSERT_EQ(seg->offsets().get_committed_offset(), model::offset{19});
}

TEST(
  SegmentReuploadUnit,
  test_short_compacted_segment_aligned_with_manifest_segment) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // compacted segment start aligned with manifest segment start, but segment
    // is too short.
    populate_log(
      b,
      {.segment_starts = {10},
       .compacted_segment_indices = {0},
       .last_segment_num_records = 5});

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();

    ASSERT_EQ(false, collector.should_replace_manifest_segment());
    auto segments = collector.segments();
    ASSERT_EQ(1, segments.size());

    const auto& seg = segments.front();
    ASSERT_EQ(seg->offsets().get_base_offset(), model::offset{10});
    ASSERT_EQ(seg->offsets().get_committed_offset(), model::offset{14});
}

TEST(
  SegmentReuploadUnit,
  test_many_compacted_segments_make_up_to_manifest_segment) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // The compacted segments are small, but combine to cover one
    // manifest segment.
    populate_log(
      b,
      {.segment_starts = {10, 12, 14, 16, 18},
       .compacted_segment_indices = {0, 1, 2, 3, 4},
       .last_segment_num_records = 3});

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();

    ASSERT_TRUE(collector.should_replace_manifest_segment());
    auto segments = collector.segments();
    ASSERT_EQ(5, segments.size());
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});
    ASSERT_EQ(collector.end_inclusive(), model::offset{19});
}

TEST(SegmentReuploadUnit, test_compacted_segment_larger_than_manifest_segment) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // Compacted segment larger than manifest segment, extending out from both
    // begin and end.
    populate_log(
      b,
      {.segment_starts = {8},
       .compacted_segment_indices = {0},
       .last_segment_num_records = 20});

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{2},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();

    ASSERT_TRUE(collector.should_replace_manifest_segment());
    auto segments = collector.segments();

    ASSERT_EQ(1, segments.size());

    // Begin and end markers are aligned to manifest segment.
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});
    ASSERT_EQ(collector.end_inclusive(), model::offset{19});
}

TEST(SegmentReuploadUnit, test_collect_capped_by_size) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // Normally the greedy collector would pick up all four compacted segments,
    // but because we restrict size, it will only pick the first three segments.
    populate_log(
      b,
      {.segment_starts = {5, 15, 25, 35, 50, 60},
       .compacted_segment_indices = {0, 1, 2, 3},
       .last_segment_num_records = 20});

    size_t max_size = b.get_segment(0).file_size()
                      + b.get_segment(1).file_size()
                      + b.get_segment(2).file_size();
    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      max_size};

    collector.collect_segments();

    ASSERT_TRUE(collector.should_replace_manifest_segment());
    auto segments = collector.segments();

    ASSERT_EQ(3, segments.size());

    // Begin marker starts on first manifest segment boundary.
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});

    // End marker ends on second manifest segment boundary.
    ASSERT_EQ(collector.end_inclusive(), model::offset{29});

    size_t collected_size = std::transform_reduce(
      segments.begin(), segments.end(), 0, std::plus<>{}, [](const auto& seg) {
          return seg->size_bytes();
      });
    ASSERT_LE(collected_size, max_size);
}

TEST(SegmentReuploadUnit, test_no_compacted_segments) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    populate_log(
      b,
      {.segment_starts = {5, 15, 25, 35, 50, 60},
       .compacted_segment_indices = {},
       .last_segment_num_records = 20});

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{5},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();

    ASSERT_EQ(false, collector.should_replace_manifest_segment());
    ASSERT_TRUE(collector.segments().empty());
}

TEST(SegmentReuploadUnit, test_segment_name_adjustment) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    populate_log(
      b,
      {.segment_starts = {8},
       .compacted_segment_indices = {0},
       .last_segment_num_records = 20});

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{8},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();
    auto name = collector.adjust_segment_name();
    ASSERT_EQ(name, cloud_storage::segment_name{"10-0-v1.log"});
}

TEST(SegmentReuploadUnit, test_segment_name_no_adjustment) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    populate_log(
      b,
      {.segment_starts = {10},
       .compacted_segment_indices = {0},
       .last_segment_num_records = 20});

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{8},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();

    auto name = collector.adjust_segment_name();
    ASSERT_EQ(name, cloud_storage::segment_name{"10-0-v1.log"});
}

TEST(SegmentReuploadUnit, test_collected_segments_completely_cover_gap) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json,
       make_manifest_stream(manifest_with_gaps))
      .get();

    using namespace storage;

    {
        temporary_dir tmp_dir("concat_segment_read");
        auto data_path = tmp_dir.get_path();

        auto b = make_log_builder(data_path.string());

        b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
        auto defer = ss::defer([&b] { b.stop().get(); });

        // The manifest has gap from 20-29. It will be replaced by re-uploaded
        // data. The re-upload will end at the gap boundary due to adjustment of
        // end offset.
        populate_log(
          b,
          {.segment_starts = {5, 15, 25, 35, 50, 60},
           .compacted_segment_indices = {0, 1, 2, 3},
           .last_segment_num_records = 20});

        size_t max_size = b.get_segment(0).file_size()
                          + b.get_segment(1).file_size()
                          + b.get_segment(2).file_size();
        archival::segment_collector collector{
          segment_collector_mode::compacted_reupload,
          model::offset{0},
          m,
          b.get_disk_log_impl(),
          max_size};

        collector.collect_segments();

        ASSERT_TRUE(collector.should_replace_manifest_segment());
        auto segments = collector.segments();

        ASSERT_EQ(3, segments.size());

        // Collection start aligned to manifest start at 10
        ASSERT_EQ(collector.begin_inclusive(), model::offset{10});

        // End marker adjusted to the end of the gap.
        ASSERT_EQ(collector.end_inclusive(), model::offset{29});

        size_t collected_size = std::transform_reduce(
          segments.begin(),
          segments.end(),
          0,
          std::plus<>{},
          [](const auto& seg) { return seg->size_bytes(); });
        ASSERT_LE(collected_size, max_size);
    }

    {
        temporary_dir tmp_dir("concat_segment_read");
        auto data_path = tmp_dir.get_path();

        auto b = make_log_builder(data_path.string());

        b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
        auto defer = ss::defer([&b] { b.stop().get(); });

        // Re-uploaded segments completely cover gap.
        populate_log(
          b,
          {.segment_starts = {5, 15, 25, 40, 50, 60},
           .compacted_segment_indices = {0, 1, 2, 3},
           .last_segment_num_records = 20});

        size_t max_size = b.get_segment(0).file_size()
                          + b.get_segment(1).file_size()
                          + b.get_segment(2).file_size();
        archival::segment_collector collector{
          segment_collector_mode::compacted_reupload,
          model::offset{0},
          m,
          b.get_disk_log_impl(),
          max_size};

        collector.collect_segments();

        ASSERT_TRUE(collector.should_replace_manifest_segment());
        auto segments = collector.segments();

        ASSERT_EQ(3, segments.size());

        // Collection start aligned to manifest start at 10
        ASSERT_EQ(collector.begin_inclusive(), model::offset{10});

        // End marker adjusted to the end of the gap.
        ASSERT_EQ(collector.end_inclusive(), model::offset{39});

        size_t collected_size = std::transform_reduce(
          segments.begin(),
          segments.end(),
          0,
          std::plus<>{},
          [](const auto& seg) { return seg->size_bytes(); });
        ASSERT_LE(collected_size, max_size);
    }
}

TEST(SegmentReuploadUnit, test_compacted_segment_after_manifest_start) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    using namespace storage;

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // manifest start: 10, compacted segment start: 15, search start: 0
    // begin offset will be realigned to end of segment 10-19 to avoid overlap.
    populate_log(
      b,
      {.segment_starts = {15, 45, 50},
       .compacted_segment_indices = {0},
       .last_segment_num_records = 20});

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector.collect_segments();
    ASSERT_TRUE(collector.should_replace_manifest_segment());
    ASSERT_EQ(1, collector.segments().size());
    ASSERT_EQ(collector.begin_inclusive(), model::offset{20});
    ASSERT_EQ(collector.end_inclusive(), model::offset{39});
}

TEST(SegmentReuploadUnit, test_upload_candidate_generation) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // For this test we need batches with single records, so that the seek
    // inside the segments aligns with manifest, because seek adjusts offsets to
    // batch boundaries.
    auto spec = log_spec{
      .segment_starts = {5, 15, 25, 35, 50, 60},
      .compacted_segment_indices = {0, 1, 2, 3},
      .last_segment_num_records = 20};

    auto first = spec.segment_starts.begin();
    auto second = std::next(first);
    for (; second != spec.segment_starts.end(); ++first, ++second) {
        b | storage::add_segment(*first);
        for (auto curr_offset = *first; curr_offset < *second; ++curr_offset) {
            b | storage::add_random_batch(curr_offset, 1);
        }
    }

    b | storage::add_segment(*first)
      | storage::add_random_batch(*first, spec.last_segment_num_records);

    for (auto i : spec.compacted_segment_indices) {
        b.get_segment(i).index().maybe_set_self_compact_timestamp(
          model::timestamp::now());
        b.get_segment(i).mark_as_finished_windowed_compaction();
    }

    size_t max_size = b.get_segment(0).size_bytes()
                      + b.get_segment(1).size_bytes()
                      + b.get_segment(2).size_bytes();
    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{5},
      m,
      b.get_disk_log_impl(),
      max_size};

    collector.collect_segments();
    ASSERT_TRUE(collector.should_replace_manifest_segment());

    auto upload_with_locks = require_upload_candidate(
      collector.make_upload_candidate(segment_lock_timeout).get());

    auto upload_candidate = upload_with_locks.candidate;
    ASSERT_TRUE(!upload_candidate.sources.empty());
    ASSERT_EQ(upload_candidate.starting_offset, model::offset{10});
    ASSERT_EQ(upload_candidate.final_offset, model::offset{29});

    // Start with all the segments collected
    auto expected_content_length = collector.collected_size();
    // Deduct the starting shift
    expected_content_length -= upload_candidate.file_offset;
    // Deduct the entire last segment
    expected_content_length -= upload_candidate.sources.back()->size_bytes();
    // Add back the portion of the last segment we included
    expected_content_length += upload_candidate.final_file_offset;

    ASSERT_EQ(expected_content_length, upload_candidate.content_length);

    ASSERT_EQ(upload_with_locks.read_locks.size(), 3);
}

TEST(SegmentReuploadUnit, test_upload_aligned_to_non_existent_offset) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    auto o = std::make_unique<ntp_config::default_overrides>();
    o->cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    b
      | start(
        ntp_config{{"test_ns", "test_tpc", 0}, {data_path}, std::move(o)});
    auto defer = ss::defer([&b] { b.stop().get(); });

    auto spec = log_spec{
      .segment_starts = {5, 15, 25, 35, 50, 60},
      .compacted_segment_indices = {0, 1, 2, 3},
      .last_segment_num_records = 20};

    auto first = spec.segment_starts.begin();
    auto second = std::next(first);
    // Segment boundaries: [5, 14][15, 24][25, 34][35, 50]...
    for (; second != spec.segment_starts.end(); ++first, ++second) {
        b | storage::add_segment(*first);
        for (auto curr_offset = *first; curr_offset < *second; ++curr_offset) {
            b.add_random_batch(
               model::test::record_batch_spec{
                 .offset = model::offset{curr_offset},
                 .count = 1,
                 .max_key_cardinality = 1,
               })
              .get();
        }
        auto seg = b.get_log_segments().back();
        seg->release_appender(&b.get_disk_log_impl().readers()).get();
        b.add_closed_segment_bytes(seg->file_size());
        if (!seg->has_clean_compact_timestamp()) {
            b.add_dirty_segment_bytes(seg->file_size());
        }
    }

    b | storage::add_segment(*first)
      | storage::add_random_batch(*first, spec.last_segment_num_records);

    // Compaction will rewrite each segment, and merge the first few.
    b.gc(model::timestamp::max(), std::nullopt).get();

    size_t max_size = b.get_segment(0).size_bytes()
                      + b.get_segment(1).size_bytes();
    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{5},
      m,
      b.get_disk_log_impl(),
      max_size};

    collector.collect_segments();
    ASSERT_TRUE(collector.should_replace_manifest_segment());

    auto upload_with_locks = require_upload_candidate(
      collector.make_upload_candidate(segment_lock_timeout).get());

    // The upload candidate should align with the manifest's segment
    // boundaries.
    auto upload_candidate = upload_with_locks.candidate;
    ASSERT_TRUE(!upload_candidate.sources.empty());
    ASSERT_EQ(upload_candidate.starting_offset, model::offset{10});
    ASSERT_EQ(upload_candidate.final_offset, model::offset{39});

    // Start with all the segments collected
    auto expected_content_length = collector.collected_size();
    // Deduct the starting shift
    expected_content_length -= upload_candidate.file_offset;
    // Deduct the entire last segment
    expected_content_length -= upload_candidate.sources.back()->size_bytes();
    // Add back the portion of the last segment we included
    expected_content_length += upload_candidate.final_file_offset;

    ASSERT_EQ(expected_content_length, upload_candidate.content_length);

    ASSERT_EQ(upload_with_locks.read_locks.size(), 1);
}

TEST(SegmentReuploadUnit, test_same_size_reupload_skipped) {
    // 'segment_collector' should not propose the re-upload
    // of a segment if the compacted size is equal to
    // the size of the segment in the manifest. In that case,
    // the resulting addresable name in cloud storage would be the
    // same for the segment before and after compaction. This would
    // result in the deletion of the segment.
    //
    // This test checks the invariant above.

    auto ntp = model::ntp{"test_ns", "test_tpc", 0};
    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    auto o = std::make_unique<ntp_config::default_overrides>();
    o->cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    b | start(ntp_config{ntp, {data_path}, std::move(o)});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // Create a segment containing two records with unique keys and
    // add it to the partition manifest.
    b | storage::add_segment(0) | storage::add_random_batch(0, 2);
    auto first_seg_size = b.get_segment(0).size_bytes();
    cloud_storage::partition_manifest m(ntp, model::initial_revision_id{1});
    m.add(
      segment_name("0-1-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = first_seg_size,
        .base_offset = model::offset(0),
        .committed_offset = model::offset(1),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});

    // Mark self compaction as complete on the segment and collect
    // segments for re-upload. The upload candidate should be a noop
    // since the selected reupload has the same size as the existing segment.
    b.get_segment(0).index().maybe_set_self_compact_timestamp(
      model::timestamp::now());
    b.get_segment(0).mark_as_finished_windowed_compaction();

    {
        archival::segment_collector collector{
          segment_collector_mode::compacted_reupload,
          model::offset{0},
          m,
          b.get_disk_log_impl(),
          first_seg_size};

        collector.collect_segments();
        ASSERT_EQ(collector.collected_size(), first_seg_size);
        ASSERT_TRUE(collector.should_replace_manifest_segment());

        require_skip_offset(
          collector.make_upload_candidate(1s).get(),
          candidate_creation_error::upload_size_unchanged,
          model::offset{1});
    }

    // Add another segment to the log and change the manifest to contain
    // only one segment that maps to the two local segments. This simulates
    // a potentila reupload after change of leadership.
    b | storage::add_segment(2) | storage::add_random_batch(2, 2);

    m = cloud_storage::partition_manifest(ntp, model::initial_revision_id{1});
    auto second_seg_size = b.get_segment(1).size_bytes();
    m.add(
      segment_name("0-1-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = first_seg_size + second_seg_size,
        .base_offset = model::offset(0),
        .committed_offset = model::offset(3),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});

    // Mark the second segment as having completed self compaction
    // and collect segments for re-upload again. Again, the upload candidate
    // should be a no-op since the reupload of the two local segments
    // results in a segment of the same size as the one that should be replaced.
    b.get_segment(1).index().maybe_set_self_compact_timestamp(
      model::timestamp::now());
    b.get_segment(1).mark_as_finished_windowed_compaction();

    {
        archival::segment_collector collector{
          segment_collector_mode::compacted_reupload,
          model::offset{0},
          m,
          b.get_disk_log_impl(),
          first_seg_size + second_seg_size};

        collector.collect_segments();
        ASSERT_EQ(collector.collected_size(), first_seg_size + second_seg_size);
        ASSERT_TRUE(collector.should_replace_manifest_segment());

        require_skip_offset(
          collector.make_upload_candidate(1s).get(),
          candidate_creation_error::upload_size_unchanged,
          model::offset{3});
    }
}

TEST(SegmentReuploadUnit, test_do_not_reupload_self_concatenated) {
    auto ntp = model::ntp{"test_ns", "test_tpc", 0};
    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    auto o = std::make_unique<ntp_config::default_overrides>();
    o->cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    b | start(ntp_config{ntp, {data_path}, std::move(o)});
    auto defer = ss::defer([&b] { b.stop().get(); });

    b | storage::add_segment(1000) | storage::add_random_batch(1000, 1000)
      | storage::add_segment(2000) | storage::add_random_batch(2000, 1000)
      | storage::add_segment(3000) | storage::add_random_batch(3000, 1000);

    auto seg_size = b.get_segment(0).size_bytes();
    cloud_storage::partition_manifest m(ntp, model::initial_revision_id{1});
    m.add(
      segment_name("1000-1999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(1000),
        .committed_offset = model::offset(1999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("2000-2999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(2000),
        .committed_offset = model::offset(2999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("3000-3999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(3000),
        .committed_offset = model::offset(3999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});

    b.update_start_offset(model::offset{3000}).get();
    b.get_segment(0).index().maybe_set_self_compact_timestamp(
      model::timestamp::now());
    b.get_segment(0).mark_as_finished_windowed_compaction();

    {
        archival::segment_collector collector{
          segment_collector_mode::compacted_reupload,
          model::offset{0},
          m,
          b.get_disk_log_impl(),
          seg_size * 10};

        collector.collect_segments();
        ASSERT_EQ(collector.segments().size(), 0);
        ASSERT_TRUE(!collector.should_replace_manifest_segment());
    }
}

TEST(SegmentReuploadUnit, test_do_not_reupload_prefix_truncated) {
    auto ntp = model::ntp{"test_ns", "test_tpc", 0};
    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    auto o = std::make_unique<ntp_config::default_overrides>();
    o->cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    b | start(ntp_config{ntp, {data_path}, std::move(o)});
    auto defer = ss::defer([&b] { b.stop().get(); });

    b | storage::add_segment(0) | storage::add_random_batch(0, 1000)
      | storage::add_segment(1000) | storage::add_random_batch(1000, 1000)
      | storage::add_segment(2000) | storage::add_random_batch(2000, 1000);

    // Set up our manifest to look as if our local data is a compacted version
    // of what's in the cloud.
    auto seg_size = b.get_segment(0).size_bytes();
    cloud_storage::partition_manifest m(ntp, model::initial_revision_id{1});
    m.add(
      segment_name("0-499-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(0),
        .committed_offset = model::offset(499),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("500-999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(500),
        .committed_offset = model::offset(999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("1000-1999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(1000),
        .committed_offset = model::offset(1999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("2000-2999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(2000),
        .committed_offset = model::offset(2999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});

    // Mark our local segments compacted, making them eligible for reupload.
    for (int i = 0; i < 3; i++) {
        b.get_segment(i).mark_as_compacted_segment();
        b.get_segment(i).index().maybe_set_self_compact_timestamp(
          model::timestamp::now());
        b.get_segment(i).mark_as_finished_windowed_compaction();
    }

    // Prefix truncate without aligning to a segment boundary, a la
    // delete-records.
    b.update_start_offset(model::offset{100}).get();

    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      seg_size * 10};

    // Since we can't replace offsets starting at 0, the first remote segment
    // isn't eligible for reupload and we should start from the next segment.
    collector.collect_segments();
    ASSERT_EQ(collector.begin_inclusive()(), 500);
    ASSERT_EQ(collector.segments().size(), 3);
    ASSERT_EQ(
      collector.segments()[0]->offsets().get_base_offset(), model::offset{0});
    ASSERT_TRUE(collector.should_replace_manifest_segment());
}

TEST(SegmentReuploadUnit, test_bump_start_when_not_aligned) {
    auto ntp = model::ntp{"test_ns", "test_tpc", 0};
    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    auto o = std::make_unique<ntp_config::default_overrides>();
    o->cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    b | start(ntp_config{ntp, {data_path}, std::move(o)});
    auto defer = ss::defer([&b] { b.stop().get(); });

    b | storage::add_segment(0) | storage::add_random_batch(0, 1000)
      | storage::add_segment(1000) | storage::add_random_batch(1000, 1000)
      | storage::add_segment(2000) | storage::add_random_batch(2000, 1000);

    // Set up our manifest to look as if our local data is a compacted version
    // of what's in the cloud.
    auto seg_size = b.get_segment(0).size_bytes();
    cloud_storage::partition_manifest m(ntp, model::initial_revision_id{1});
    m.add(
      segment_name("0-499-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(0),
        .committed_offset = model::offset(499),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("500-999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(500),
        .committed_offset = model::offset(999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("1000-1999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(1000),
        .committed_offset = model::offset(1999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});
    m.add(
      segment_name("2000-2999-v1.log"),
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = seg_size,
        .base_offset = model::offset(2000),
        .committed_offset = model::offset(2999),
        .delta_offset = model::offset_delta(0),
        .delta_offset_end = model::offset_delta(0)});

    // Mark our local segments compacted, making them eligible for reupload.
    for (int i = 0; i < 3; i++) {
        b.get_segment(i).mark_as_compacted_segment();
        b.get_segment(i).index().maybe_set_self_compact_timestamp(
          model::timestamp::now());
        b.get_segment(i).mark_as_finished_windowed_compaction();
    }

    // Try collecting from the middle of a local segment that hapens to align
    // with our manifest. The containing segment should be included, and the
    // start offset of the upload candidate should be aligned with our
    // manifest.
    archival::segment_collector collector{
      segment_collector_mode::compacted_reupload,
      model::offset{500},
      m,
      b.get_disk_log_impl(),
      seg_size * 10};

    collector.collect_segments();
    ASSERT_EQ(collector.begin_inclusive()(), 500);
    ASSERT_EQ(collector.segments().size(), 3);
    ASSERT_EQ(
      collector.segments()[0]->offsets().get_base_offset(), model::offset{0});
    ASSERT_TRUE(collector.should_replace_manifest_segment());
}

TEST(SegmentReuploadUnit, test_adjacent_segment_collection) {
    auto ntp = model::ntp{"test_ns", "test_tpc", 0};
    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    auto o = std::make_unique<ntp_config::default_overrides>();
    b | start(ntp_config{ntp, {data_path}, std::move(o)});
    auto defer = ss::defer([&b] { b.stop().get(); });

    /*
          +-----------------------------------++------------------------------+
  Local   |2                               115||116                        211|
          +-----------------------------------++------------------------------+
          +----------++----------++-----------++------------------------------+
  Cloud   |2      103||104    113||114     115||116                        211|
          +----------++----------++-----------++------------------------------+
    */

    b | storage::add_segment(0) | storage::add_random_batch(0, 2)
      | storage::add_segment(2) | storage::add_random_batch(2, 102)
      | storage::add_random_batch(104, 11) | storage::add_segment(115)
      | storage::add_random_batch(115, 96);

    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json,
       make_manifest_stream(test_manifest))
      .get();

    archival::segment_collector collector{
      segment_collector_mode::non_compacted_reupload,
      model::offset{104},
      m,
      b.get_disk_log_impl(),
      12001752,
      model::offset{115}};

    collector.collect_segments();
    auto candidate = require_upload_candidate(
      collector.make_upload_candidate(10s).get());
    ASSERT_EQ(candidate.candidate.starting_offset, model::offset{104});
    ASSERT_EQ(candidate.candidate.final_offset, model::offset{115});
}

static constexpr std::string_view cross_term_reupload_manifest = R"json({
  "version": 2,
  "namespace": "test-ns",
  "topic": "test-topic",
  "partition": 1,
  "revision": 21,
  "last_offset": 211,
  "segments": {
    "0-1-v1.log": {
      "base_offset": 0,
      "committed_offset": 100,
      "is_compacted": false,
      "size_bytes": 1000,
      "archiver_term": 1,
      "delta_offset": 0,
      "base_timestamp": 1686389191244,
      "max_timestamp": 1686389191244,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 1,
      "delta_offset_end": 0
    },
    "101-1-v1.log": {
      "base_offset": 101,
      "committed_offset": 200,
      "is_compacted": false,
      "size_bytes": 1000,
      "archiver_term": 1,
      "delta_offset": 0,
      "base_timestamp": 1686389202577,
      "max_timestamp": 1686389230060,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 1,
      "delta_offset_end": 0
    },
    "201-1-v1.log": {
      "base_offset": 201,
      "committed_offset": 300,
      "is_compacted": false,
      "size_bytes": 1000,
      "archiver_term": 1,
      "delta_offset": 0,
      "base_timestamp": 1686389230182,
      "max_timestamp": 1686389233222,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 1,
      "delta_offset_end": 0
    },
    "301-2-v1.log": {
      "base_offset": 301,
      "committed_offset": 400,
      "is_compacted": false,
      "size_bytes": 1000,
      "archiver_term": 1,
      "delta_offset": 0,
      "base_timestamp": 1686389230182,
      "max_timestamp": 1686389233222,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 1,
      "delta_offset_end": 0
    },
    "401-2-v1.log": {
      "base_offset": 401,
      "committed_offset": 500,
      "is_compacted": false,
      "size_bytes": 1000000,
      "archiver_term": 2,
      "delta_offset": 0,
      "base_timestamp": 1686389230182,
      "max_timestamp": 1686389233222,
      "ntp_revision": 21,
      "sname_format": 3,
      "segment_term": 2,
      "delta_offset_end": 0
    }
  }
})json";

TEST(SegmentReuploadUnit, test_adjacent_segment_collection_x_term) {
    // The test validates that the segments from different terms are
    // not merged.

    auto ntp = model::ntp{"test_ns", "test_tpc", 0};

    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json,
       make_manifest_stream(cross_term_reupload_manifest))
      .get();

    auto run = adjacent_segment_run(ntp);

    // This covers three segments with total size of 3000
    ASSERT_TRUE(!run.maybe_add_segment(
      m,
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 100,
        .base_offset = model::offset(0),
        .committed_offset = model::offset(300),
        .base_timestamp = model::timestamp(1686389191244),
        .max_timestamp = model::timestamp(1686389233222),
        .delta_offset = model::offset_delta(0),
        .ntp_revision = model::initial_revision_id(21),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(1),
        .delta_offset_end = model::offset_delta(0),
        .sname_format = cloud_storage::segment_name_format::v3,
      },
      5000,
      path_provider));

    ASSERT_TRUE(!run.maybe_add_segment(
      m,
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 100,
        .base_offset = model::offset(301),
        .committed_offset = model::offset(400),
        .base_timestamp = model::timestamp(1686389191244),
        .max_timestamp = model::timestamp(1686389233222),
        .delta_offset = model::offset_delta(0),
        .ntp_revision = model::initial_revision_id(21),
        .archiver_term = model::term_id(1),
        .segment_term = model::term_id(1),
        .delta_offset_end = model::offset_delta(0),
        .sname_format = cloud_storage::segment_name_format::v3,
      },
      5000,
      path_provider));

    // The extra segment fits in by size but can't be added because it
    // has different term. The method should return 'true' because
    // we were able to add a segment to the run and we can't extend it
    // further.
    ASSERT_TRUE(run.maybe_add_segment(
      m,
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 100,
        .base_offset = model::offset(401),
        .committed_offset = model::offset(500),
        .base_timestamp = model::timestamp(1686389191244),
        .max_timestamp = model::timestamp(1686389233222),
        .delta_offset = model::offset_delta(0),
        .ntp_revision = model::initial_revision_id(21),
        .archiver_term = model::term_id(2),
        .segment_term = model::term_id(2),
        .delta_offset_end = model::offset_delta(0),
        .sname_format = cloud_storage::segment_name_format::v3,
      },
      5000,
      path_provider));

    ASSERT_EQ(run.num_segments, 2);
    ASSERT_EQ(run.meta.base_offset(), 0);
    ASSERT_EQ(run.meta.committed_offset(), 400);
}

TEST(SegmentReuploadUnit, test_segment_concurrent_compaction) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // Local disk log starts before manifest and ends after manifest. First
    // three segments are compacted.
    populate_log(
      b,
      {.segment_starts = {0, 20, 30, 40},
       .compacted_segment_indices = {},
       .last_segment_num_records = 10});

    // Should be aligned to the manifest segments.
    archival::segment_collector collector{
      segment_collector_mode::non_compacted_reupload,
      model::offset{10},
      m,
      b.get_disk_log_impl(),
      max_upload_size,
      model::offset{39}};

    collector.collect_segments();
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});
    ASSERT_EQ(collector.end_inclusive(), model::offset{39});

    // Simulate concurrent compaction.
    // The collector already stores collected segments. After the compaction
    // the segments are still valid, but segment sizes could be different so
    // the collection should be retried.
    for (auto& s : b.get_disk_log_impl().segments()) {
        s->advance_generation();
    }

    // The three compacted segments are collected, with the begin and end
    // markers set to align with manifest segment.
    auto candidate = collector.make_upload_candidate(1s).get();
    ASSERT_TRUE(std::holds_alternative<candidate_creation_error>(candidate));
    ASSERT_TRUE(
      std::get<candidate_creation_error>(candidate)
      == candidate_creation_error::concurrency_error);
}

// When the last collected segment is unsealed (has appender) and multiple
// segments have generation changes, the upload should still fail with
// concurrency_error since we can't trust the sealed segments.
TEST(
  SegmentReuploadUnit,
  test_segment_concurrent_compaction_unsealed_tail_multi_gen_change) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // Three segments aligned with manifest boundaries. The last one
    // (starting at 30) is the active segment with an appender since no
    // further segment is created after it.
    populate_log(
      b,
      {.segment_starts = {10, 20, 30},
       .compacted_segment_indices = {},
       .last_segment_num_records = 10});

    archival::segment_collector collector{
      segment_collector_mode::non_compacted_reupload,
      model::offset{10},
      m,
      b.get_disk_log_impl(),
      max_upload_size,
      model::offset{39}};

    collector.collect_segments();
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});
    ASSERT_EQ(collector.end_inclusive(), model::offset{39});

    auto& segs = b.get_disk_log_impl().segments();
    ASSERT_TRUE(segs.back()->has_appender());

    // Advance generation on the middle segment AND the last (unsealed)
    // segment. Two segments have generation changes, so the skip logic
    // should not apply even though the tail is unsealed.
    std::next(segs.begin())->get()->advance_generation();
    segs.back()->advance_generation();

    auto candidate = collector.make_upload_candidate(1s).get();
    ASSERT_TRUE(std::holds_alternative<candidate_creation_error>(candidate));
    ASSERT_TRUE(
      std::get<candidate_creation_error>(candidate)
      == candidate_creation_error::concurrency_error);
}

// When the last collected segment is unsealed (has appender) and it is the
// ONLY segment with a generation change, the upload should proceed. This is
// expected because the active segment can legitimately change generation as
// new data is appended.
TEST(
  SegmentReuploadUnit,
  test_segment_concurrent_compaction_unsealed_tail_only_gen_change) {
    cloud_storage::partition_manifest m;
    m.update(
       cloud_storage::manifest_format::json, make_manifest_stream(manifest))
      .get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // Three segments aligned with manifest boundaries. The last one
    // (starting at 30) is the active segment with an appender since no
    // further segment is created after it.
    populate_log(
      b,
      {.segment_starts = {10, 20, 30},
       .compacted_segment_indices = {},
       .last_segment_num_records = 10});

    archival::segment_collector collector{
      segment_collector_mode::non_compacted_reupload,
      model::offset{10},
      m,
      b.get_disk_log_impl(),
      max_upload_size,
      model::offset{39}};

    collector.collect_segments();
    ASSERT_EQ(collector.begin_inclusive(), model::offset{10});
    ASSERT_EQ(collector.end_inclusive(), model::offset{39});

    auto& segs = b.get_disk_log_impl().segments();
    ASSERT_TRUE(segs.back()->has_appender());

    // Only advance generation on the last (unsealed) segment. Since it is
    // the only segment with a generation change and it has an appender, the
    // generation check should be skipped.
    segs.back()->advance_generation();

    auto candidate = collector.make_upload_candidate(1s).get();
    ASSERT_TRUE(std::holds_alternative<upload_candidate_with_locks>(candidate));
}

static void validate_non_compacted_collector(archival::segment_collector& c) {
    if (!c.segment_ready_for_upload()) {
        return;
    }
    auto interval = model::bounded_offset_interval::checked(
      c.begin_inclusive(), c.end_inclusive());

    auto collected_interval = model::bounded_offset_interval::checked(
      c.segments().front()->offsets().get_base_offset(),
      c.segments().back()->offsets().get_committed_offset());
    ASSERT_TRUE(collected_interval.contains(c.begin_inclusive()));
    ASSERT_TRUE(collected_interval.contains(c.end_inclusive()));

    // Unrelated segments should not be collected.
    for (const auto& s : c.segments()) {
        auto seg_interval = model::bounded_offset_interval::checked(
          s->offsets().get_base_offset(), s->offsets().get_committed_offset());
        ASSERT_TRUE(interval.overlaps(seg_interval));
    }

    // Check that collected segments are aligned
    auto prev = model::prev_offset(collected_interval.min());
    for (const auto& s : c.segments()) {
        ASSERT_TRUE(model::next_offset(prev) == s->offsets().get_base_offset());
        prev = s->offsets().get_committed_offset();
    }
}

TEST(SegmentReuploadUnit, test_new_segment_upload) {
    // Start with empty manifest and upload the log.
    cloud_storage::partition_manifest m(
      model::ntp{"test_ns", "test_tpc", 0}, model::initial_revision_id{0});

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // Local disk log starts before manifest and ends after manifest. First
    // three segments are compacted.
    populate_log(
      b,
      {.segment_starts = {0, 20, 30, 40},
       .compacted_segment_indices = {},
       .last_segment_num_records = 10});

    for (auto& s : b.get_disk_log_impl().segments()) {
        if (s->offsets().get_base_offset() == model::offset{40}) {
            // Keep last segment unsealed
            break;
        }
        s->release_appender()->close().get();
    }

    // Upload the first segment of the log. This replicates the normal
    // upload flow in the ntp_archiver. The archiver uploads segments
    // when they are sealed.
    archival::segment_collector collector1{
      segment_collector_mode::new_upload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector1.collect_segments();
    ASSERT_EQ(collector1.begin_inclusive(), model::offset{00});
    ASSERT_EQ(collector1.end_inclusive(), model::offset{19});
    validate_non_compacted_collector(collector1);

    // Upload the segment in the middle of the log.
    archival::segment_collector collector2{
      segment_collector_mode::new_upload,
      model::offset{20},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector2.collect_segments();
    ASSERT_EQ(collector2.begin_inclusive(), model::offset{20});
    ASSERT_EQ(collector2.end_inclusive(), model::offset{29});
    validate_non_compacted_collector(collector2);

    // Upload the last segment of the log. This should fail because
    // the segment is not sealed.
    archival::segment_collector collector3{
      segment_collector_mode::new_upload,
      model::offset{40},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector3.collect_segments();
    ASSERT_TRUE(!collector3.segment_ready_for_upload());
    validate_non_compacted_collector(collector3);

    // Collect multiple segments at once. This should succeed.
    // The collector will be guided by the target end offset
    // and max size. The collection will include the whole log
    // except the last unsealed segment.
    archival::segment_collector collector4{
      segment_collector_mode::new_upload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      max_upload_size,
      model::offset{39}};

    collector4.collect_segments();
    ASSERT_TRUE(collector4.segment_ready_for_upload());
    ASSERT_EQ(collector4.begin_inclusive(), model::offset{0});
    ASSERT_EQ(collector4.end_inclusive(), model::offset{39});
    validate_non_compacted_collector(collector4);

    // Same as the previous case but the target end offset is set to
    // be in the middle of the unsealed segment. The collector should
    // still be able to collect the same set of segments + part of the
    // last unsealed segment.
    // When the upload is forced by the timeout we have to upload all
    // data that we have up to the LSO. The LSO could be in the middle of
    // of the unsealed segment in this case.
    archival::segment_collector collector5{
      segment_collector_mode::new_upload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      max_upload_size,
      model::offset{42}};

    collector5.collect_segments();
    ASSERT_TRUE(collector5.segment_ready_for_upload());
    ASSERT_EQ(collector5.begin_inclusive(), model::offset{0});
    ASSERT_EQ(collector5.end_inclusive(), model::offset{42});
    validate_non_compacted_collector(collector5);

    // Similar to the previous case but the start of the upload is not aligned
    // to the segment boundary. So does the end of the upload.
    archival::segment_collector collector6{
      segment_collector_mode::new_upload,
      model::offset{8},
      m,
      b.get_disk_log_impl(),
      max_upload_size,
      model::offset{42}};
    collector6.collect_segments();
    ASSERT_TRUE(collector6.segment_ready_for_upload());
    ASSERT_EQ(collector6.begin_inclusive(), model::offset{8});
    ASSERT_EQ(collector6.end_inclusive(), model::offset{42});
    validate_non_compacted_collector(collector6);

    // The upload starts and stops inside the unsealed segment.
    archival::segment_collector collector7{
      segment_collector_mode::new_upload,
      model::offset{42},
      m,
      b.get_disk_log_impl(),
      max_upload_size,
      model::offset{45}};
    collector7.collect_segments();
    ASSERT_TRUE(collector7.segment_ready_for_upload());
    ASSERT_EQ(collector7.begin_inclusive(), model::offset{42});
    ASSERT_EQ(collector7.end_inclusive(), model::offset{45});
    validate_non_compacted_collector(collector7);

    // The upload size is limited by the size. The last offset is not set
    // explicitly. We should pick up only one segment.
    auto first_segment_size
      = b.get_disk_log_impl().segments().front()->file_size();
    archival::segment_collector collector8{
      segment_collector_mode::new_upload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      first_segment_size};
    collector8.collect_segments();
    ASSERT_TRUE(collector8.segment_ready_for_upload());
    ASSERT_EQ(collector8.begin_inclusive(), model::offset{0});
    ASSERT_EQ(collector8.end_inclusive(), model::offset{19});
    validate_non_compacted_collector(collector8);

    // Same case but the last offset is set explicitly. The collector should
    // chose the same segment as in the previous case.
    archival::segment_collector collector9{
      segment_collector_mode::new_upload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      first_segment_size,
      model::offset{42} /*this will be ignored*/};
    collector9.collect_segments();
    ASSERT_TRUE(collector9.segment_ready_for_upload());
    ASSERT_EQ(collector9.begin_inclusive(), model::offset{0});
    ASSERT_EQ(collector9.end_inclusive(), model::offset{19});
    validate_non_compacted_collector(collector9);

    // Similar to the previous case but the start of the upload is not aligned
    // with the segment boundary. The collector will stop on a segment boundary
    // anyway based on the segment size.
    archival::segment_collector collector10{
      segment_collector_mode::new_upload,
      model::offset{18},
      m,
      b.get_disk_log_impl(),
      first_segment_size,
      model::offset{42} /*this will be ignored*/};
    collector10.collect_segments();
    ASSERT_TRUE(collector10.segment_ready_for_upload());
    ASSERT_EQ(collector10.begin_inclusive(), model::offset{18});
    ASSERT_EQ(collector10.end_inclusive(), model::offset{19});
    validate_non_compacted_collector(collector10);
}

TEST(SegmentReuploadUnit, test_new_segment_upload_off_by_one) {
    // Upload segments that contain only one record so begin_inclusive
    // is equal to end_inclusive.
    cloud_storage::partition_manifest m(
      model::ntp{"test_ns", "test_tpc", 0}, model::initial_revision_id{0});

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // Local disk log starts before manifest and ends after manifest. First
    // three segments are compacted.
    populate_log(
      b,
      {.segment_starts = {0, 1, 2, 3},
       .compacted_segment_indices = {},
       .last_segment_num_records = 1});

    for (auto& s : b.get_disk_log_impl().segments()) {
        if (s->offsets().get_base_offset() == model::offset{3}) {
            // Keep last segment unsealed
            break;
        }
        s->release_appender()->close().get();
    }

    // Upload first segment which contains only one record [0-0 offset range]
    archival::segment_collector collector1{
      segment_collector_mode::new_upload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      max_upload_size};

    collector1.collect_segments();
    ASSERT_EQ(collector1.begin_inclusive(), model::offset{0});
    ASSERT_EQ(collector1.end_inclusive(), model::offset{0});
    ASSERT_TRUE(collector1.segment_ready_for_upload());
    validate_non_compacted_collector(collector1);

    // Upload second segment which contains only one record [1-1 offset range]
    archival::segment_collector collector2{
      segment_collector_mode::new_upload,
      model::offset{1},
      m,
      b.get_disk_log_impl(),
      max_upload_size};
    collector2.collect_segments();
    ASSERT_EQ(collector2.begin_inclusive(), model::offset{1});
    ASSERT_EQ(collector2.end_inclusive(), model::offset{1});
    ASSERT_TRUE(collector2.segment_ready_for_upload());
    validate_non_compacted_collector(collector2);

    // Upload second segment which contains only one record [3-3 offset range]
    archival::segment_collector collector3{
      segment_collector_mode::new_upload,
      model::offset{3},
      m,
      b.get_disk_log_impl(),
      max_upload_size};
    collector3.collect_segments();
    ASSERT_TRUE(!collector3.segment_ready_for_upload());
    validate_non_compacted_collector(collector3);

    // Upload series of segments. Last segment which is not sealed is not
    // included.
    archival::segment_collector collector4{
      segment_collector_mode::new_upload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      max_upload_size,
      model::offset{2}};
    collector4.collect_segments();
    ASSERT_EQ(collector4.begin_inclusive(), model::offset{0});
    ASSERT_EQ(collector4.end_inclusive(), model::offset{2});
    ASSERT_TRUE(collector4.segment_ready_for_upload());
    validate_non_compacted_collector(collector4);

    // Upload series of segments but include last segment which is not sealed.
    // This should work because target_end_inclusive is set to 3.
    // The 'target_end_inclusive' is supposed to be used to pass LSO value
    // to the collector. This means that the collector now knows that it's safe
    // to read from unsealed segment below this offset.
    archival::segment_collector collector5{
      segment_collector_mode::new_upload,
      model::offset{0},
      m,
      b.get_disk_log_impl(),
      max_upload_size,
      model::offset{3}};
    collector5.collect_segments();
    ASSERT_EQ(collector5.begin_inclusive(), model::offset{0});
    ASSERT_EQ(collector5.end_inclusive(), model::offset{3});
    ASSERT_TRUE(collector5.segment_ready_for_upload());
    validate_non_compacted_collector(collector5);
}

/// Collect all batch boundaries
struct consumer {
    ss::future<ss::stop_iteration> operator()(model::record_batch b) noexcept {
        auto interval = model::bounded_offset_interval::checked(
          b.base_offset(), b.last_offset());
        boundaries->push_back(interval);
        auto hdr_size = model::packed_record_batch_header_size;
        auto data_size = b.data().size_bytes();
        auto prev_size = running_sum->empty() ? 0 : running_sum->back();
        running_sum->push_back(prev_size + hdr_size + data_size);
        co_return ss::stop_iteration::no;
    }

    bool end_of_stream() const { return false; }

    std::vector<model::bounded_offset_interval>* boundaries;
    std::vector<size_t>* running_sum;
};

TEST(SegmentReuploadUnit, test_new_segment_upload_fuzz) {
    static ss::logger testlog("test_new_segment_upload_fuzz");
    cloud_storage::partition_manifest m(
      model::ntp{"test_ns", "test_tpc", 0}, model::initial_revision_id{0});

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // Local disk log starts before manifest and ends after manifest. First
    // three segments are compacted.

    const int max_records_per_batch = 100;
    const int batches_per_segment = 100;
    const int num_segments = 20;
    model::offset start_offset{0};
    model::term_id start_term{0};
    for (int segment_ix = 0; segment_ix < num_segments; segment_ix++) {
        auto num_batches = random_generators::get_int(1, batches_per_segment);
        vlog(
          testlog.info,
          "Add segment{}: {} {}",
          segment_ix,
          start_offset,
          start_term);

        b.add_segment(start_offset, start_term).get();
        for (int batch_ix = 0; batch_ix < num_batches; batch_ix++) {
            auto num_records = random_generators::get_int(
              1, max_records_per_batch);
            vlog(testlog.info, "Add batch: {} {}", start_offset, num_records);
            b.add_random_batch(start_offset, num_records).get();
            start_offset = model::offset(start_offset() + num_records);
        }
        // Maybe bump the term id of the next segment
        // start_term = random_generators::get_int(2) == 0 ? start_term :
        // model::term_id(start_term() + 1);
    }

    model::offset last_offset = b.get_disk_log_impl()
                                  .segments()
                                  .back()
                                  ->offsets()
                                  .get_committed_offset();

    std::vector<model::bounded_offset_interval> batch_boundaries;
    std::vector<model::bounded_offset_interval> segment_boundaries;
    std::vector<size_t> acc_batch_size;

    // Find all segment boundaries
    for (auto& s : b.get_disk_log_impl().segments()) {
        auto interval = model::bounded_offset_interval::checked(
          s->offsets().get_base_offset(), s->offsets().get_committed_offset());
        segment_boundaries.push_back(interval);
        vlog(
          testlog.info,
          "Segment boundaries: {} - {} (closed: {})",
          s->offsets().get_base_offset(),
          s->offsets().get_committed_offset(),
          s->is_closed());
        if (s->has_appender() && !s->is_closed()) {
            s->release_appender()->close().get();
        }
    }

    // Consume the log to find all batch boundaries
    auto reader = b.get_disk_log_impl()
                    .make_reader(
                      storage::local_log_reader_config(
                        model::offset{0}, model::offset{last_offset}))
                    .get();

    vlog(testlog.info, "Consuming from the log 0 - {}", last_offset);
    std::move(reader)
      .consume(
        consumer{
          .boundaries = &batch_boundaries, .running_sum = &acc_batch_size},
        model::no_timeout)
      .get();

    for (auto t : batch_boundaries) {
        vlog(testlog.info, "Batch boundaries: {} - {}", t.min(), t.max());
    }
    vlog(testlog.info, "Done consuming from the log");

    // In a loop generate a random offset range and check that
    // the collector is able to collect the segments that are in the range.
    for (int i = 0; i < 25; i++) {
        auto ix_start = random_generators::get_int(
          0UL, batch_boundaries.size() - 1);
        auto ix_end = random_generators::get_int(
          0UL, batch_boundaries.size() - 1);
        if (ix_start == ix_end) {
            continue;
        } else if (ix_start > ix_end) {
            std::swap(ix_start, ix_end);
        }
        auto start_bound = batch_boundaries.at(ix_start);
        auto end_bound = batch_boundaries.at(ix_end);
        auto expected_size
          = acc_batch_size.at(ix_end)
            - (ix_start == 0 ? 0 : acc_batch_size.at(ix_start - 1));

        auto check_upload_candidate = [](
                                        segment_collector& collector,
                                        auto start_bound,
                                        auto end_bound,
                                        size_t expected_size) {
            auto candidate = collector.make_upload_candidate(10s).get();
            ASSERT_TRUE(
              std::holds_alternative<upload_candidate_with_locks>(candidate));
            auto& c = std::get<upload_candidate_with_locks>(candidate);
            vlog(
              testlog.info,
              "Upload candidate: {}-{}, content length: {}",
              c.candidate.starting_offset,
              c.candidate.final_offset,
              c.candidate.content_length);
            ASSERT_EQ(
              std::get<upload_candidate_with_locks>(candidate)
                .candidate.starting_offset,
              start_bound.min());
            ASSERT_EQ(
              std::get<upload_candidate_with_locks>(candidate)
                .candidate.final_offset,
              end_bound.max());
            auto candidate_size = std::get<upload_candidate_with_locks>(
                                    candidate)
                                    .candidate.content_length;
            ASSERT_EQ(candidate_size, expected_size);
        };

        // Case 1:
        // Specify the full range and expect the collector to collect
        // all batches in the range. The size should match the expectation
        // precisely.
        // This code path is used when we're reuploading the offset range
        // or when we're uploading new segment but forcing upload up to LSO.
        vlog(
          testlog.info,
          "Collecting {} bytes starting from the offset {}-{}",
          expected_size,
          start_bound.min(),
          end_bound.max());
        archival::segment_collector closed_range_collector{
          segment_collector_mode::new_upload,
          start_bound.min(),
          m,
          b.get_disk_log_impl(),
          expected_size
            * 10, // Make sure the size limit is not affecting anything
          end_bound.max()};

        closed_range_collector.collect_segments();

        validate_non_compacted_collector(closed_range_collector);
        check_upload_candidate(
          closed_range_collector, start_bound, end_bound, expected_size);

        // Case 2:
        // In this case the end offset is not specified. The collector
        // should be able to collect the segments in the range based on
        // size limit (size limit is set to match the expected size).
        // This code path is used when we're uploading new non compacted
        // segments.

        // Since this will upload only one segment we need to adjust the
        // end offset and the size.
        bool batch_found = false;
        for (auto s : segment_boundaries) {
            if (s.contains(start_bound.min())) {
                // The uploaded segment is found. Now we need to find the end
                // batch.
                ix_end = ix_start;
                for (auto i = ix_start; i < batch_boundaries.size(); i++) {
                    if (batch_boundaries.at(i).contains(s.max())) {
                        ix_end = i;
                        end_bound = batch_boundaries.at(i);
                        expected_size
                          = acc_batch_size.at(ix_end)
                            - (ix_start == 0 ? 0 : acc_batch_size.at(ix_start - 1));
                        batch_found = true;
                        vlog(
                          testlog.info,
                          "New range: {}-{}, new content length: {}",
                          start_bound.min(),
                          end_bound.max(),
                          expected_size);
                        break;
                    }
                }
                break;
            }
        }
        ASSERT_TRUE(batch_found);
        vlog(
          testlog.info,
          "Collecting {} bytes starting from the offset {}-{}",
          expected_size,
          start_bound.min(),
          end_bound.max());
        archival::segment_collector open_range_collector{
          segment_collector_mode::new_upload,
          start_bound.min(),
          m,
          b.get_disk_log_impl(),
          expected_size,
          std::nullopt};

        open_range_collector.collect_segments();
        validate_non_compacted_collector(open_range_collector);
        check_upload_candidate(
          open_range_collector, start_bound, end_bound, expected_size);
    }
}

TEST(SegmentReuploadUnit, test_new_segment_never_skip_offsets) {
    // Start with empty manifest and try to upload
    cloud_storage::partition_manifest m(
      model::ntp{"test_ns", "test_tpc", 0}, model::initial_revision_id{0});

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    populate_log(
      b,
      {.segment_starts = {0, 20},
       .compacted_segment_indices = {},
       .last_segment_num_records = 10});

    {
        // with a clean manifest, we should be able to upload the segment, but
        // the begin offset lands inside a batch.
        archival::segment_collector collector{
          segment_collector_mode::new_upload,
          model::offset{10},
          m,
          b.get_disk_log_impl(),
          max_upload_size,
          model::offset{19},
        };
        collector.collect_segments();

        auto c = collector.make_upload_candidate_stream(1s).get();
        ASSERT_TRUE(std::holds_alternative<candidate_creation_error>(c));
        ASSERT_TRUE(
          std::get<candidate_creation_error>(c)
          == candidate_creation_error::offset_inside_batch);
    }
}
