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
#include "cluster/archival/segment_reupload.h"
#include "cluster/archival/tests/service_fixture.h"
#include "model/metadata.h"
#include "storage/log_manager.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/archival.h"
#include "test_utils/tmp_dir.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>

using namespace archival;

namespace {
static const cloud_storage::remote_path_provider
  path_provider(std::nullopt, std::nullopt);
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

SEASTAR_THREAD_TEST_CASE(test_segment_collection) {
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
      model::offset{4}, m, b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    // The three compacted segments are collected, with the begin and end
    // markers set to align with manifest segment.
    BOOST_REQUIRE(collector.should_replace_manifest_segment());
    BOOST_REQUIRE_EQUAL(collector.begin_inclusive(), model::offset{10});
    BOOST_REQUIRE_EQUAL(collector.end_inclusive(), model::offset{39});
    BOOST_REQUIRE_EQUAL(3, collector.segments().size());
}

SEASTAR_THREAD_TEST_CASE(test_start_ahead_of_manifest) {
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
          model::offset{400}, m, b.get_disk_log_impl(), max_upload_size};

        collector.collect_segments();

        BOOST_REQUIRE_EQUAL(false, collector.should_replace_manifest_segment());
        auto segments = collector.segments();
        BOOST_REQUIRE(segments.empty());
    }

    {
        // start at manifest end. the collector will advance it first to prevent
        // overlap. no collection happens.
        archival::segment_collector collector{
          model::offset{39}, m, b.get_disk_log_impl(), max_upload_size};

        collector.collect_segments();

        BOOST_REQUIRE_EQUAL(false, collector.should_replace_manifest_segment());
        auto segments = collector.segments();
        BOOST_REQUIRE(segments.empty());
    }
}

SEASTAR_THREAD_TEST_CASE(test_empty_manifest) {
    cloud_storage::partition_manifest m;

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    archival::segment_collector collector{
      model::offset{2}, m, b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE_EQUAL(false, collector.should_replace_manifest_segment());
    BOOST_REQUIRE(collector.segments().empty());
}

SEASTAR_THREAD_TEST_CASE(test_short_compacted_segment_inside_manifest_segment) {
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
      model::offset{1}, m, b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE_EQUAL(false, collector.should_replace_manifest_segment());
    BOOST_REQUIRE_EQUAL(collector.segments().size(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_compacted_segment_aligned_with_manifest_segment) {
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
      model::offset{1}, m, b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE(collector.should_replace_manifest_segment());
    auto segments = collector.segments();
    BOOST_REQUIRE_EQUAL(1, segments.size());

    const auto& seg = segments.front();
    BOOST_REQUIRE_EQUAL(seg->offsets().get_base_offset(), model::offset{10});
    BOOST_REQUIRE_EQUAL(
      seg->offsets().get_committed_offset(), model::offset{19});
}

SEASTAR_THREAD_TEST_CASE(
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
      model::offset{0}, m, b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE_EQUAL(false, collector.should_replace_manifest_segment());
    auto segments = collector.segments();
    BOOST_REQUIRE_EQUAL(1, segments.size());

    const auto& seg = segments.front();
    BOOST_REQUIRE_EQUAL(seg->offsets().get_base_offset(), model::offset{10});
    BOOST_REQUIRE_EQUAL(
      seg->offsets().get_committed_offset(), model::offset{14});
}

SEASTAR_THREAD_TEST_CASE(
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
      model::offset{0}, m, b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE(collector.should_replace_manifest_segment());
    auto segments = collector.segments();
    BOOST_REQUIRE_EQUAL(5, segments.size());
    BOOST_REQUIRE_EQUAL(collector.begin_inclusive(), model::offset{10});
    BOOST_REQUIRE_EQUAL(collector.end_inclusive(), model::offset{19});
}

SEASTAR_THREAD_TEST_CASE(test_compacted_segment_larger_than_manifest_segment) {
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
      model::offset{2}, m, b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE(collector.should_replace_manifest_segment());
    auto segments = collector.segments();

    BOOST_REQUIRE_EQUAL(1, segments.size());

    // Begin and end markers are aligned to manifest segment.
    BOOST_REQUIRE_EQUAL(collector.begin_inclusive(), model::offset{10});
    BOOST_REQUIRE_EQUAL(collector.end_inclusive(), model::offset{19});
}

SEASTAR_THREAD_TEST_CASE(test_collect_capped_by_size) {
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
      model::offset{0}, m, b.get_disk_log_impl(), max_size};

    collector.collect_segments();

    BOOST_REQUIRE(collector.should_replace_manifest_segment());
    auto segments = collector.segments();

    BOOST_REQUIRE_EQUAL(3, segments.size());

    // Begin marker starts on first manifest segment boundary.
    BOOST_REQUIRE_EQUAL(collector.begin_inclusive(), model::offset{10});

    // End marker ends on second manifest segment boundary.
    BOOST_REQUIRE_EQUAL(collector.end_inclusive(), model::offset{29});

    size_t collected_size = std::transform_reduce(
      segments.begin(), segments.end(), 0, std::plus<>{}, [](const auto& seg) {
          return seg->size_bytes();
      });
    BOOST_REQUIRE_LE(collected_size, max_size);
}

SEASTAR_THREAD_TEST_CASE(test_no_compacted_segments) {
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
      model::offset{5}, m, b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE_EQUAL(false, collector.should_replace_manifest_segment());
    BOOST_REQUIRE(collector.segments().empty());
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_adjustment) {
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
      model::offset{8}, m, b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();
    auto name = collector.adjust_segment_name();
    BOOST_REQUIRE_EQUAL(name, cloud_storage::segment_name{"10-0-v1.log"});
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_no_adjustment) {
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
      model::offset{8}, m, b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();
    auto name = collector.adjust_segment_name();
    BOOST_REQUIRE_EQUAL(name, cloud_storage::segment_name{"10-0-v1.log"});
}

SEASTAR_THREAD_TEST_CASE(test_collected_segments_completely_cover_gap) {
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
          model::offset{0}, m, b.get_disk_log_impl(), max_size};

        collector.collect_segments();

        BOOST_REQUIRE(collector.should_replace_manifest_segment());
        auto segments = collector.segments();

        BOOST_REQUIRE_EQUAL(3, segments.size());

        // Collection start aligned to manifest start at 10
        BOOST_REQUIRE_EQUAL(collector.begin_inclusive(), model::offset{10});

        // End marker adjusted to the end of the gap.
        BOOST_REQUIRE_EQUAL(collector.end_inclusive(), model::offset{29});

        size_t collected_size = std::transform_reduce(
          segments.begin(),
          segments.end(),
          0,
          std::plus<>{},
          [](const auto& seg) { return seg->size_bytes(); });
        BOOST_REQUIRE_LE(collected_size, max_size);
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
          model::offset{0}, m, b.get_disk_log_impl(), max_size};

        collector.collect_segments();

        BOOST_REQUIRE(collector.should_replace_manifest_segment());
        auto segments = collector.segments();

        BOOST_REQUIRE_EQUAL(3, segments.size());

        // Collection start aligned to manifest start at 10
        BOOST_REQUIRE_EQUAL(collector.begin_inclusive(), model::offset{10});

        // End marker adjusted to the end of the gap.
        BOOST_REQUIRE_EQUAL(collector.end_inclusive(), model::offset{39});

        size_t collected_size = std::transform_reduce(
          segments.begin(),
          segments.end(),
          0,
          std::plus<>{},
          [](const auto& seg) { return seg->size_bytes(); });
        BOOST_REQUIRE_LE(collected_size, max_size);
    }
}

SEASTAR_THREAD_TEST_CASE(test_compacted_segment_after_manifest_start) {
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
      model::offset{0}, m, b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();
    BOOST_REQUIRE(collector.should_replace_manifest_segment());
    BOOST_REQUIRE_EQUAL(1, collector.segments().size());
    BOOST_REQUIRE_EQUAL(collector.begin_inclusive(), model::offset{20});
    BOOST_REQUIRE_EQUAL(collector.end_inclusive(), model::offset{39});
}

SEASTAR_THREAD_TEST_CASE(test_upload_candidate_generation) {
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
        b.get_segment(i).mark_as_finished_self_compaction();
        b.get_segment(i).mark_as_finished_windowed_compaction();
    }

    size_t max_size = b.get_segment(0).size_bytes()
                      + b.get_segment(1).size_bytes()
                      + b.get_segment(2).size_bytes();
    archival::segment_collector collector{
      model::offset{5}, m, b.get_disk_log_impl(), max_size};

    collector.collect_segments();
    BOOST_REQUIRE(collector.should_replace_manifest_segment());

    auto upload_with_locks = require_upload_candidate(
      collector
        .make_upload_candidate(
          ss::default_priority_class(), segment_lock_timeout)
        .get());

    auto upload_candidate = upload_with_locks.candidate;
    BOOST_REQUIRE(!upload_candidate.sources.empty());
    BOOST_REQUIRE_EQUAL(upload_candidate.starting_offset, model::offset{10});
    BOOST_REQUIRE_EQUAL(upload_candidate.final_offset, model::offset{29});

    // Start with all the segments collected
    auto expected_content_length = collector.collected_size();
    // Deduct the starting shift
    expected_content_length -= upload_candidate.file_offset;
    // Deduct the entire last segment
    expected_content_length -= upload_candidate.sources.back()->size_bytes();
    // Add back the portion of the last segment we included
    expected_content_length += upload_candidate.final_file_offset;

    BOOST_REQUIRE_EQUAL(
      expected_content_length, upload_candidate.content_length);

    BOOST_REQUIRE_EQUAL(upload_with_locks.read_locks.size(), 3);
}

SEASTAR_THREAD_TEST_CASE(test_upload_aligned_to_non_existent_offset) {
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
    for (; second != spec.segment_starts.end(); ++first, ++second) {
        b | storage::add_segment(*first);
        for (auto curr_offset = *first; curr_offset < *second; ++curr_offset) {
            b.add_random_batch(model::test::record_batch_spec{
                                 .offset = model::offset{curr_offset},
                                 .count = 1,
                                 .max_key_cardinality = 1,
                               })
              .get();
        }
        auto seg = b.get_log_segments().back();
        seg->appender().close().get();
        seg->release_appender().get();
    }

    b | storage::add_segment(*first)
      | storage::add_random_batch(*first, spec.last_segment_num_records);

    // Compaction will rewrite each segment.
    b.gc(model::timestamp::max(), std::nullopt).get();

    size_t max_size = b.get_segment(0).size_bytes()
                      + b.get_segment(1).size_bytes()
                      + b.get_segment(2).size_bytes();
    archival::segment_collector collector{
      model::offset{5}, m, b.get_disk_log_impl(), max_size};

    collector.collect_segments();
    BOOST_REQUIRE(collector.should_replace_manifest_segment());

    auto upload_with_locks = require_upload_candidate(
      collector
        .make_upload_candidate(
          ss::default_priority_class(), segment_lock_timeout)
        .get());

    auto upload_candidate = upload_with_locks.candidate;
    BOOST_REQUIRE(!upload_candidate.sources.empty());
    BOOST_REQUIRE_EQUAL(upload_candidate.starting_offset, model::offset{10});
    BOOST_REQUIRE_EQUAL(upload_candidate.final_offset, model::offset{29});

    // Start with all the segments collected
    auto expected_content_length = collector.collected_size();
    // Deduct the starting shift
    expected_content_length -= upload_candidate.file_offset;
    // Deduct the entire last segment
    expected_content_length -= upload_candidate.sources.back()->size_bytes();
    // Add back the portion of the last segment we included
    expected_content_length += upload_candidate.final_file_offset;

    BOOST_REQUIRE_EQUAL(
      expected_content_length, upload_candidate.content_length);

    BOOST_REQUIRE_EQUAL(upload_with_locks.read_locks.size(), 3);
}

SEASTAR_THREAD_TEST_CASE(test_same_size_reupload_skipped) {
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
    b.get_segment(0).mark_as_finished_self_compaction();
    b.get_segment(0).mark_as_finished_windowed_compaction();

    {
        archival::segment_collector collector{
          model::offset{0}, m, b.get_disk_log_impl(), first_seg_size};

        collector.collect_segments();
        BOOST_REQUIRE_EQUAL(collector.collected_size(), first_seg_size);
        BOOST_REQUIRE(collector.should_replace_manifest_segment());

        require_skip_offset(
          collector.make_upload_candidate(ss::default_priority_class(), 1s)
            .get(),
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
    b.get_segment(1).mark_as_finished_self_compaction();
    b.get_segment(1).mark_as_finished_windowed_compaction();

    {
        archival::segment_collector collector{
          model::offset{0},
          m,
          b.get_disk_log_impl(),
          first_seg_size + second_seg_size};

        collector.collect_segments();
        BOOST_REQUIRE_EQUAL(
          collector.collected_size(), first_seg_size + second_seg_size);
        BOOST_REQUIRE(collector.should_replace_manifest_segment());

        require_skip_offset(
          collector.make_upload_candidate(ss::default_priority_class(), 1s)
            .get(),
          candidate_creation_error::upload_size_unchanged,
          model::offset{3});
    }
}

SEASTAR_THREAD_TEST_CASE(test_do_not_reupload_self_concatenated) {
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
    b.get_segment(0).mark_as_finished_self_compaction();
    b.get_segment(0).mark_as_finished_windowed_compaction();

    {
        archival::segment_collector collector{
          model::offset{0}, m, b.get_disk_log_impl(), seg_size * 10};

        collector.collect_segments();
        BOOST_REQUIRE_EQUAL(collector.segments().size(), 0);
        BOOST_REQUIRE(!collector.should_replace_manifest_segment());
    }
}

SEASTAR_THREAD_TEST_CASE(test_do_not_reupload_prefix_truncated) {
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
        b.get_segment(i).mark_as_finished_self_compaction();
        b.get_segment(i).mark_as_finished_windowed_compaction();
    }

    // Prefix truncate without aligning to a segment boundary, a la
    // delete-records.
    b.update_start_offset(model::offset{100}).get();

    archival::segment_collector collector{
      model::offset{0}, m, b.get_disk_log_impl(), seg_size * 10};

    // Since we can't replace offsets starting at 0, the first remote segment
    // isn't eligible for reupload and we should start from the next segment.
    collector.collect_segments();
    BOOST_REQUIRE_EQUAL(collector.begin_inclusive()(), 500);
    BOOST_REQUIRE_EQUAL(collector.segments().size(), 3);
    BOOST_REQUIRE_EQUAL(
      collector.segments()[0]->offsets().get_base_offset(), model::offset{0});
    BOOST_REQUIRE(collector.should_replace_manifest_segment());
}

SEASTAR_THREAD_TEST_CASE(test_bump_start_when_not_aligned) {
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
        b.get_segment(i).mark_as_finished_self_compaction();
        b.get_segment(i).mark_as_finished_windowed_compaction();
    }

    // Try collecting from the middle of a local segment that hapens to align
    // with our manifest. The containing segment should be included, and the
    // start offset of the upload candidate should be aligned with our
    // manifest.
    archival::segment_collector collector{
      model::offset{500}, m, b.get_disk_log_impl(), seg_size * 10};
    collector.collect_segments();
    BOOST_REQUIRE_EQUAL(collector.begin_inclusive()(), 500);
    BOOST_REQUIRE_EQUAL(collector.segments().size(), 3);
    BOOST_REQUIRE_EQUAL(
      collector.segments()[0]->offsets().get_base_offset(), model::offset{0});
    BOOST_REQUIRE(collector.should_replace_manifest_segment());
}

SEASTAR_THREAD_TEST_CASE(test_adjacent_segment_collection) {
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
      model::offset{104},
      m,
      b.get_disk_log_impl(),
      12001752,
      model::offset{115}};

    collector.collect_segments(segment_collector_mode::collect_non_compacted);
    auto candidate = require_upload_candidate(
      collector.make_upload_candidate(ss::default_priority_class(), 10s).get());
    BOOST_REQUIRE_EQUAL(
      candidate.candidate.starting_offset, model::offset{104});
    BOOST_REQUIRE_EQUAL(candidate.candidate.final_offset, model::offset{115});
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

SEASTAR_THREAD_TEST_CASE(test_adjacent_segment_collection_x_term) {
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
    BOOST_REQUIRE(!run.maybe_add_segment(
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

    BOOST_REQUIRE(!run.maybe_add_segment(
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
    BOOST_REQUIRE(run.maybe_add_segment(
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

    BOOST_REQUIRE_EQUAL(run.num_segments, 2);
    BOOST_REQUIRE_EQUAL(run.meta.base_offset(), 0);
    BOOST_REQUIRE_EQUAL(run.meta.committed_offset(), 400);
}
