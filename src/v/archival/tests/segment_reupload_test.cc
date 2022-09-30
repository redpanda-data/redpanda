/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/segment_reupload.h"
#include "cloud_storage/partition_manifest.h"
#include "storage/log_manager.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/tmp_dir.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

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

static constexpr size_t max_upload_size{4096_KiB};

ss::input_stream<char> make_manifest_stream(std::string_view json) {
    iobuf i;
    i.append(json.data(), json.size());
    return make_iobuf_input_stream(std::move(i));
}

struct log_spec {
    std::vector<size_t> segment_starts;
    std::vector<size_t> compacted_segment_indices;
    size_t last_segment_num_records;
};

storage::disk_log_builder make_log_builder(std::string_view data_path) {
    return storage::disk_log_builder{storage::log_config{
      storage::log_config::storage_type::disk,
      {data_path.data(), data_path.size()},
      4_KiB,
      storage::debug_sanitize_files::yes,
    }};
}

static void populate_log(storage::disk_log_builder& b, const log_spec& spec) {
    auto first = spec.segment_starts.begin();
    auto second = std::next(first);
    for (; second != spec.segment_starts.end(); ++first, ++second) {
        auto num_records = *second - *first;
        b | storage::add_segment(*first)
          | storage::add_random_batch(*first, num_records);
    }
    b | storage::add_segment(*first)
      | storage::add_random_batch(*first, spec.last_segment_num_records);

    for (auto i : spec.compacted_segment_indices) {
        b.get_segment(i).mark_as_finished_self_compaction();
    }
}

SEASTAR_THREAD_TEST_CASE(test_segment_collection) {
    cloud_storage::partition_manifest m;
    m.update(make_manifest_stream(manifest)).get();

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

    auto& disk_log = b.get_disk_log_impl();

    archival::segment_collector collector{
      model::offset{4}, &m, &disk_log, max_upload_size};

    collector.collect_segments();

    // The three compacted segments are collected, with the begin and end
    // markers set to align with manifest segment.
    BOOST_REQUIRE(collector.can_replace_manifest_segment());
    BOOST_REQUIRE_EQUAL(collector.begin_inclusive(), model::offset{10});
    BOOST_REQUIRE_EQUAL(collector.end_inclusive(), model::offset{39});
    BOOST_REQUIRE_EQUAL(3, collector.take_segments().size());
}

SEASTAR_THREAD_TEST_CASE(test_start_ahead_of_manifest) {
    cloud_storage::partition_manifest m;
    m.update(make_manifest_stream(manifest)).get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());
    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    {
        // start ahead of manifest end, no collection happens.
        archival::segment_collector collector{
          model::offset{400}, &m, &b.get_disk_log_impl(), max_upload_size};

        collector.collect_segments();

        BOOST_REQUIRE_EQUAL(false, collector.can_replace_manifest_segment());
        auto segments = collector.take_segments();
        BOOST_REQUIRE(segments.empty());
    }

    {
        // start at manifest end. the collector will advance it first to prevent
        // overlap. no collection happens.
        archival::segment_collector collector{
          model::offset{39}, &m, &b.get_disk_log_impl(), max_upload_size};

        collector.collect_segments();

        BOOST_REQUIRE_EQUAL(false, collector.can_replace_manifest_segment());
        auto segments = collector.take_segments();
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
      model::offset{2}, &m, &b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE_EQUAL(false, collector.can_replace_manifest_segment());
    BOOST_REQUIRE(collector.take_segments().empty());
}

SEASTAR_THREAD_TEST_CASE(test_short_compacted_segment_inside_manifest_segment) {
    cloud_storage::partition_manifest m;
    m.update(make_manifest_stream(manifest)).get();

    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    auto b = make_log_builder(data_path.string());

    b | start(ntp_config{{"test_ns", "test_tpc", 0}, {data_path}});
    auto defer = ss::defer([&b] { b.stop().get(); });

    // segment [12-14] lies inside manifest segment [10-19]. start offset 12 is
    // adjusted to 19+1 = 20 to avoid overlap with manifest segment. no
    // collection will be done after adjustment.
    populate_log(
      b,
      {.segment_starts = {12},
       .compacted_segment_indices = {0},
       .last_segment_num_records = 2});

    archival::segment_collector collector{
      model::offset{12}, &m, &b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE_EQUAL(false, collector.can_replace_manifest_segment());
    BOOST_REQUIRE(collector.take_segments().empty());
}

SEASTAR_THREAD_TEST_CASE(test_compacted_segment_aligned_with_manifest_segment) {
    cloud_storage::partition_manifest m;
    m.update(make_manifest_stream(manifest)).get();

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
      model::offset{10}, &m, &b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE(collector.can_replace_manifest_segment());
    auto segments = collector.take_segments();
    BOOST_REQUIRE_EQUAL(1, segments.size());

    const auto& seg = segments.front();
    BOOST_REQUIRE_EQUAL(seg->offsets().base_offset, model::offset{10});
    BOOST_REQUIRE_EQUAL(seg->offsets().committed_offset, model::offset{19});
}

SEASTAR_THREAD_TEST_CASE(
  test_short_compacted_segment_aligned_with_manifest_segment) {
    cloud_storage::partition_manifest m;
    m.update(make_manifest_stream(manifest)).get();

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
      model::offset{10}, &m, &b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE_EQUAL(false, collector.can_replace_manifest_segment());
    auto segments = collector.take_segments();
    BOOST_REQUIRE_EQUAL(1, segments.size());

    const auto& seg = segments.front();
    BOOST_REQUIRE_EQUAL(seg->offsets().base_offset, model::offset{10});
    BOOST_REQUIRE_EQUAL(seg->offsets().committed_offset, model::offset{14});
}

SEASTAR_THREAD_TEST_CASE(
  test_many_compacted_segments_make_up_to_manifest_segment) {
    cloud_storage::partition_manifest m;
    m.update(make_manifest_stream(manifest)).get();

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
      model::offset{10}, &m, &b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE(collector.can_replace_manifest_segment());
    auto segments = collector.take_segments();
    BOOST_REQUIRE_EQUAL(5, segments.size());
    BOOST_REQUIRE_EQUAL(collector.begin_inclusive(), model::offset{10});
    BOOST_REQUIRE_EQUAL(collector.end_inclusive(), model::offset{19});
}

SEASTAR_THREAD_TEST_CASE(test_compacted_segment_larger_than_manifest_segment) {
    cloud_storage::partition_manifest m;
    m.update(make_manifest_stream(manifest)).get();

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
      model::offset{8}, &m, &b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE(collector.can_replace_manifest_segment());
    auto segments = collector.take_segments();

    BOOST_REQUIRE_EQUAL(1, segments.size());

    // Begin and end markers are aligned to manifest segment.
    BOOST_REQUIRE_EQUAL(collector.begin_inclusive(), model::offset{10});
    BOOST_REQUIRE_EQUAL(collector.end_inclusive(), model::offset{19});
}

SEASTAR_THREAD_TEST_CASE(test_collect_capped_by_size) {
    cloud_storage::partition_manifest m;
    m.update(make_manifest_stream(manifest)).get();

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
      model::offset{5}, &m, &b.get_disk_log_impl(), max_size};

    collector.collect_segments();

    BOOST_REQUIRE(collector.can_replace_manifest_segment());
    auto segments = collector.take_segments();

    BOOST_REQUIRE_EQUAL(3, segments.size());

    // Begin marker starts on first manifest segment boundary.
    BOOST_REQUIRE_EQUAL(collector.begin_inclusive(), model::offset{10});

    // End marker ends on second manifest segment boundary.
    BOOST_REQUIRE_EQUAL(collector.end_inclusive(), model::offset{29});
}

SEASTAR_THREAD_TEST_CASE(test_no_compacted_segments) {
    cloud_storage::partition_manifest m;
    m.update(make_manifest_stream(manifest)).get();

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
      model::offset{5}, &m, &b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();

    BOOST_REQUIRE_EQUAL(false, collector.can_replace_manifest_segment());
    BOOST_REQUIRE(collector.take_segments().empty());
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_adjustment) {
    cloud_storage::partition_manifest m;
    m.update(make_manifest_stream(manifest)).get();

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
      model::offset{8}, &m, &b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();
    auto name = collector.adjust_segment_name();
    BOOST_REQUIRE_EQUAL(name, cloud_storage::segment_name{"10-0-v1.log"});
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_no_adjustment) {
    cloud_storage::partition_manifest m;
    m.update(make_manifest_stream(manifest)).get();

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
      model::offset{8}, &m, &b.get_disk_log_impl(), max_upload_size};

    collector.collect_segments();
    auto name = collector.adjust_segment_name();
    BOOST_REQUIRE_EQUAL(name, cloud_storage::segment_name{"10-0-v1.log"});
}
