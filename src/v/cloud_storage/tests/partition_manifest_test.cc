/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "seastarx.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

using namespace cloud_storage;

static constexpr std::string_view empty_manifest_json = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 0,
    "last_offset": 0
})json";
static constexpr std::string_view complete_manifest_json = R"json({
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
            "committed_offset": 19,
            "base_timestamp": 123000,
            "max_timestamp": 123009
        },
        "20-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 2048,
            "base_offset": 20,
            "committed_offset": 29,
            "base_timestamp": 123010,
            "max_timestamp": 123019
        },
        "30-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 30,
            "committed_offset": 39,
            "base_timestamp": 123020,
            "max_timestamp": 123029,
            "delta_offset": 1
        },
        "40-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 40,
            "committed_offset": 49,
            "base_timestamp": 123030,
            "max_timestamp": 123039,
            "delta_offset": 2,
            "archiver_term": 42,
            "segment_term": 11,
            "delta_offset_end": 9,
            "sname_format": 2
        }
    }
})json";

static constexpr std::string_view max_segment_meta_manifest_json = R"json({
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
            "committed_offset": 19,
            "base_timestamp" : 123456,
            "max_timestamp": 123456789,
            "delta_offset": 12313,
            "ntp_revision": 3,
            "archiver_term" : 3
        }
    }
})json";

static constexpr std::string_view no_size_bytes_segment_meta = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 1,
    "last_offset": 39,
    "segments": {
        "10-1-v1.log": {
            "is_compacted": false,
            "base_offset": 10,
            "committed_offset": 19
        }
    }
})json";

static constexpr std::string_view no_timestamps_segment_meta = R"json({
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
        }
    }
})json";

static constexpr std::string_view segment_meta_gets_smaller_manifest_json
  = R"json({
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
            "committed_offset": 19,
            "base_timestamp" : 123456,
            "max_timestamp": 123456789,
            "delta_offset": 12313,
            "ntp_revision": 3,
            "archiver_term" : 3
        },
        "20-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 2048,
            "base_offset": 20,
            "committed_offset": 29
        }
    }
})json";

static constexpr std::string_view no_closing_bracket_segment_meta = R"json({
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
        }
})json";

static constexpr std::string_view fields_after_segments_json = R"json({
    "segments": {
        "10-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 1024,
            "base_offset": 10,
            "committed_offset": 19
        }
    },
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 1,
    "last_offset": 39
})json";

static constexpr std::string_view missing_last_offset_manifest_json = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 1,
    "segments": {
        "10-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 1024,
            "base_offset": 10,
            "committed_offset": 19
        }
    }
})json";

/**
 * This manifest has gaps in both its offsets and timestamps.
 */
constexpr std::string_view manifest_with_gaps = R"json({
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
            "committed_offset": 19,
            "base_timestamp": 123001,
            "max_timestamp": 123020
        },
        "20-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 2048,
            "base_offset": 20,
            "committed_offset": 29,
            "base_timestamp": 123021,
            "max_timestamp": 123030
        },
        "30-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 30,
            "committed_offset": 39,
            "base_timestamp": 123031,
            "max_timestamp": 123040
        },
        "50-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 50,
            "committed_offset": 59,
            "base_timestamp": 123051,
            "max_timestamp": 123060
        }
    }
    })json";

static const model::ntp manifest_ntp(
  model::ns("test-ns"), model::topic("test-topic"), model::partition_id(42));

inline ss::input_stream<char> make_manifest_stream(std::string_view json) {
    iobuf i;
    i.append(json.data(), json.size());
    return make_iobuf_input_stream(std::move(i));
}

SEASTAR_THREAD_TEST_CASE(test_segment_contains) {
    partition_manifest m;
    m.update(make_manifest_stream(manifest_with_gaps)).get0();
    BOOST_REQUIRE(m.segment_containing(model::offset{9}) == m.end());
    BOOST_REQUIRE(m.segment_containing(model::offset{90}) == m.end());
    BOOST_REQUIRE(
      m.segment_containing(model::offset{39}) == std::next(m.begin(), 2));
    BOOST_REQUIRE(m.segment_containing(model::offset{10}) == m.begin());
    BOOST_REQUIRE(m.segment_containing(model::offset{15}) == m.begin());
    BOOST_REQUIRE(
      m.segment_containing(model::offset{22}) == std::next(m.begin()));
    BOOST_REQUIRE(
      m.segment_containing(model::offset{38}) == std::next(m.begin(), 2));
    BOOST_REQUIRE(m.segment_containing(model::offset{42}) == m.end());
    BOOST_REQUIRE(
      m.segment_containing(model::offset{50}) == std::prev(m.end()));
    BOOST_REQUIRE(
      m.segment_containing(model::offset{52}) == std::prev(m.end()));
    BOOST_REQUIRE(
      m.segment_containing(model::offset{59}) == std::prev(m.end()));
}

SEASTAR_THREAD_TEST_CASE(test_manifest_type) {
    partition_manifest m;
    BOOST_REQUIRE(m.get_manifest_type() == manifest_type::partition);
}

SEASTAR_THREAD_TEST_CASE(test_segment_path) {
    auto path = generate_remote_segment_path(
      manifest_ntp,
      model::initial_revision_id(0),
      segment_name("22-11-v1.log"),
      model::term_id{123});
    // use pre-calculated murmur hash value from full ntp path + file name
    BOOST_REQUIRE_EQUAL(
      path, "2bea9275/test-ns/test-topic/42_0/22-11-v1.log.123");
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing) {
    std::filesystem::path path
      = "b0000000/meta/kafka/redpanda-test/4_2/manifest.json";
    auto res = cloud_storage::get_partition_manifest_path_components(path);
    BOOST_REQUIRE_EQUAL(res->_origin, path);
    BOOST_REQUIRE_EQUAL(res->_ns(), "kafka");
    BOOST_REQUIRE_EQUAL(res->_topic(), "redpanda-test");
    BOOST_REQUIRE_EQUAL(res->_part(), 4);
    BOOST_REQUIRE_EQUAL(res->_rev(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing_failure_1) {
    std::filesystem::path path
      = "b0000000/meta/kafka/redpanda-test/a_b/manifest.json";
    auto res = cloud_storage::get_partition_manifest_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing_failure_2) {
    std::filesystem::path path
      = "b0000000/kafka/redpanda-test/4_2/manifest.json";
    auto res = cloud_storage::get_partition_manifest_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing_failure_3) {
    std::filesystem::path path
      = "b0000000/meta/kafka/redpanda-test//manifest.json";
    auto res = cloud_storage::get_partition_manifest_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing_failure_4) {
    std::filesystem::path path
      = "b0000000/meta/kafka/redpanda-test/4_2/foo.bar";
    auto res = cloud_storage::get_partition_manifest_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_parsing) {
    segment_name name{"3587-1-v1.log"};
    auto res = parse_segment_name(name);
    BOOST_REQUIRE(res);
    BOOST_REQUIRE_EQUAL(res->base_offset(), 3587);
    BOOST_REQUIRE_EQUAL(res->term(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_parsing_failure_1) {
    segment_name name{"-1-v1.log"};
    auto res = parse_segment_name(name);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_parsing_failure_2) {
    segment_name name{"abc-1-v1.log"};
    auto res = parse_segment_name(name);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_path) {
    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "20000000/meta/test-ns/test-topic/42_0/manifest.json");
}

SEASTAR_THREAD_TEST_CASE(test_empty_manifest_update) {
    partition_manifest m;
    m.update(make_manifest_stream(empty_manifest_json)).get0();
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "20000000/meta/test-ns/test-topic/42_0/manifest.json");
}

void require_equal_segment_meta(
  partition_manifest::segment_meta expected,
  partition_manifest::segment_meta actual) {
    BOOST_REQUIRE_EQUAL(expected.is_compacted, actual.is_compacted);
    BOOST_REQUIRE_EQUAL(expected.size_bytes, actual.size_bytes);
    BOOST_REQUIRE_EQUAL(expected.base_offset, actual.base_offset);
    BOOST_REQUIRE_EQUAL(expected.committed_offset, actual.committed_offset);
    BOOST_REQUIRE_EQUAL(expected.base_timestamp, actual.base_timestamp);
    BOOST_REQUIRE_EQUAL(expected.max_timestamp, actual.max_timestamp);
    BOOST_REQUIRE_EQUAL(expected.delta_offset, actual.delta_offset);
    BOOST_REQUIRE_EQUAL(expected.ntp_revision, actual.ntp_revision);
    BOOST_REQUIRE_EQUAL(expected.archiver_term, actual.archiver_term);
}

SEASTAR_THREAD_TEST_CASE(test_complete_manifest_update) {
    partition_manifest m;
    m.update(make_manifest_stream(complete_manifest_json)).get0();
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "60000000/meta/test-ns/test-topic/42_1/manifest.json");
    BOOST_REQUIRE_EQUAL(m.size(), 4);
    std::map<ss::sstring, partition_manifest::segment_meta> expected = {
      {"10-1-v1.log",
       partition_manifest::segment_meta{
         .is_compacted = false,
         .size_bytes = 1024,
         .base_offset = model::offset(10),
         .committed_offset = model::offset(19),
         .base_timestamp = model::timestamp(123000),
         .max_timestamp = model::timestamp(123009),
         .ntp_revision = model::initial_revision_id(
           1) // revision is propagated from manifest
       }},
      {"20-1-v1.log",
       partition_manifest::segment_meta{
         .is_compacted = false,
         .size_bytes = 2048,
         .base_offset = model::offset(20),
         .committed_offset = model::offset(29),
         .base_timestamp = model::timestamp(123010),
         .max_timestamp = model::timestamp(123019),
         .ntp_revision = model::initial_revision_id(
           1) // revision is propagated from manifest
       }},
      {"30-1-v1.log",
       partition_manifest::segment_meta{
         .is_compacted = false,
         .size_bytes = 4096,
         .base_offset = model::offset(30),
         .committed_offset = model::offset(39),
         .base_timestamp = model::timestamp(123020),
         .max_timestamp = model::timestamp(123029),
         .delta_offset = model::offset_delta(1),
         .ntp_revision = model::initial_revision_id(
           1), // revision is propagated from manifest
       }},
      {"40-1-v1.log",
       partition_manifest::segment_meta{
         .is_compacted = false,
         .size_bytes = 4096,
         .base_offset = model::offset(40),
         .committed_offset = model::offset(49),
         .base_timestamp = model::timestamp(123030),
         .max_timestamp = model::timestamp(123039),
         .delta_offset = model::offset_delta(2),
         .ntp_revision = model::initial_revision_id(
           1), // revision is propagated from manifest
         .archiver_term = model::term_id{42},
         .segment_term = model::term_id{11},
         .delta_offset_end = model::offset_delta(9),
         .sname_format = segment_name_format::v2,
       }},
    };
    for (const auto& actual : m) {
        auto sn = generate_local_segment_name(
          actual.first.base_offset, actual.first.term);
        auto it = expected.find(sn());
        BOOST_REQUIRE(it != expected.end());
        require_equal_segment_meta(it->second, actual.second);
    }
}

SEASTAR_THREAD_TEST_CASE(test_max_segment_meta_update) {
    partition_manifest m;
    m.update(make_manifest_stream(max_segment_meta_manifest_json)).get0();
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "60000000/meta/test-ns/test-topic/42_1/manifest.json");
    BOOST_REQUIRE_EQUAL(m.size(), 1);
    std::map<ss::sstring, partition_manifest::segment_meta> expected = {
      {"10-1-v1.log",
       partition_manifest::segment_meta{
         false,
         1024,
         model::offset(10),
         model::offset(19),
         model::timestamp(123456),
         model::timestamp(123456789),
         model::offset_delta(12313),
         model::initial_revision_id(3),
         model::term_id(3)}}};

    for (const auto& actual : m) {
        auto sn = generate_local_segment_name(
          actual.first.base_offset, actual.first.term);
        auto it = expected.find(sn());
        BOOST_REQUIRE(it != expected.end());
        require_equal_segment_meta(it->second, actual.second);
    }
}

SEASTAR_THREAD_TEST_CASE(test_no_size_bytes_segment_meta) {
    partition_manifest m;
    BOOST_REQUIRE_EXCEPTION(
      m.update(make_manifest_stream(no_size_bytes_segment_meta)).get0(),
      std::runtime_error,
      [](std::runtime_error ex) {
          return std::string(ex.what()).find(
                   "Missing size_bytes value in {10-1-v1.log} segment meta")
                 != std::string::npos;
      });
}

/**
 * Segment metadata with missing timestamps are default initialized
 */
SEASTAR_THREAD_TEST_CASE(test_timestamps_segment_meta) {
    partition_manifest m;
    m.update(make_manifest_stream(no_timestamps_segment_meta)).get0();
    std::map<ss::sstring, partition_manifest::segment_meta> expected = {
      {"10-1-v1.log",
       partition_manifest::segment_meta{
         .is_compacted = false,
         .size_bytes = 1024,
         .base_offset = model::offset(10),
         .committed_offset = model::offset(19),
         .ntp_revision = model::initial_revision_id(
           1) // revision is propagated from manifest
       }}};
}

SEASTAR_THREAD_TEST_CASE(test_metas_get_smaller) {
    partition_manifest m;
    m.update(make_manifest_stream(segment_meta_gets_smaller_manifest_json))
      .get0();
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "60000000/meta/test-ns/test-topic/42_1/manifest.json");
    BOOST_REQUIRE_EQUAL(m.size(), 2);
    std::map<ss::sstring, partition_manifest::segment_meta> expected = {
      {"10-1-v1.log",
       partition_manifest::segment_meta{
         .is_compacted = false,
         .size_bytes = 1024,
         .base_offset = model::offset(10),
         .committed_offset = model::offset(19),
         .base_timestamp = model::timestamp(123456),
         .max_timestamp = model::timestamp(123456789),
         .delta_offset = model::offset_delta(12313),
         .ntp_revision = model::initial_revision_id(3),
         .archiver_term = model::term_id(3)}},
      {"20-1-v1.log",
       partition_manifest::segment_meta{
         .is_compacted = false,
         .size_bytes = 2048,
         .base_offset = model::offset(20),
         .committed_offset = model::offset(29),
         .ntp_revision = model::initial_revision_id(
           1)}}, // if ntp_revision if missing in meta, we get revision from
                 // manifest
    };
    for (const auto& actual : m) {
        auto sn = generate_local_segment_name(
          actual.first.base_offset, actual.first.term);
        auto it = expected.find(sn());
        BOOST_REQUIRE(it != expected.end());
        require_equal_segment_meta(it->second, actual.second);
    }
}

SEASTAR_THREAD_TEST_CASE(test_no_closing_bracket_meta) {
    partition_manifest m;
    BOOST_REQUIRE_EXCEPTION(
      m.update(make_manifest_stream(no_closing_bracket_segment_meta)).get0(),
      std::runtime_error,
      [](std::runtime_error ex) {
          return std::string(ex.what()).find(
                   "Failed to parse topic manifest "
                   "{\"b0000000/meta///-2147483648_-9223372036854775808/"
                   "manifest.json\"}: Missing a comma or '}' after an object "
                   "member. at offset 325")
                 != std::string::npos;
      });
}

SEASTAR_THREAD_TEST_CASE(test_fields_after_segments) {
    partition_manifest m;
    m.update(make_manifest_stream(fields_after_segments_json)).get0();
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "60000000/meta/test-ns/test-topic/42_1/manifest.json");
    BOOST_REQUIRE_EQUAL(m.size(), 1);
    std::map<ss::sstring, partition_manifest::segment_meta> expected = {
      {"10-1-v1.log",
       partition_manifest::segment_meta{
         false, 1024, model::offset(10), model::offset(19)}}};

    for (const auto& actual : m) {
        auto sn = generate_local_segment_name(
          actual.first.base_offset, actual.first.term);
        auto it = expected.find(sn());
        BOOST_REQUIRE(it != expected.end());
        require_equal_segment_meta(it->second, actual.second);
    }
}

SEASTAR_THREAD_TEST_CASE(test_missing_manifest_field) {
    partition_manifest m;
    BOOST_REQUIRE_EXCEPTION(
      m.update(make_manifest_stream(missing_last_offset_manifest_json)).get0(),
      std::runtime_error,
      [](std::runtime_error ex) {
          return std::string(ex.what()).find(
                   "Missing last_offset value in partition manifest")
                 != std::string::npos;
      });
}

SEASTAR_THREAD_TEST_CASE(test_manifest_serialization) {
    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    m.add(
      segment_name("10-1-v1.log"),
      {
        .is_compacted = false,
        .size_bytes = 1024,
        .base_offset = model::offset(10),
        .committed_offset = model::offset(19),
        .max_timestamp = model::timestamp::missing(),
        .ntp_revision = model::initial_revision_id(0),
      });
    m.add(
      segment_name("20-1-v1.log"),
      {
        .is_compacted = false,
        .size_bytes = 2048,
        .base_offset = model::offset(20),
        .committed_offset = model::offset(29),
        .max_timestamp = model::timestamp::missing(),
        .ntp_revision = model::initial_revision_id(3),
      });
    auto [is, size] = m.serialize();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    partition_manifest restored;
    restored.update(std::move(rstr)).get0();

    BOOST_REQUIRE(m == restored);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_replaced) {
    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    m.add(
      segment_name("0-1-v1.log"),
      {.base_offset = model::offset{0},
       .committed_offset = model::offset{9},
       .sname_format = segment_name_format::v2});
    m.add(
      segment_name("10-1-v1.log"),
      {.base_offset = model::offset{10},
       .committed_offset = model::offset{19},
       .sname_format = segment_name_format::v2});
    m.add(
      segment_name("20-1-v1.log"),
      {.base_offset = model::offset{20},
       .committed_offset = model::offset{29},
       .sname_format = segment_name_format::v2});
    m.add(
      segment_name("30-1-v1.log"),
      {.base_offset = model::offset{30},
       .committed_offset = model::offset{39},
       .sname_format = segment_name_format::v2});
    m.add(
      segment_name("40-1-v1.log"),
      {.base_offset = model::offset{40},
       .committed_offset = model::offset{49},
       .sname_format = segment_name_format::v2});
    // There shouldn't be any replaced segments
    {
        auto res = m.replaced_segments();
        BOOST_REQUIRE(res.empty());
    }
    // Try to replace in the middle
    m.add(
      segment_name("20-1-v1.log"),
      {.base_offset = model::offset{20},
       .committed_offset = model::offset{29},
       .sname_format = segment_name_format::v2});
    {
        auto res = m.replaced_segments();
        BOOST_REQUIRE(res.size() == 1);
        BOOST_REQUIRE(res[0].meta.base_offset == model::offset{20});
        BOOST_REQUIRE(res[0].meta.committed_offset == model::offset{29});
    }
    // Replace several segments
    m.add(
      segment_name("0-1-v1.log"),
      {.base_offset = model::offset{0},
       .committed_offset = model::offset{29},
       .sname_format = segment_name_format::v2});
    {
        auto res = m.replaced_segments();
        BOOST_REQUIRE(res.size() == 4);
        BOOST_REQUIRE(res[0].meta.base_offset == model::offset{0});
        BOOST_REQUIRE(res[0].meta.committed_offset == model::offset{9});
        BOOST_REQUIRE(res[1].meta.base_offset == model::offset{10});
        BOOST_REQUIRE(res[1].meta.committed_offset == model::offset{19});
        BOOST_REQUIRE(res[2].meta.base_offset == model::offset{20});
        BOOST_REQUIRE(res[2].meta.committed_offset == model::offset{29});
        // The segment with base offset 20 was replaced twice
        BOOST_REQUIRE(res[3].meta.base_offset == model::offset{20});
        BOOST_REQUIRE(res[3].meta.committed_offset == model::offset{29});
    }
}

namespace cloud_storage {

struct partition_manifest_accessor {
    static void add_replaced_segment(
      partition_manifest* m,
      const segment_name& key,
      const partition_manifest::segment_meta& meta) {
        auto comp = parse_segment_name(key);
        m->_replaced.insert(std::make_pair(*comp, meta));
    }
};
} // namespace cloud_storage

// modeled after cluster::archival_metadata_stm::segment
struct metadata_stm_segment
  : public serde::envelope<
      metadata_stm_segment,
      serde::version<0>,
      serde::compat_version<0>> {
    cloud_storage::segment_name name;
    cloud_storage::partition_manifest::segment_meta meta;

    bool operator==(const metadata_stm_segment&) const = default;
};

namespace old {
struct segment_meta_v0 {
    using value_t = segment_meta_v0;
    static constexpr serde::version_t redpanda_serde_version = 0;
    static constexpr serde::version_t redpanda_serde_compat_version = 0;

    bool is_compacted;
    size_t size_bytes;
    model::offset base_offset;
    model::offset committed_offset;
    model::timestamp base_timestamp;
    model::timestamp max_timestamp;
    model::offset delta_offset;

    auto operator<=>(const segment_meta_v0&) const = default;
};
struct segment_meta_v1 {
    using value_t = segment_meta_v1;
    static constexpr serde::version_t redpanda_serde_version = 1;
    static constexpr serde::version_t redpanda_serde_compat_version = 0;

    bool is_compacted;
    size_t size_bytes;
    model::offset base_offset;
    model::offset committed_offset;
    model::timestamp base_timestamp;
    model::timestamp max_timestamp;
    model::offset delta_offset;

    model::initial_revision_id ntp_revision;
    model::term_id archiver_term;

    auto operator<=>(const segment_meta_v1&) const = default;
};

template<class segment_meta_t>
struct metadata_stm_segment
  : public serde::envelope<
      metadata_stm_segment<segment_meta_t>,
      serde::version<0>,
      serde::compat_version<0>> {
    cloud_storage::segment_name name;
    segment_meta_t meta;

    bool operator==(const metadata_stm_segment&) const = default;
};

} // namespace old

template<class segment_meta_prev>
void test_segment_meta_serde_compat_impl(
  cloud_storage::partition_manifest::segment_meta meta_new,
  cloud_storage::partition_manifest::segment_meta meta_wo_new_fields,
  segment_meta_prev meta_old) {
    BOOST_CHECK(
      serde::from_iobuf<cloud_storage::partition_manifest::segment_meta>(
        serde::to_iobuf(meta_old))
      == meta_wo_new_fields);

    BOOST_CHECK(
      serde::from_iobuf<segment_meta_prev>(serde::to_iobuf(meta_new))
      == meta_old);

    auto name = segment_name{"12-11-v1.log"};

    metadata_stm_segment segment_new{
      .name = name,
      .meta = meta_new,
    };
    metadata_stm_segment segment_wo_new_fields{
      .name = name,
      .meta = meta_wo_new_fields,
    };
    old::metadata_stm_segment<segment_meta_prev> segment_old{
      .name = name,
      .meta = meta_old,
    };

    BOOST_CHECK(
      serde::from_iobuf<metadata_stm_segment>(serde::to_iobuf(segment_old))
      == segment_wo_new_fields);

    BOOST_CHECK(
      serde::from_iobuf<old::metadata_stm_segment<segment_meta_prev>>(
        serde::to_iobuf(segment_new))
      == segment_old);
}

SEASTAR_THREAD_TEST_CASE(test_segment_meta_serde_compat_v1) {
    auto timestamp = model::timestamp::now();
    cloud_storage::partition_manifest::segment_meta meta_new{
      .is_compacted = false,
      .size_bytes = 1234,
      .base_offset = model::offset{12},
      .committed_offset = model::offset{34},
      .base_timestamp = timestamp,
      .max_timestamp = timestamp,
      .delta_offset = model::offset_delta{7},
      .ntp_revision = model::initial_revision_id{42},
      .archiver_term = model::term_id{123},
    };
    cloud_storage::partition_manifest::segment_meta meta_wo_new_fields
      = meta_new;
    meta_wo_new_fields.ntp_revision = model::initial_revision_id{};
    meta_wo_new_fields.archiver_term = model::term_id{};

    old::segment_meta_v0 meta_old{
      .is_compacted = meta_new.is_compacted,
      .size_bytes = meta_new.size_bytes,
      .base_offset = meta_new.base_offset,
      .committed_offset = meta_new.committed_offset,
      .base_timestamp = meta_new.base_timestamp,
      .max_timestamp = meta_new.max_timestamp,
      .delta_offset = model::offset_cast(meta_new.delta_offset),
    };

    test_segment_meta_serde_compat_impl(meta_new, meta_wo_new_fields, meta_old);
}

SEASTAR_THREAD_TEST_CASE(test_segment_meta_serde_compat_v2) {
    auto timestamp = model::timestamp::now();
    cloud_storage::partition_manifest::segment_meta meta_new{
      .is_compacted = false,
      .size_bytes = 1234,
      .base_offset = model::offset{12},
      .committed_offset = model::offset{34},
      .base_timestamp = timestamp,
      .max_timestamp = timestamp,
      .delta_offset = model::offset_delta{7},
      .ntp_revision = model::initial_revision_id{42},
      .archiver_term = model::term_id{123},
      .segment_term = model::term_id{2},
      .delta_offset_end = model::offset_delta{11},
      .sname_format = segment_name_format::v2,
    };
    cloud_storage::partition_manifest::segment_meta meta_wo_new_fields
      = meta_new;
    meta_wo_new_fields.segment_term = model::term_id{};
    meta_wo_new_fields.delta_offset_end = model::offset_delta{};
    meta_wo_new_fields.sname_format = segment_name_format::v1;

    old::segment_meta_v1 meta_old{
      .is_compacted = meta_new.is_compacted,
      .size_bytes = meta_new.size_bytes,
      .base_offset = meta_new.base_offset,
      .committed_offset = meta_new.committed_offset,
      .base_timestamp = meta_new.base_timestamp,
      .max_timestamp = meta_new.max_timestamp,
      .delta_offset = model::offset_cast(meta_new.delta_offset),
      .ntp_revision = model::initial_revision_id{42},
      .archiver_term = model::term_id{123},
    };

    test_segment_meta_serde_compat_impl(meta_new, meta_wo_new_fields, meta_old);
}

/**
 * Reference implementation of partition_manifest::timequery
 */
std::optional<partition_manifest::segment_meta>
reference_timequery(const partition_manifest& m, model::timestamp t) {
    auto segment_iter = m.begin();
    while (segment_iter != m.end()) {
        auto base_timestamp = segment_iter->second.base_timestamp;
        auto max_timestamp = segment_iter->second.max_timestamp;
        if (base_timestamp > t || max_timestamp >= t) {
            break;
        } else {
            ++segment_iter;
        }
    }

    if (segment_iter == m.end()) {
        return std::nullopt;
    } else {
        return segment_iter->second;
    }
}

/**
 * Assert equality of results from actual implementation
 * and reference implementation at a particualr timestamp.
 */
void reference_check_timequery(
  const partition_manifest& m, model::timestamp t) {
    auto result = m.timequery(t);
    auto reference_result = reference_timequery(m, t);
    BOOST_REQUIRE(result == reference_result);
}

/**
 * Generalized variation of scan_ts_segments that doesn't
 * make assumptions about the overlapping-ness of segments: this
 * does its own linear scan of segments, as a reference implementation
 * of timequery.
 */
void scan_ts_segments_general(partition_manifest const& m) {
    // Before range: should get first segment
    reference_check_timequery(
      m, model::timestamp{m.begin()->second.base_timestamp() - 100});

    for (partition_manifest::const_iterator i = m.begin(); i != m.end(); ++i) {
        auto& s = i->second;

        // Time queries on times within the segment's range should return
        // this segment.
        reference_check_timequery(m, s.base_timestamp);
        reference_check_timequery(m, s.max_timestamp);
    }

    // After range: should get null
    reference_check_timequery(
      m, model::timestamp{m.rbegin()->second.max_timestamp() + 1});
}
/**
 * For a timequery() result, assert it is non-null and equal to the expected
 * offset
 */
void expect_ts_segment(
  std::optional<partition_manifest::segment_meta> const& s, int base_offset) {
    BOOST_REQUIRE(s);
    BOOST_REQUIRE_EQUAL(s->base_offset, model::offset{base_offset});
}

/**
 * For non-overlapping time ranges on segments, verify that each timestamp
 * maps to the expected segment.
 */
void scan_ts_segments(partition_manifest const& m) {
    // Before range: should get first segment
    expect_ts_segment(
      m.timequery(model::timestamp{m.begin()->second.base_timestamp() - 100}),
      m.begin()->second.base_offset);

    partition_manifest::const_iterator previous;
    for (partition_manifest::const_iterator i = m.begin(); i != m.end(); ++i) {
        auto& s = i->second;

        // Time queries on times within the segment's range should return
        // this segment.
        expect_ts_segment(m.timequery(s.base_timestamp), s.base_offset);
        expect_ts_segment(m.timequery(s.max_timestamp), s.base_offset);

        previous = i;
    }

    // After range: should get null
    BOOST_REQUIRE(
      m.timequery(model::timestamp{m.rbegin()->second.max_timestamp() + 1})
      == std::nullopt);

    // Also compare all results with reference implementation.
    scan_ts_segments_general(m);
}

/**
 * Test time queries on a manifest with contiguous non-overlapping time ranges.
 */
SEASTAR_THREAD_TEST_CASE(test_timequery_complete) {
    partition_manifest m;
    m.update(make_manifest_stream(complete_manifest_json)).get0();

    scan_ts_segments(m);
}

/**
 * Test time query when there is only one segment in the manifest
 */
SEASTAR_THREAD_TEST_CASE(test_timequery_single_segment) {
    partition_manifest m;
    m.update(make_manifest_stream(max_segment_meta_manifest_json)).get0();

    scan_ts_segments(m);
}

SEASTAR_THREAD_TEST_CASE(test_timequery_gaps) {
    partition_manifest m;
    m.update(make_manifest_stream(manifest_with_gaps)).get0();

    // Timestamps within the segments and off the ends
    scan_ts_segments(m);

    // Timestamp in the gap
    expect_ts_segment(m.timequery(model::timestamp{123045}), 50);
}

SEASTAR_THREAD_TEST_CASE(test_timequery_wide_range) {
    // Intentionaly ruin the initial interpolation guess by setting
    // a minimum timestamp bound very far away from other timestamps: this
    // makes sure we aren't just getting lucky in other tests when their guesses
    // happen to hit the right segment in one shot.

    static constexpr std::string_view wide_timestamps = R"json({
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
            "committed_offset": 19,
            "base_timestamp": 123000,
            "max_timestamp": 123009
        },
        "20-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 2048,
            "base_offset": 20,
            "committed_offset": 29,
            "base_timestamp": 923010,
            "max_timestamp": 923019
        },
        "30-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 30,
            "committed_offset": 39,
            "base_timestamp": 923020,
            "max_timestamp": 923029
        },
        "40-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 40,
            "committed_offset": 49,
            "base_timestamp": 923030,
            "max_timestamp": 923039
        }
    }
})json";

    partition_manifest m;
    m.update(make_manifest_stream(wide_timestamps)).get0();

    scan_ts_segments(m);
}

/**
 * Nothing internally to redpanda guarantees anything about the relationship
 * between timestamps on different segments.  In CreateTime, the timestamps
 * are totally arbitrary and don't even have to be in any kind of order:
 * one segment's base_timestamp might be before a previous segment's
 * base_timestamp.
 *
 * There are no particular rules about how redpanda has to handle totally
 * janky out of order timestamps, but simple overlaps should work fine.
 */
SEASTAR_THREAD_TEST_CASE(test_timequery_overlaps) {
    static constexpr std::string_view overlapping_timestamps = R"json({
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
                "committed_offset": 19,
                "base_timestamp": 123000,
                "max_timestamp": 123010
            },
            "20-1-v1.log": {
                "is_compacted": false,
                "size_bytes": 2048,
                "base_offset": 20,
                "committed_offset": 29,
                "base_timestamp": 123010,
                "max_timestamp": 123020
            },
            "30-1-v1.log": {
                "is_compacted": false,
                "size_bytes": 4096,
                "base_offset": 30,
                "committed_offset": 39,
                "base_timestamp": 923020,
                "max_timestamp": 923030
            },
            "40-1-v1.log": {
                "is_compacted": false,
                "size_bytes": 4096,
                "base_offset": 40,
                "committed_offset": 49,
                "base_timestamp": 923030,
                "max_timestamp": 923040
            }
        }
    })json";

    partition_manifest m;
    m.update(make_manifest_stream(overlapping_timestamps)).get0();

    scan_ts_segments_general(m);
}

/**
 * With entirely out of order timestamps, all bets are off, other than that
 * we should not e.g. crash under these circumstances, and that queries
 * outside the outer begin/end timestamps should return the first segment
 * and null respectively.
 */
SEASTAR_THREAD_TEST_CASE(test_timequery_out_of_order) {
    static constexpr std::string_view raw = R"json({
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
                "committed_offset": 19,
                "base_timestamp": 123000,
                "max_timestamp": 123010
            },
            "20-1-v1.log": {
                "is_compacted": false,
                "size_bytes": 2048,
                "base_offset": 20,
                "committed_offset": 29,
                "base_timestamp": 113000,
                "max_timestamp": 113020
            },
            "30-1-v1.log": {
                "is_compacted": false,
                "size_bytes": 4096,
                "base_offset": 30,
                "committed_offset": 39,
                "base_timestamp": 112000,
                "max_timestamp": 114000
            },
            "40-1-v1.log": {
                "is_compacted": false,
                "size_bytes": 4096,
                "base_offset": 40,
                "committed_offset": 49,
                "base_timestamp": 150000,
                "max_timestamp": 150333
            }
        }
    })json";

    partition_manifest m;
    m.update(make_manifest_stream(raw)).get0();

    // Before range: should get first segment
    expect_ts_segment(
      m.timequery(model::timestamp{m.begin()->second.base_timestamp() - 100}),
      m.begin()->second.base_offset);

    for (partition_manifest::const_iterator i = m.begin(); i != m.end(); ++i) {
        auto& s = i->second;
        // Not checking results: undefined for out of order data.
        m.timequery(s.base_timestamp);
        m.timequery(s.max_timestamp);
    }

    // After range: should get null
    BOOST_REQUIRE(
      m.timequery(model::timestamp{m.rbegin()->second.max_timestamp() + 1})
      == std::nullopt);
}

SEASTAR_THREAD_TEST_CASE(test_generate_segment_name_format) {
    static constexpr std::string_view raw = R"json({
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
                "committed_offset": 19,
                "base_timestamp": 1000,
                "max_timestamp":  1001,
                "archiver_term": 1
            },
            "20-1-v1.log": {
                "is_compacted": false,
                "size_bytes": 2048,
                "base_offset": 20,
                "committed_offset": 29,
                "base_timestamp": 1002,
                "max_timestamp":  1003,
                "delta_offset": 1,
                "delta_offset_end": 10,
                "archiver_term": 2,
                "segment_term": 1,
                "sname_format": 2
            },
            "30-1-v1.log": {
                "is_compacted": false,
                "size_bytes": 1024,
                "base_offset": 30,
                "committed_offset": 39,
                "base_timestamp": 1004,
                "max_timestamp":  1005
            },
            "40-2-v1.log": {
                "is_compacted": false,
                "size_bytes": 4096,
                "base_offset": 40,
                "committed_offset": 42,
                "base_timestamp": 1006,
                "max_timestamp":  1007,
                "delta_offset": 1,
                "segment_term": 2,
                "delta_offset_end": 10,
                "sname_format": 2
            }
        }
    })json";

    partition_manifest m;
    m.update(make_manifest_stream(raw)).get0();

    {
        // old format with archival term
        auto s = m.find(model::offset(10));
        auto expected = remote_segment_path(
          "9b367cb7/test-ns/test-topic/42_1/10-1-v1.log.1");
        auto actual1 = partition_manifest::generate_remote_segment_path(
          m.get_ntp(), s->first, s->second);
        auto actual2 = m.generate_segment_path(s->first, s->second);
        BOOST_REQUIRE_EQUAL(expected, actual1);
        BOOST_REQUIRE_EQUAL(expected, actual2);
    }

    {
        // new format with archival term
        auto s = m.find(model::offset(20));
        auto expected = remote_segment_path(
          "96c6b7a9/test-ns/test-topic/42_1/20-29-2048-1-v1.log.2");
        auto actual1 = partition_manifest::generate_remote_segment_path(
          m.get_ntp(), s->first, s->second);
        auto actual2 = m.generate_segment_path(s->first, s->second);
        BOOST_REQUIRE_EQUAL(expected, actual1);
        BOOST_REQUIRE_EQUAL(expected, actual2);
    }

    {
        // old format without archival term
        auto s = m.find(model::offset(30));
        auto expected = remote_segment_path(
          "df1262f5/test-ns/test-topic/42_1/30-1-v1.log");
        auto actual1 = partition_manifest::generate_remote_segment_path(
          m.get_ntp(), s->first, s->second);
        auto actual2 = m.generate_segment_path(s->first, s->second);
        BOOST_REQUIRE_EQUAL(expected, actual1);
        BOOST_REQUIRE_EQUAL(expected, actual2);
    }

    {
        // new format without archival term
        auto s = m.find(model::offset(40));
        auto expected = remote_segment_path(
          "e44e8104/test-ns/test-topic/42_1/40-42-4096-2-v1.log");
        auto actual1 = partition_manifest::generate_remote_segment_path(
          m.get_ntp(), s->first, s->second);
        auto actual2 = m.generate_segment_path(s->first, s->second);
        BOOST_REQUIRE_EQUAL(expected, actual1);
        BOOST_REQUIRE_EQUAL(expected, actual2);
    }
}
