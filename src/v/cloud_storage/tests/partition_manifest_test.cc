/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/segment_path_utils.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "utils/tracking_allocator.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/context.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

using namespace cloud_storage;

namespace {
const remote_path_provider path_provider(std::nullopt, std::nullopt);
} // anonymous namespace

static constexpr std::string_view empty_manifest_json = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 0,
    "insync_offset": 0,
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
        "40-11-v1.log": {
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
        },
        "50-11-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 50,
            "committed_offset": 59,
            "base_timestamp": 123040,
            "max_timestamp": 123049,
            "delta_offset": 9,
            "archiver_term": 42,
            "segment_term": 11,
            "delta_offset_end": 10,
            "sname_format": 3,
            "metadata_size_hint": 321
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

namespace {

ss::logger test_log("partition_manifest_test");

// Returns a manifest with segments with the corresponding offset bounds, with
// the last offsets in the list referring to the exclusive end of the returned
// manifest.
partition_manifest
manifest_for(std::vector<std::pair<model::offset, kafka::offset>> o) {
    partition_manifest manifest;
    manifest
      .update(
        cloud_storage::manifest_format::json,
        make_manifest_stream(empty_manifest_json))
      .get();
    for (size_t i = 0; i < o.size() - 1; i++) {
        segment_meta seg{
          .is_compacted = false,
          .size_bytes = 1024,
          .base_offset = o[i].first,
          .committed_offset = model::offset(o[i + 1].first() - 1),
          .delta_offset = model::offset_delta(o[i].first() - o[i].second()),
          .delta_offset_end = model::offset_delta(
            o[i + 1].first() - o[i + 1].second()),
        };
        manifest.add(
          segment_name(fmt::format("{}-1-v1.log", o[i].first())), seg);
    }
    return manifest;
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_segment_contains_by_kafka_offset) {
    // Returns true if a kafka::offset lookup returns the segment with the
    // expected base offsets.
    const auto check_offset = [](
                                const partition_manifest& m,
                                kafka::offset ko,
                                model::offset expected_base_mo,
                                kafka::offset expected_base_ko) {
        const auto it = m.segment_containing(ko);
        bool success = it != m.end()
                       // Check that the segment base offsets match the segment
                       // we were expecting...
                       && it->base_offset == expected_base_mo
                       && it->base_kafka_offset() == expected_base_ko
                       // ...and as a sanity check, make sure the kafka::offset
                       // falls in the segment.
                       && it->base_kafka_offset() <= ko
                       && it->next_kafka_offset() > ko;
        if (success) {
            return true;
        }
        if (it == m.end()) {
            test_log.error("no segment for {}", ko);
            return false;
        }
        test_log.error(
          "segment {} doesn't match the expected base model::offset {} or "
          "kafka::offset {}, or doesn't include "
          "{} in its range",
          *it,
          expected_base_mo,
          expected_base_ko,
          ko);
        return false;
    };

    // Returns true if a kafka::offset lookup returns that no such segment is
    // returned.
    const auto check_no_offset = [](
                                   const partition_manifest& m,
                                   kafka::offset ko) {
        const auto it = m.segment_containing(ko);
        bool success = it == m.end();
        if (success) {
            return true;
        }
        test_log.error("unexpected segment for kafka::offset {}: {}", ko, *it);
        return false;
    };

    // mo: 0      10     20     30
    //     [a    ][b    ][c    ]end
    // ko: 0      5      10     15
    partition_manifest full_manifest = manifest_for({
      {model::offset(0), kafka::offset(0)},
      {model::offset(10), kafka::offset(5)},
      {model::offset(20), kafka::offset(10)},
      {model::offset(30), kafka::offset(15)},
    });
    BOOST_REQUIRE(check_offset(
      full_manifest, kafka::offset(0), model::offset(0), kafka::offset(0)));
    BOOST_REQUIRE(check_offset(
      full_manifest, kafka::offset(4), model::offset(0), kafka::offset(0)));
    BOOST_REQUIRE(check_offset(
      full_manifest, kafka::offset(5), model::offset(10), kafka::offset(5)));
    BOOST_REQUIRE(check_offset(
      full_manifest, kafka::offset(9), model::offset(10), kafka::offset(5)));
    BOOST_REQUIRE(check_offset(
      full_manifest, kafka::offset(10), model::offset(20), kafka::offset(10)));
    BOOST_REQUIRE(check_offset(
      full_manifest, kafka::offset(14), model::offset(20), kafka::offset(10)));
    BOOST_REQUIRE(check_no_offset(full_manifest, kafka::offset(15)));

    // mo: 10     20     30
    //     [b    ][c    ]end
    // ko: 5      10     15
    partition_manifest truncated_manifest = manifest_for({
      {model::offset(10), kafka::offset(5)},
      {model::offset(20), kafka::offset(10)},
      {model::offset(30), kafka::offset(15)},
    });
    BOOST_REQUIRE(check_no_offset(truncated_manifest, kafka::offset(0)));
    BOOST_REQUIRE(check_no_offset(truncated_manifest, kafka::offset(4)));
    BOOST_REQUIRE(check_offset(
      truncated_manifest,
      kafka::offset(5),
      model::offset(10),
      kafka::offset(5)));
    BOOST_REQUIRE(check_offset(
      truncated_manifest,
      kafka::offset(9),
      model::offset(10),
      kafka::offset(5)));
    BOOST_REQUIRE(check_offset(
      truncated_manifest,
      kafka::offset(10),
      model::offset(20),
      kafka::offset(10)));
    BOOST_REQUIRE(check_offset(
      truncated_manifest,
      kafka::offset(14),
      model::offset(20),
      kafka::offset(10)));
    BOOST_REQUIRE(check_no_offset(full_manifest, kafka::offset(15)));
}

SEASTAR_THREAD_TEST_CASE(test_segment_contains) {
    partition_manifest m;
    m.update(manifest_format::json, make_manifest_stream(manifest_with_gaps))
      .get();
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
      m.segment_containing(model::offset{50})
      == std::next(m.begin(), m.size() - 1));
    BOOST_REQUIRE(
      m.segment_containing(model::offset{52})
      == std::next(m.begin(), m.size() - 1));
    BOOST_REQUIRE(
      m.segment_containing(model::offset{59})
      == std::next(m.begin(), m.size() - 1));
}

// Test for the partition manifest tracked memory.
SEASTAR_THREAD_TEST_CASE(test_manifest_mem_tracking) {
    partition_manifest empty_manifest;
    empty_manifest
      .update(manifest_format::json, make_manifest_stream(empty_manifest_json))
      .get();
}

SEASTAR_THREAD_TEST_CASE(test_manifest_type) {
    partition_manifest m;
    BOOST_REQUIRE(m.get_manifest_type() == manifest_type::partition);
}

SEASTAR_THREAD_TEST_CASE(test_segment_path) {
    auto path = prefixed_segment_path(
      manifest_ntp,
      model::initial_revision_id(0),
      segment_name("22-11-v1.log"),
      model::term_id{123});
    // use pre-calculated murmur hash value from full ntp path + file name
    BOOST_REQUIRE_EQUAL(
      path, "2bea9275/test-ns/test-topic/42_0/22-11-v1.log.123");
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
    auto path = m.get_manifest_path(path_provider);
    BOOST_REQUIRE_EQUAL(
      path, "20000000/meta/test-ns/test-topic/42_0/manifest.bin");
}

SEASTAR_THREAD_TEST_CASE(test_empty_manifest_update) {
    partition_manifest m;
    m.update(manifest_format::json, make_manifest_stream(empty_manifest_json))
      .get();
    auto path = m.get_manifest_path(path_provider);
    BOOST_REQUIRE_EQUAL(
      path, "20000000/meta/test-ns/test-topic/42_0/manifest.bin");
}

void require_equal_segment_meta(
  partition_manifest::segment_meta expected,
  partition_manifest::segment_meta actual) {
    BOOST_CHECK_EQUAL(expected.is_compacted, actual.is_compacted);
    BOOST_CHECK_EQUAL(expected.size_bytes, actual.size_bytes);
    BOOST_CHECK_EQUAL(expected.base_offset, actual.base_offset);
    BOOST_CHECK_EQUAL(expected.committed_offset, actual.committed_offset);
    BOOST_CHECK_EQUAL(expected.base_timestamp, actual.base_timestamp);
    BOOST_CHECK_EQUAL(expected.max_timestamp, actual.max_timestamp);
    BOOST_CHECK_EQUAL(expected.delta_offset, actual.delta_offset);
    BOOST_CHECK_EQUAL(expected.ntp_revision, actual.ntp_revision);
    BOOST_CHECK_EQUAL(expected.archiver_term, actual.archiver_term);
    BOOST_CHECK_EQUAL(expected.segment_term, actual.segment_term);
    BOOST_CHECK_EQUAL(expected.delta_offset_end, actual.delta_offset_end);
    BOOST_CHECK_EQUAL(expected.sname_format, actual.sname_format);
}

SEASTAR_THREAD_TEST_CASE(test_complete_manifest_update) {
    partition_manifest m;
    m.update(
       manifest_format::json, make_manifest_stream(complete_manifest_json))
      .get();
    auto path = m.get_manifest_path(path_provider);
    BOOST_REQUIRE_EQUAL(
      path, "60000000/meta/test-ns/test-topic/42_1/manifest.bin");
    BOOST_REQUIRE_EQUAL(m.size(), 5);
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
           1), // revision is propagated from manifest
         .segment_term = model::term_id(
           1), // segment_term is propagated from manifest
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
           1), // revision is propagated from manifest
         .segment_term = model::term_id(
           1), // segment_term is propagated from manifest
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
         .segment_term = model::term_id(
           1), // segment_term is propagated from manifest
       }},
      {"40-11-v1.log",
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
      {"50-11-v1.log",
       partition_manifest::segment_meta{
         .is_compacted = false,
         .size_bytes = 4096,
         .base_offset = model::offset(50),
         .committed_offset = model::offset(59),
         .base_timestamp = model::timestamp(123040),
         .max_timestamp = model::timestamp(123049),
         .delta_offset = model::offset_delta(9),
         .ntp_revision = model::initial_revision_id(
           1), // revision is propagated from manifest
         .archiver_term = model::term_id{42},
         .segment_term = model::term_id{11},
         .delta_offset_end = model::offset_delta(10),
         .sname_format = segment_name_format::v3,
         .metadata_size_hint = 321,
       }},
    };
    for (const auto& actual : m) {
        auto sn = generate_local_segment_name(
          actual.base_offset, actual.segment_term);
        auto it = expected.find(sn());
        BOOST_REQUIRE(it != expected.end());
        require_equal_segment_meta(it->second, actual);
    }
}

SEASTAR_THREAD_TEST_CASE(test_max_segment_meta_update) {
    partition_manifest m;
    m.update(
       manifest_format::json,
       make_manifest_stream(max_segment_meta_manifest_json))
      .get();
    auto path = m.get_manifest_path(path_provider);
    BOOST_REQUIRE_EQUAL(
      path, "60000000/meta/test-ns/test-topic/42_1/manifest.bin");
    BOOST_REQUIRE_EQUAL(m.size(), 1);
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
         .archiver_term = model::term_id(3),
         .segment_term = model::term_id(1), // set by the manifest
       }}};

    for (const auto& actual : m) {
        auto sn = generate_local_segment_name(
          actual.base_offset, actual.segment_term);
        auto it = expected.find(sn());
        BOOST_REQUIRE(it != expected.end());
        require_equal_segment_meta(it->second, actual);
    }
}

SEASTAR_THREAD_TEST_CASE(test_no_size_bytes_segment_meta) {
    partition_manifest m;
    BOOST_REQUIRE_EXCEPTION(
      m.update(
         manifest_format::json,
         make_manifest_stream(no_size_bytes_segment_meta))
        .get(),
      std::runtime_error,
      [](std::runtime_error ex) {
          return std::string(ex.what()).find(
                   "Missing size_bytes value in 10-1-v1.log segment meta")
                 != std::string::npos;
      });
}

/**
 * Segment metadata with missing timestamps are default initialized
 */
SEASTAR_THREAD_TEST_CASE(test_timestamps_segment_meta) {
    partition_manifest m;
    m.update(
       manifest_format::json, make_manifest_stream(no_timestamps_segment_meta))
      .get();
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
    m.update(
       manifest_format::json,
       make_manifest_stream(segment_meta_gets_smaller_manifest_json))
      .get();
    auto path = m.get_manifest_path(path_provider);
    BOOST_REQUIRE_EQUAL(
      path, "60000000/meta/test-ns/test-topic/42_1/manifest.bin");
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
         .archiver_term = model::term_id(3),
         .segment_term = model::term_id(1)}},
      {"20-1-v1.log",
       partition_manifest::segment_meta{
         .is_compacted = false,
         .size_bytes = 2048,
         .base_offset = model::offset(20),
         .committed_offset = model::offset(29),
         .ntp_revision = model::initial_revision_id(
           1), // if ntp_revision if missing in meta, we get revision from
               // manifest
         .segment_term = model::term_id(1), // set by the manifest

       }},
    };
    for (const auto& actual : m) {
        auto sn = generate_local_segment_name(
          actual.base_offset, actual.segment_term);
        auto it = expected.find(sn());
        BOOST_REQUIRE(it != expected.end());
        require_equal_segment_meta(it->second, actual);
    }
}

SEASTAR_THREAD_TEST_CASE(test_no_closing_bracket_meta) {
    partition_manifest m;
    BOOST_REQUIRE_EXCEPTION(
      m.update(
         manifest_format::json,
         make_manifest_stream(no_closing_bracket_segment_meta))
        .get(),
      std::runtime_error,
      [](std::runtime_error ex) {
          return std::string(ex.what()).find(
                   "Failed to parse partition manifest "
                   "b0000000/meta///-2147483648_-9223372036854775808/"
                   "manifest.json: Missing a comma or '}' after an object "
                   "member. at offset 325")
                 != std::string::npos;
      });
}

SEASTAR_THREAD_TEST_CASE(test_fields_after_segments) {
    partition_manifest m;
    m.update(
       manifest_format::json, make_manifest_stream(fields_after_segments_json))
      .get();
    auto path = m.get_manifest_path(path_provider);
    BOOST_REQUIRE_EQUAL(
      path, "60000000/meta/test-ns/test-topic/42_1/manifest.bin");
    BOOST_REQUIRE_EQUAL(m.size(), 1);
    std::map<ss::sstring, partition_manifest::segment_meta> expected = {
      {"10-1-v1.log",
       partition_manifest::segment_meta{
         .is_compacted = false,
         .size_bytes = 1024,
         .base_offset = model::offset(10),
         .committed_offset = model::offset(19),
         .segment_term = model::term_id(1)}}};

    for (const auto& actual : m) {
        auto sn = generate_local_segment_name(
          actual.base_offset, actual.segment_term);
        auto it = expected.find(sn());
        BOOST_REQUIRE(it != expected.end());
        require_equal_segment_meta(it->second, actual);
    }
}

SEASTAR_THREAD_TEST_CASE(test_missing_manifest_field) {
    partition_manifest m;
    BOOST_REQUIRE_EXCEPTION(
      m.update(
         manifest_format::json,
         make_manifest_stream(missing_last_offset_manifest_json))
        .get(),
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
        .segment_term = model::term_id(1),
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
        .segment_term = model::term_id(1),
      });
    auto [is, size] = m.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    partition_manifest restored;
    restored.update(std::move(rstr)).get();

    BOOST_REQUIRE(m == restored);

    BOOST_REQUIRE(m.to_iobuf() == restored.to_iobuf());

    m.from_iobuf(m.to_iobuf());
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
       .sname_format = segment_name_format::v1});
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
        BOOST_REQUIRE(res[0].base_offset == model::offset{20});
        BOOST_REQUIRE(res[0].committed_offset == model::offset{29});
    }
    // Replace several segments
    m.add(
      segment_name("0-1-v1.log"),
      {.base_offset = model::offset{0},
       .committed_offset = model::offset{29},
       .sname_format = segment_name_format::v2});
    {
        auto res = m.replaced_segments();
        std::sort(res.begin(), res.end());
        BOOST_REQUIRE(res.size() == 4);
        BOOST_REQUIRE(res[0].base_offset == model::offset{0});
        BOOST_REQUIRE(res[0].committed_offset == model::offset{9});
        BOOST_REQUIRE(res[1].base_offset == model::offset{10});
        BOOST_REQUIRE(res[1].committed_offset == model::offset{19});
        BOOST_REQUIRE(res[2].base_offset == model::offset{20});
        BOOST_REQUIRE(res[2].committed_offset == model::offset{29});
        // The segment with base offset 20 was replaced twice
        BOOST_REQUIRE(res[3].base_offset == model::offset{20});
        BOOST_REQUIRE(res[3].committed_offset == model::offset{29});
    }
}

SEASTAR_THREAD_TEST_CASE(test_replaced_sname_format_version) {
    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    m.add(
      segment_name("0-1-v1.log"),
      {
        .base_offset = model::offset{0},
        .committed_offset = model::offset{9},
      });

    m.add(
      segment_name("10-1-v1.log"),
      {
        .size_bytes = 0,
        .base_offset = model::offset{10},
        .committed_offset = model::offset{19},
      });
    m.add(
      segment_name("20-1-v1.log"),
      {
        .size_bytes = 0,
        .base_offset = model::offset{20},
        .committed_offset = model::offset{29},
      });
    m.add(
      segment_name("30-1-v1.log"),
      {
        .size_bytes = 0,
        .base_offset = model::offset{30},
        .committed_offset = model::offset{39},
        .sname_format = segment_name_format::v2,
      });
    m.add(
      segment_name("40-1-v1.log"),
      {
        .size_bytes = 0,
        .base_offset = model::offset{40},
        .committed_offset = model::offset{49},
        .sname_format = segment_name_format::v3,
      });

    // replacement
    m.add(
      segment_name("10-1-v1.log"),
      {
        .size_bytes = 0,
        .base_offset = model::offset{10},
        .committed_offset = model::offset{19},
        .segment_term = model::term_id{3},
      });
    m.add(
      segment_name("20-1-v1.log"),
      {
        .size_bytes = 1,
        .base_offset = model::offset{20},
        .committed_offset = model::offset{29},
        .segment_term = model::term_id{3},
      });
    m.add(
      segment_name("30-1-v1.log"),
      {
        .size_bytes = 0,
        .base_offset = model::offset{30},
        .committed_offset = model::offset{39},
        .segment_term = model::term_id{3},
        .sname_format = segment_name_format::v2,
      });
    m.add(
      segment_name("40-1-v1.log"),
      {
        .size_bytes = 0,
        .base_offset = model::offset{40},
        .committed_offset = model::offset{49},
        .segment_term = model::term_id{3},
        .sname_format = segment_name_format::v3,
      });

    std::stringstream sstr;
    m.serialize_json(sstr);

    vlog(test_log.info, "serialized: {}", sstr.str());

    partition_manifest m2;
    m2.update(manifest_format::json, make_manifest_stream(sstr.str())).get();
    auto replaced = m2.replaced_segments();
    BOOST_REQUIRE_EQUAL(replaced.size(), 4);
    // size 0, inferred as v1
    BOOST_REQUIRE_EQUAL(replaced[0].sname_format, segment_name_format::v1);
    BOOST_REQUIRE_EQUAL(replaced[1].sname_format, segment_name_format::v1);
    // size 0, explicitly set to v2
    BOOST_REQUIRE_EQUAL(replaced[2].sname_format, segment_name_format::v2);
    // size 0, explicitly set to v3
    BOOST_REQUIRE_EQUAL(replaced[3].sname_format, segment_name_format::v3);
}

namespace cloud_storage {

struct partition_manifest_accessor {
    static void add_replaced_segment(
      partition_manifest* m,
      const segment_name&,
      const partition_manifest::segment_meta& meta) {
        m->_replaced.push_back(
          partition_manifest::lw_segment_meta::convert(meta));
    }
    static auto find(segment_name n, const partition_manifest& m) {
        auto key = parse_segment_name(n);
        for (auto it = m._replaced.begin(); it != m._replaced.end(); it++) {
            if (
              key
              == segment_name_components{it->base_offset, it->segment_term}) {
                return std::make_pair(it, m._replaced.end());
            }
        }
        return std::make_pair(m._replaced.end(), m._replaced.end());
    }
};
} // namespace cloud_storage

SEASTAR_THREAD_TEST_CASE(test_complete_manifest_serialization_roundtrip) {
    using accessor = cloud_storage::partition_manifest_accessor;
    std::map<ss::sstring, partition_manifest::segment_meta> expected_segments
      = {
        // v0 segments
        {"0-1-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 1024,
           .base_offset = model::offset(0),
           .committed_offset = model::offset(99),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(1),
         }},
        {"100-1-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 2048,
           .base_offset = model::offset(100),
           .committed_offset = model::offset(199),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(1),
         }},
        // v1 segments
        {"200-2-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 4096,
           .base_offset = model::offset(200),
           .committed_offset = model::offset(299),
           .max_timestamp = model::timestamp(1234567890),
           .delta_offset = model::offset_delta(1),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(2),
         }},
        // v2 segments
        {"300-3-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 4096,
           .base_offset = model::offset(300),
           .committed_offset = model::offset(399),
           .max_timestamp = model::timestamp(1234567890),
           .delta_offset = model::offset_delta(2),
           .ntp_revision = model::initial_revision_id(1),
           .archiver_term = model::term_id{42},
           .segment_term = model::term_id{3},
           .delta_offset_end = model::offset_delta(9),
           .sname_format = segment_name_format::v2,
         }},
        {"400-3-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 4096,
           .base_offset = model::offset(400),
           .committed_offset = model::offset(499),
           .max_timestamp = model::timestamp(1234567890),
           .delta_offset = model::offset_delta(2),
           .ntp_revision = model::initial_revision_id(1),
           .archiver_term = model::term_id{42},
           .segment_term = model::term_id{3},
           .delta_offset_end = model::offset_delta(9),
           .sname_format = segment_name_format::v2,
         }},
        {"500-3-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 4096,
           .base_offset = model::offset(500),
           .committed_offset = model::offset(599),
           .max_timestamp = model::timestamp(1234567890),
           .delta_offset = model::offset_delta(2),
           .ntp_revision = model::initial_revision_id(1),
           .archiver_term = model::term_id{42},
           .segment_term = model::term_id{3},
           .delta_offset_end = model::offset_delta(9),
           .sname_format = segment_name_format::v3,
         }},
        {"600-3-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 4096,
           .base_offset = model::offset(600),
           .committed_offset = model::offset(699),
           .max_timestamp = model::timestamp(1234567890),
           .delta_offset = model::offset_delta(2),
           .ntp_revision = model::initial_revision_id(1),
           .archiver_term = model::term_id{42},
           .segment_term = model::term_id{3},
           .delta_offset_end = model::offset_delta(9),
           .sname_format = segment_name_format::v3,
         }},
      };
    std::multimap<ss::sstring, partition_manifest::segment_meta>
      expected_replaced_segments = {
        // v0 segments
        {"0-1-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 1024,
           .base_offset = model::offset(0),
           .committed_offset = model::offset(9),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(1),
         }},
        {"0-1-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 2048,
           .base_offset = model::offset(0),
           .committed_offset = model::offset(59),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(1)}},
        // v1 segments
        {"60-2-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 4096,
           .base_offset = model::offset(60),
           .committed_offset = model::offset(79),
           .max_timestamp = model::timestamp(1234567890),
           .delta_offset = model::offset_delta(1),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(2),
         }},
        {"60-2-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 4096,
           .base_offset = model::offset(60),
           .committed_offset = model::offset(199),
           .max_timestamp = model::timestamp(1234567890),
           .delta_offset = model::offset_delta(1),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(2),
         }},
        // v2 segments
        {"200-3-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 4096,
           .base_offset = model::offset(200),
           .committed_offset = model::offset(279),
           .max_timestamp = model::timestamp(1234567890),
           .delta_offset = model::offset_delta(2),
           .ntp_revision = model::initial_revision_id(1),
           .archiver_term = model::term_id{41},
           .segment_term = model::term_id{3},
           .delta_offset_end = model::offset_delta(9),
           .sname_format = segment_name_format::v2,
         }},
        {"200-3-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 4096,
           .base_offset = model::offset(200),
           .committed_offset = model::offset(389),
           .max_timestamp = model::timestamp(1234567890),
           .delta_offset = model::offset_delta(2),
           .ntp_revision = model::initial_revision_id(1),
           .archiver_term = model::term_id{41},
           .segment_term = model::term_id{3},
           .delta_offset_end = model::offset_delta(9),
           .sname_format = segment_name_format::v2,
         }},
      };

    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    for (const auto& segment : expected_segments) {
        m.add(segment_name(segment.first), segment.second);
    }

    for (const auto& segment : expected_replaced_segments) {
        accessor::add_replaced_segment(
          &m, segment_name(segment.first), segment.second);
    }
    spillover_manifest spm(manifest_ntp, m.get_revision_id());
    model::offset spillover_offset{200};
    for (auto [_, segment_meta] : expected_segments) {
        if (segment_meta.base_offset >= spillover_offset) {
            break;
        }
        spm.add(segment_meta);
    }
    auto spillover_manifest_meta = spm.make_manifest_metadata();
    m.spillover(spillover_manifest_meta);

    auto run_checks = [&](partition_manifest restored, std::string test_info) {
        BOOST_TEST_INFO(test_info);
        BOOST_REQUIRE(restored == m);

        for (const auto& [name, segment_meta] : expected_segments) {
            if (segment_meta.base_offset < spillover_offset) {
                auto spm = restored.get_spillover_map().find(
                  segment_meta.base_offset);
                if (spm != restored.get_spillover_map().end()) {
                    require_equal_segment_meta(*spm, spillover_manifest_meta);
                }
                continue;
            }

            auto actual = restored.find(segment_meta.base_offset);
            BOOST_REQUIRE(actual != restored.end());
            require_equal_segment_meta(segment_meta, *actual);
        }

        for (const auto& expected : expected_replaced_segments) {
            auto actual = accessor::find(
              segment_name(expected.first), restored);
            auto res = std::any_of(
              actual.first, actual.second, [&expected](const auto& a) {
                  return partition_manifest::lw_segment_meta::convert(
                           expected.second)
                         == a;
              });
            BOOST_REQUIRE(res);
        }
    };

    run_checks(
      [&] {
          auto [is, size] = m.serialize().get();
          iobuf buf;
          auto os = make_iobuf_ref_output_stream(buf);
          ss::copy(is, os).get();

          auto rstr = make_iobuf_input_stream(std::move(buf));
          partition_manifest restored;
          restored.update(std::move(rstr)).get();
          return restored;
      }(),
      "serialize/deserialize");

    run_checks(
      [&] {
          partition_manifest restored{};
          restored.from_iobuf(m.to_iobuf());
          return restored;
      }(),
      "to_iobuf/from_iobuf");
    run_checks(
      [&] {
          partition_manifest restored{};
          iobuf buf;
          auto os = make_iobuf_ref_output_stream(buf);
          m.serialize_json(os).get();
          os.flush().get();
          restored.update_with_json(std::move(buf));
          return restored;
      }(),
      "to_json/from_json");
}

SEASTAR_THREAD_TEST_CASE(test_partition_manifest_start_offset_advance) {
    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    BOOST_REQUIRE(m.get_start_offset() == std::nullopt);
    m.add(
      segment_name("0-1-v1.log"),
      partition_manifest::segment_meta{
        .base_offset = model::offset{0},
        .committed_offset = model::offset{100},
      });
    BOOST_REQUIRE(m.get_start_offset() == model::offset(0));
    m.add(
      segment_name("101-1-v1.log"),
      partition_manifest::segment_meta{
        .base_offset = model::offset{101},
        .committed_offset = model::offset{200},
      });
    BOOST_REQUIRE(m.get_start_offset() == model::offset(0));
    m.add(
      segment_name("201-2-v1.log"),
      partition_manifest::segment_meta{
        .base_offset = model::offset{201},
        .committed_offset = model::offset{300},
      });
    m.add(
      segment_name("301-3-v1.log"),
      partition_manifest::segment_meta{
        .base_offset = model::offset{301},
        .committed_offset = model::offset{400},
      });
    BOOST_REQUIRE(m.advance_start_offset(model::offset(100)));
    BOOST_REQUIRE(m.get_start_offset() == model::offset(0));
    BOOST_REQUIRE(m.advance_start_offset(model::offset(101)));
    BOOST_REQUIRE(m.get_start_offset() == model::offset(101));
    BOOST_REQUIRE(m.advance_start_offset(model::offset(200)));
    BOOST_REQUIRE(m.get_start_offset() == model::offset(101));
    BOOST_REQUIRE(m.advance_start_offset(model::offset(201)));
    BOOST_REQUIRE(m.get_start_offset() == model::offset(201));
    BOOST_REQUIRE(m.advance_start_offset(model::offset(300)));
    BOOST_REQUIRE(m.get_start_offset() == model::offset(201));
    BOOST_REQUIRE(m.advance_start_offset(model::offset(301)));
    BOOST_REQUIRE(m.get_start_offset() == model::offset(301));
    auto m2 = m.truncate();
    BOOST_REQUIRE(m2.size() == 3);
    BOOST_REQUIRE(m.size() == 1);

    auto m3 = m.truncate(model::offset(401));
    BOOST_REQUIRE(m3.size() == 1);
    BOOST_REQUIRE(m.size() == 0);
    BOOST_REQUIRE(m.get_start_offset() == std::nullopt);
}

SEASTAR_THREAD_TEST_CASE(test_partition_manifest_start_kafka_offset_advance) {
    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    BOOST_REQUIRE(m.get_start_kafka_offset() == std::nullopt);
    m.add(
      segment_name("0-1-v1.log"),
      partition_manifest::segment_meta{
        .base_offset = model::offset{0},
        .committed_offset = model::offset{99},
        .delta_offset = model::offset_delta{0},
      });
    BOOST_REQUIRE(*m.get_start_kafka_offset() == kafka::offset(0));
    m.add(
      segment_name("100-1-v1.log"),
      partition_manifest::segment_meta{
        .base_offset = model::offset{100}, // kafka offset 90
        .committed_offset = model::offset{199},
        .delta_offset = model::offset_delta{10},
      });
    m.add(
      segment_name("200-2-v1.log"),
      partition_manifest::segment_meta{
        .base_offset = model::offset{200}, // kafka offset 180
        .committed_offset = model::offset{299},
        .delta_offset = model::offset_delta{20},
      });
    m.add(
      segment_name("300-3-v1.log"),
      partition_manifest::segment_meta{
        .base_offset = model::offset{300}, // kafka offset 270
        .committed_offset = model::offset{399},
        .delta_offset = model::offset_delta{30},
      });

    BOOST_REQUIRE(m.advance_start_kafka_offset(kafka::offset(80)));
    BOOST_REQUIRE_EQUAL(m.get_start_kafka_offset_override(), kafka::offset(80));
    BOOST_REQUIRE_EQUAL(m.get_start_kafka_offset(), kafka::offset(0));
    BOOST_REQUIRE_EQUAL(m.get_start_offset(), model::offset(0));

    // Moving the start offset ahead of the kafka override removes the
    // override.
    BOOST_REQUIRE(m.advance_start_offset(model::offset(100)));
    BOOST_REQUIRE_EQUAL(m.get_start_kafka_offset_override(), kafka::offset{});
    BOOST_REQUIRE_EQUAL(m.get_start_kafka_offset(), kafka::offset(90));
    BOOST_REQUIRE_EQUAL(m.get_start_offset(), model::offset(100));

    BOOST_REQUIRE(m.advance_start_kafka_offset(kafka::offset(180)));
    BOOST_REQUIRE_EQUAL(
      m.get_start_kafka_offset_override(), kafka::offset(180));
    BOOST_REQUIRE_EQUAL(m.get_start_offset(), model::offset(100));
    BOOST_REQUIRE_EQUAL(m.get_start_kafka_offset(), kafka::offset(90));

    BOOST_REQUIRE(m.advance_start_kafka_offset(kafka::offset(200)));
    BOOST_REQUIRE_EQUAL(
      m.get_start_kafka_offset_override(), kafka::offset(200));
    BOOST_REQUIRE_EQUAL(m.get_start_offset(), model::offset(100));
    BOOST_REQUIRE_EQUAL(m.get_start_kafka_offset(), kafka::offset(90));

    BOOST_REQUIRE(m.advance_start_kafka_offset(kafka::offset(370)));
    BOOST_REQUIRE_EQUAL(
      m.get_start_kafka_offset_override(), kafka::offset(370));
    BOOST_REQUIRE_EQUAL(m.get_start_offset(), model::offset(100));
    BOOST_REQUIRE_EQUAL(m.get_start_kafka_offset(), kafka::offset(90));
    // Allow update with the same value
    BOOST_REQUIRE(m.advance_start_kafka_offset(kafka::offset(370)));
    // If trying to move back, it should no-op.
    BOOST_REQUIRE(!m.advance_start_kafka_offset(kafka::offset(369)));
    BOOST_REQUIRE_EQUAL(
      m.get_start_kafka_offset_override(), kafka::offset(370));
    BOOST_REQUIRE_EQUAL(m.get_start_offset(), model::offset(100));
    BOOST_REQUIRE_EQUAL(m.get_start_kafka_offset(), kafka::offset(90));

    // Truncating should clean up any out-of-bounds segments, but shouldn't
    // affected the override.
    BOOST_REQUIRE_EQUAL(4, m.size());
    m.truncate();
    BOOST_REQUIRE_EQUAL(3, m.size());
    BOOST_REQUIRE_EQUAL(
      m.get_start_kafka_offset_override(), kafka::offset(370));

    // When we truncate at a specific offset, it shouldn't take into account
    // the override (callers may, but the manifest itself shouldn't).
    m.truncate(model::offset(200));
    BOOST_REQUIRE_EQUAL(2, m.size());
    BOOST_REQUIRE_EQUAL(
      m.get_start_kafka_offset_override(), kafka::offset(370));
    BOOST_REQUIRE_EQUAL(m.get_start_offset(), model::offset(200));
    BOOST_REQUIRE_EQUAL(m.get_start_kafka_offset(), kafka::offset(180));

    // When we truncate past the override, the override is removed.
    auto m2 = m.truncate(model::offset(400));
    BOOST_REQUIRE_EQUAL(2, m2.size());
    BOOST_REQUIRE_EQUAL(0, m.size());
    BOOST_REQUIRE(m.get_start_offset() == std::nullopt);
    BOOST_REQUIRE(m.get_start_kafka_offset() == std::nullopt);
    BOOST_REQUIRE_EQUAL(m.get_start_kafka_offset_override(), kafka::offset{});
}

SEASTAR_THREAD_TEST_CASE(
  test_partition_manifest_start_offset_advance_with_gap) {
    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    BOOST_REQUIRE(m.get_start_offset() == std::nullopt);
    m.add(
      segment_name("10-1-v1.log"),
      partition_manifest::segment_meta{
        .base_offset = model::offset{10},
        .committed_offset = model::offset{100},
      });
    BOOST_REQUIRE(m.get_start_offset() == model::offset(10));
    m.add(
      segment_name("200-2-v1.log"),
      partition_manifest::segment_meta{
        .base_offset = model::offset{200},
        .committed_offset = model::offset{300},
      });
    BOOST_REQUIRE(!m.advance_start_offset(model::offset(0)));
    BOOST_REQUIRE_EQUAL(m.get_start_offset().value(), model::offset(10));
    BOOST_REQUIRE(m.advance_start_offset(model::offset(100)));
    BOOST_REQUIRE_EQUAL(m.get_start_offset().value(), model::offset(10));
    BOOST_REQUIRE(m.advance_start_offset(model::offset(150)));
    BOOST_REQUIRE_EQUAL(m.get_start_offset().value(), model::offset(200));
}

SEASTAR_THREAD_TEST_CASE(
  test_complete_manifest_serialization_roundtrip_with_start_offset) {
    std::map<ss::sstring, partition_manifest::segment_meta> expected_segments
      = {
        {"0-1-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 1024,
           .base_offset = model::offset(0),
           .committed_offset = model::offset(99),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(1),
         }},
        {"100-1-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 2048,
           .base_offset = model::offset(100),
           .committed_offset = model::offset(199),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(1),
         }},
        {"200-2-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 4096,
           .base_offset = model::offset(200),
           .committed_offset = model::offset(299),
           .max_timestamp = model::timestamp(1234567890),
           .delta_offset = model::offset_delta(1),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(2),
         }},
        {"300-2-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 4096,
           .base_offset = model::offset(300),
           .committed_offset = model::offset(399),
           .max_timestamp = model::timestamp(1234567890),
           .delta_offset = model::offset_delta(2),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(2),
         }},
      };

    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    for (const auto& segment : expected_segments) {
        m.add(segment_name(segment.first), segment.second);
    }
    m.advance_start_offset(model::offset(100));

    auto [is, size] = m.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    partition_manifest restored;
    restored.update(std::move(rstr)).get();

    BOOST_REQUIRE(restored == m);

    for (const auto& expected : expected_segments) {
        auto actual = restored.find(expected.second.base_offset);
        require_equal_segment_meta(expected.second, *actual);
    }

    BOOST_REQUIRE(restored.get_start_offset() == model::offset(100));
}

// modeled after cluster::archival_metadata_stm::segment
struct metadata_stm_segment
  : public serde::envelope<
      metadata_stm_segment,
      serde::version<0>,
      serde::compat_version<0>> {
    cloud_storage::segment_name name;
    cloud_storage::partition_manifest::segment_meta meta;

    auto serde_fields() { return std::tie(name, meta); }

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

    auto serde_fields() {
        return std::tie(
          is_compacted,
          size_bytes,
          base_offset,
          committed_offset,
          base_timestamp,
          max_timestamp,
          delta_offset);
    }

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

    auto serde_fields() {
        return std::tie(
          is_compacted,
          size_bytes,
          base_offset,
          committed_offset,
          base_timestamp,
          max_timestamp,
          delta_offset,
          ntp_revision,
          archiver_term);
    }

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

    auto serde_fields() { return std::tie(name, meta); }

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
        auto base_timestamp = segment_iter->base_timestamp;
        auto max_timestamp = segment_iter->max_timestamp;
        if (base_timestamp > t || max_timestamp >= t) {
            break;
        } else {
            ++segment_iter;
        }
    }

    if (segment_iter == m.end()) {
        return std::nullopt;
    } else {
        return *segment_iter;
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
void scan_ts_segments_general(const partition_manifest& m) {
    // Before range: should get first segment
    reference_check_timequery(
      m, model::timestamp{m.begin()->base_timestamp() - 100});

    for (partition_manifest::const_iterator i = m.begin(); i != m.end(); ++i) {
        // Time queries on times within the segment's range should return
        // this segment.
        reference_check_timequery(m, i->base_timestamp);
        reference_check_timequery(m, i->max_timestamp);
    }

    // After range: should get null
    reference_check_timequery(
      m, model::timestamp{m.last_segment()->max_timestamp() + 1});
}
/**
 * For a timequery() result, assert it is non-null and equal to the expected
 * offset
 */
void expect_ts_segment(
  const std::optional<partition_manifest::segment_meta>& s, int base_offset) {
    BOOST_REQUIRE(s);
    BOOST_REQUIRE_EQUAL(s->base_offset, model::offset{base_offset});
}

/**
 * For non-overlapping time ranges on segments, verify that each timestamp
 * maps to the expected segment.
 */
void scan_ts_segments(const partition_manifest& m) {
    // Before range: should get first segment
    expect_ts_segment(
      m.timequery(model::timestamp{m.begin()->base_timestamp() - 100}),
      m.begin()->base_offset);

    for (partition_manifest::const_iterator i = m.begin(); i != m.end(); ++i) {
        // Time queries on times within the segment's range should return
        // this segment.
        expect_ts_segment(m.timequery(i->base_timestamp), i->base_offset);
        expect_ts_segment(m.timequery(i->max_timestamp), i->base_offset);
    }

    // After range: should get null
    BOOST_REQUIRE(
      m.timequery(model::timestamp{m.last_segment()->max_timestamp() + 1})
      == std::nullopt);

    // Also compare all results with reference implementation.
    scan_ts_segments_general(m);
}

/**
 * Test time queries on a manifest with contiguous non-overlapping time ranges.
 */
SEASTAR_THREAD_TEST_CASE(test_timequery_complete) {
    partition_manifest m;
    m.update(
       manifest_format::json, make_manifest_stream(complete_manifest_json))
      .get();

    scan_ts_segments(m);
}

/**
 * Test time query when there is only one segment in the manifest
 */
SEASTAR_THREAD_TEST_CASE(test_timequery_single_segment) {
    partition_manifest m;
    m.update(
       manifest_format::json,
       make_manifest_stream(max_segment_meta_manifest_json))
      .get();

    scan_ts_segments(m);
}

SEASTAR_THREAD_TEST_CASE(test_timequery_gaps) {
    partition_manifest m;
    m.update(manifest_format::json, make_manifest_stream(manifest_with_gaps))
      .get();

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
    m.update(manifest_format::json, make_manifest_stream(wide_timestamps))
      .get();

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
    m.update(
       manifest_format::json, make_manifest_stream(overlapping_timestamps))
      .get();

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
    m.update(manifest_format::json, make_manifest_stream(raw)).get();

    // Before range: should get first segment
    expect_ts_segment(
      m.timequery(model::timestamp{m.begin()->base_timestamp() - 100}),
      m.begin()->base_offset);

    for (partition_manifest::const_iterator i = m.begin(); i != m.end(); ++i) {
        // Not checking results: undefined for out of order data.
        m.timequery(i->base_timestamp);
        m.timequery(i->max_timestamp);
    }

    // After range: should get null
    BOOST_REQUIRE(
      m.timequery(model::timestamp{m.last_segment()->max_timestamp() + 1})
      == std::nullopt);
}

SEASTAR_THREAD_TEST_CASE(test_reset_manifest) {
    partition_manifest m;
    m.update(
       manifest_format::json, make_manifest_stream(complete_manifest_json))
      .get();
    auto path = m.get_manifest_path(path_provider);
    BOOST_REQUIRE_EQUAL(
      path, "60000000/meta/test-ns/test-topic/42_1/manifest.bin");
    BOOST_REQUIRE_EQUAL(m.size(), 5);

    partition_manifest expected{
      m.get_ntp(), m.get_revision_id(), m.mem_tracker()};

    m.unsafe_reset();
    BOOST_REQUIRE(m == expected);
}

SEASTAR_THREAD_TEST_CASE(test_partition_manifest_v2_json) {
    // manifest_version::v2 gets generated by converting manifest.bin (serde) to
    // an equivalent json. the differences with v1 are only in the version
    // number. (2, to distinguish from v1 that is generated by old redpanda)
    constexpr static std::string_view v2_json = R"json(
{
  "version": 2,
  "namespace": "kafka",
  "topic": "panda-topic",
  "partition": 0,
  "revision": 21,
  "last_offset": 22159,
  "segments": {},
  "replaced": {},
  "insync_offset": 25554,
  "last_uploaded_compacted_offset": 9223372036854776000,
  "start_offset": 0
}
  )json";
    partition_manifest m;
    auto buf = iobuf{};
    buf.append(v2_json.data(), v2_json.size());
    m.update_with_json(std::move(buf));
    BOOST_CHECK_EQUAL(m.size(), 0);
    BOOST_CHECK_EQUAL(m.replaced_segments_count(), 0);
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
    m.update(manifest_format::json, make_manifest_stream(raw)).get();

    {
        // old format with archival term
        auto s = m.find(model::offset(10));
        auto expected = remote_segment_path(
          "9b367cb7/test-ns/test-topic/42_1/10-1-v1.log.1");
        auto actual = m.generate_segment_path(*s, path_provider);
        BOOST_REQUIRE_EQUAL(expected, actual);
    }

    {
        // new format with archival term
        auto s = m.find(model::offset(20));
        auto expected = remote_segment_path(
          "96c6b7a9/test-ns/test-topic/42_1/20-29-2048-1-v1.log.2");
        auto actual = m.generate_segment_path(*s, path_provider);
        BOOST_REQUIRE_EQUAL(expected, actual);
    }

    {
        // old format without archival term
        auto s = m.find(model::offset(30));
        auto expected = remote_segment_path(
          "df1262f5/test-ns/test-topic/42_1/30-1-v1.log");
        auto actual = m.generate_segment_path(*s, path_provider);
        BOOST_REQUIRE_EQUAL(expected, actual);
    }

    {
        // new format without archival term
        auto s = m.find(model::offset(40));
        auto expected = remote_segment_path(
          "e44e8104/test-ns/test-topic/42_1/40-42-4096-2-v1.log");
        auto actual = m.generate_segment_path(*s, path_provider);
        BOOST_REQUIRE_EQUAL(expected, actual);
    }
}

SEASTAR_THREAD_TEST_CASE(test_cloud_log_size_updates) {
    partition_manifest manifest;
    manifest
      .update(manifest_format::json, make_manifest_stream(empty_manifest_json))
      .get();

    BOOST_REQUIRE_EQUAL(manifest.cloud_log_size(), 0);

    segment_meta seg1{
      .is_compacted = false,
      .size_bytes = 1024,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(10)};

    manifest.add(
      segment_name(fmt::format("{}-1-v1.log", model::offset(0))), seg1);

    BOOST_REQUIRE_EQUAL(manifest.size(), 1);
    BOOST_REQUIRE_EQUAL(manifest.cloud_log_size(), 1024);

    segment_meta seg2{
      .is_compacted = false,
      .size_bytes = 1024,
      .base_offset = model::offset(11),
      .committed_offset = model::offset(20)};

    manifest.add(
      segment_name(fmt::format("{}-1-v1.log", model::offset(11))), seg2);

    BOOST_REQUIRE_EQUAL(manifest.size(), 2);
    BOOST_REQUIRE_EQUAL(manifest.cloud_log_size(), 2048);

    segment_meta merged_seg{
      .is_compacted = false,
      .size_bytes = 2000,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(20),
      .sname_format = segment_name_format::v2};

    manifest.add(
      segment_name(fmt::format("{}-1-v1.log", model::offset(0))), merged_seg);

    BOOST_REQUIRE_EQUAL(manifest.size(), 1);
    BOOST_REQUIRE_EQUAL(manifest.cloud_log_size(), 2000);

    segment_meta compacted_seg{
      .is_compacted = true,
      .size_bytes = 100,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(20),
      .sname_format = segment_name_format::v2};

    manifest.add(
      segment_name(fmt::format("{}-1-v1.log", model::offset(0))),
      compacted_seg);

    BOOST_REQUIRE_EQUAL(manifest.size(), 1);
    BOOST_REQUIRE_EQUAL(manifest.cloud_log_size(), 100);
}

SEASTAR_THREAD_TEST_CASE(test_cloud_log_size_truncate_twice) {
    // This test advances the start offset of the manifest twice
    // in a row, without cleaning up the metadata in between.
    //
    // This scenario can occur when a leadership transfer happens
    // after houskeeping replicates the truncate command, but before
    // it gets to replicate the cleanup_metadata command. When the next
    // leader runs housekeeping, it might choose to advance the start offset
    // even further.

    partition_manifest manifest;
    manifest
      .update(
        manifest_format::json, make_manifest_stream(complete_manifest_json))
      .get();

    BOOST_REQUIRE_EQUAL(manifest.cloud_log_size(), 15360);

    manifest.advance_start_offset(model::offset{30});

    BOOST_REQUIRE_EQUAL(manifest.cloud_log_size(), 12288);

    manifest.advance_start_offset(model::offset{40});

    BOOST_REQUIRE_EQUAL(manifest.cloud_log_size(), 8192);
}

SEASTAR_THREAD_TEST_CASE(test_deserialize_v1_manifest) {
    // Test the deserialisation of v1 partition manifests (prior to v23.2).

    constexpr std::string_view v1_manifest_json = R"json({
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
        "40-11-v1.log": {
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

    partition_manifest manifest;
    manifest
      .update(manifest_format::json, make_manifest_stream(v1_manifest_json))
      .get();

    BOOST_REQUIRE_EQUAL(manifest.cloud_log_size(), 11264);
}

SEASTAR_THREAD_TEST_CASE(test_readd_protection) {
    partition_manifest m(manifest_ntp, model::initial_revision_id(1));
    segment_meta s{
      .is_compacted = false,
      .size_bytes = 1024,
      .base_offset = model::offset(111222),
      .committed_offset = model::offset(222333),
      .delta_offset = model::offset_delta(42),
      .ntp_revision = model::initial_revision_id(1),
      .delta_offset_end = model::offset_delta(24),
      .sname_format = segment_name_format::v2,
    };
    BOOST_REQUIRE(m.add(s));
    auto size = m.cloud_log_size();
    BOOST_REQUIRE(m.add(s) == std::nullopt);
    BOOST_REQUIRE(m.cloud_log_size() == size);
    auto backlog = m.replaced_segments();
    BOOST_REQUIRE(backlog.empty());
}

SEASTAR_THREAD_TEST_CASE(test_archive_offsets_serialization) {
    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    m.add(
      segment_name("1000-1-v1.log"),
      {
        .base_offset = model::offset{1000},
        .committed_offset = model::offset{1999},
      });
    m.add(
      segment_name("2000-1-v1.log"),
      {
        .base_offset = model::offset{2000},
        .committed_offset = model::offset{3000},
      });
    BOOST_REQUIRE(m.get_start_offset() == model::offset(1000));
    m.set_archive_start_offset(model::offset(100), model::offset_delta(0));
    m.set_archive_clean_offset(model::offset(50), 0);

    BOOST_REQUIRE(m.get_archive_start_offset() == model::offset(100));
    BOOST_REQUIRE(m.get_archive_clean_offset() == model::offset(50));

    auto [is, size] = m.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    partition_manifest restored;
    restored.update(std::move(rstr)).get();

    BOOST_REQUIRE_EQUAL(
      restored.get_archive_start_offset(), model::offset(100));
    BOOST_REQUIRE_EQUAL(restored.get_archive_clean_offset(), model::offset(50));
}

SEASTAR_THREAD_TEST_CASE(test_partition_manifest_outofbound_trigger) {
    BOOST_TEST_INFO(
      fmt::format("random_seed: [{}]", random_generators::internal::gen));
    auto m = partition_manifest{manifest_ntp, model::initial_revision_id(0)};
    BOOST_REQUIRE(m.get_start_offset() == std::nullopt);
    auto max_committed_offset = random_generators::get_int(0, 100000);
    auto all_bo = std::vector<model::offset>{};
    for (int i = 0; i < max_committed_offset;) {
        auto co = random_generators::get_int(1, 100);
        m.add(partition_manifest::segment_meta{
          .base_offset = model::offset{i},
          .committed_offset = model::offset{i + co},
        });
        i += co;
        all_bo.emplace_back(model::offset{i});
    }
    for (auto o : all_bo) {
        auto b = m.begin();
        auto e = m.end();
        m.truncate(o);
        vlog(
          test_log.debug,
          "Truncating from {}, iterating from {}",
          o,
          b->base_offset);
        model::offset sum{0};
        size_t size_sum{0};
        for (; b != e; ++b) {
            auto t = *b;
            sum = sum + t.base_offset;
            size_sum += m.size();
        }
        vlog(
          test_log.debug,
          "Scanned from {}, sum {}, size_sum{}",
          o,
          sum,
          size_sum);
    }
}

SEASTAR_THREAD_TEST_CASE(test_partition_manifest_unsafe_segment_add) {
    auto manifest = partition_manifest{
      manifest_ntp, model::initial_revision_id(0)};
    // [3899366 4015450]
    // [4015451 4133668]
    // [4133669 4253000]
    // [4253001 4371690]
    // [4371691 4485596]
    std::vector<segment_meta> segments = {
      {.is_compacted = true,
       .size_bytes = 107304760,
       .base_offset = model::offset(3899366),
       .committed_offset = model::offset(4015450),
       .base_timestamp = model::timestamp{1689985196373},
       .max_timestamp = model::timestamp{1689985643129},
       .delta_offset = model::offset_delta(27),
       .ntp_revision = model::initial_revision_id(1660922),
       .archiver_term = model::term_id(16),
       .segment_term = model::term_id(1),
       .delta_offset_end = model::offset_delta(27),
       .sname_format = segment_name_format::v3,
       .metadata_size_hint = 0},
      {.is_compacted = true,
       .size_bytes = 107304760,
       .base_offset = model::offset(4015451),
       .committed_offset = model::offset(4133668),
       .base_timestamp = model::timestamp{1689985196373},
       .max_timestamp = model::timestamp{1689985643129},
       .delta_offset = model::offset_delta(27),
       .ntp_revision = model::initial_revision_id(1660922),
       .archiver_term = model::term_id(16),
       .segment_term = model::term_id(1),
       .delta_offset_end = model::offset_delta(29),
       .sname_format = segment_name_format::v3,
       .metadata_size_hint = 0},
      {.is_compacted = true,
       .size_bytes = 107304760,
       .base_offset = model::offset(4133669),
       .committed_offset = model::offset(4253000),
       .base_timestamp = model::timestamp{1689985196373},
       .max_timestamp = model::timestamp{1689985643129},
       .delta_offset = model::offset_delta(29),
       .ntp_revision = model::initial_revision_id(1660922),
       .archiver_term = model::term_id(16),
       .segment_term = model::term_id(1),
       .delta_offset_end = model::offset_delta(30),
       .sname_format = segment_name_format::v3,
       .metadata_size_hint = 0},
      {.is_compacted = true,
       .size_bytes = 107304760,
       .base_offset = model::offset(4253001),
       .committed_offset = model::offset(4371690),
       .base_timestamp = model::timestamp{1689985196373},
       .max_timestamp = model::timestamp{1689985643129},
       .delta_offset = model::offset_delta(30),
       .ntp_revision = model::initial_revision_id(1660922),
       .archiver_term = model::term_id(16),
       .segment_term = model::term_id(1),
       .delta_offset_end = model::offset_delta(32),
       .sname_format = segment_name_format::v3,
       .metadata_size_hint = 0},
      {.is_compacted = true,
       .size_bytes = 107304760,
       .base_offset = model::offset(4371691),
       .committed_offset = model::offset(4485596),
       .base_timestamp = model::timestamp{1689985196373},
       .max_timestamp = model::timestamp{1689985643129},
       .delta_offset = model::offset_delta(32),
       .ntp_revision = model::initial_revision_id(1660922),
       .archiver_term = model::term_id(16),
       .segment_term = model::term_id(1),
       .delta_offset_end = model::offset_delta(32),
       .sname_format = segment_name_format::v3,
       .metadata_size_hint = 0}};

    for (auto m : segments) {
        bool is_safe = manifest.safe_segment_meta_to_add(m);
        BOOST_REQUIRE(is_safe);
        manifest.add(m);
    }

    // Updates are safe to apply individually but not together
    std::vector<segment_meta> normal = {
      {.is_compacted = false,
       .size_bytes = 107304760,
       .base_offset = model::offset(3899366),
       .committed_offset = model::offset(4253000),
       .base_timestamp = model::timestamp{1689985196373},
       .max_timestamp = model::timestamp{1689985643129},
       .delta_offset = model::offset_delta(27),
       .ntp_revision = model::initial_revision_id(1660922),
       .archiver_term = model::term_id(16),
       .segment_term = model::term_id(1),
       .delta_offset_end = model::offset_delta(30),
       .sname_format = segment_name_format::v3,
       .metadata_size_hint = 0},
      {.is_compacted = false,
       .size_bytes = 120952194,
       .base_offset = model::offset(4133669),
       .committed_offset = model::offset(4485596),
       .base_timestamp = model::timestamp{1689985493616},
       .max_timestamp = model::timestamp{1689985943704},
       .delta_offset = model::offset_delta(29),
       .ntp_revision = model::initial_revision_id(1660922),
       .archiver_term = model::term_id(16),
       .segment_term = model::term_id(1),
       .delta_offset_end = model::offset_delta(32),
       .sname_format = segment_name_format::v3,
       .metadata_size_hint = 0},
    };

    // Check that updates are safe to apply individually
    for (auto m : normal) {
        bool is_safe = manifest.safe_segment_meta_to_add(m);
        BOOST_REQUIRE(is_safe);
    }

    // Check that updates are not safe to apply together
    {
        auto tmp = partition_manifest{
          manifest_ntp, model::initial_revision_id(0)};
        for (auto m : segments) {
            bool is_safe = tmp.safe_segment_meta_to_add(m);
            BOOST_REQUIRE(is_safe);
            tmp.add(m);
        }
        BOOST_REQUIRE(tmp.safe_segment_meta_to_add(normal[0]));
        tmp.add(normal[0]);
        BOOST_REQUIRE(!tmp.safe_segment_meta_to_add(normal[1]));
    }

    std::vector<segment_meta> compacted = {
      {.is_compacted = true,
       .size_bytes = 10730476,
       .base_offset = model::offset(3899366),
       .committed_offset = model::offset(4253000),
       .base_timestamp = model::timestamp{1689985196373},
       .max_timestamp = model::timestamp{1689985643129},
       .delta_offset = model::offset_delta(27),
       .ntp_revision = model::initial_revision_id(1660922),
       .archiver_term = model::term_id(16),
       .segment_term = model::term_id(1),
       .delta_offset_end = model::offset_delta(30),
       .sname_format = segment_name_format::v3,
       .metadata_size_hint = 0},
      {.is_compacted = true,
       .size_bytes = 12095219,
       .base_offset = model::offset(4133669),
       .committed_offset = model::offset(4485596),
       .base_timestamp = model::timestamp{1689985493616},
       .max_timestamp = model::timestamp{1689985943704},
       .delta_offset = model::offset_delta(29),
       .ntp_revision = model::initial_revision_id(1660922),
       .archiver_term = model::term_id(16),
       .segment_term = model::term_id(1),
       .delta_offset_end = model::offset_delta(32),
       .sname_format = segment_name_format::v3,
       .metadata_size_hint = 0},
    };

    // Check that updates are safe to apply individually
    for (auto m : compacted) {
        bool is_safe = manifest.safe_segment_meta_to_add(m);
        BOOST_REQUIRE(is_safe);
    }

    // Check that updates are not safe to apply together
    {
        auto tmp = partition_manifest{
          manifest_ntp, model::initial_revision_id(0)};
        for (auto m : segments) {
            bool is_safe = tmp.safe_segment_meta_to_add(m);
            BOOST_REQUIRE(is_safe);
            tmp.add(m);
        }
        BOOST_REQUIRE(tmp.safe_segment_meta_to_add(compacted[0]));
        tmp.add(compacted[0]);
        BOOST_REQUIRE(!tmp.safe_segment_meta_to_add(compacted[1]));
    }
}

SEASTAR_THREAD_TEST_CASE(test_last_partition_scrub_json_serde) {
    /*
     * Test that JSON ser/de works for last_partition_scrub and
     * last_scrubbed_offset
     */
    constexpr std::string_view manifest_v3 = R"json({
    "version": 3,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 0,
    "insync_offset": 0,
    "last_offset": 0,
    "last_partition_scrub": 100,
    "last_scrubbed_offset": 10
})json";

    partition_manifest manifest;
    manifest.update(manifest_format::json, make_manifest_stream(manifest_v3))
      .get();

    BOOST_REQUIRE_EQUAL(manifest.last_partition_scrub(), model::timestamp(100));
    BOOST_REQUIRE_EQUAL(
      manifest.last_scrubbed_offset().value(), model::offset(10));

    std::stringstream sstr;
    manifest.serialize_json(sstr);

    partition_manifest manifest_after_round_trip;
    manifest_after_round_trip
      .update(manifest_format::json, make_manifest_stream(sstr.str()))
      .get();

    BOOST_REQUIRE(manifest == manifest_after_round_trip);
}

SEASTAR_THREAD_TEST_CASE(
  test_manifest_serialization_roundtrip_with_applied_offset) {
    std::map<ss::sstring, partition_manifest::segment_meta> expected_segments
      = {
        {"0-1-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 1024,
           .base_offset = model::offset(0),
           .committed_offset = model::offset(99),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(1),
         }},
        {"100-2-v1.log",
         partition_manifest::segment_meta{
           .is_compacted = false,
           .size_bytes = 2048,
           .base_offset = model::offset(100),
           .committed_offset = model::offset(199),
           .ntp_revision = model::initial_revision_id(1),
           .segment_term = model::term_id(2),
         }},
      };

    partition_manifest m(manifest_ntp, model::initial_revision_id(0));
    for (const auto& segment : expected_segments) {
        m.add(segment_name(segment.first), segment.second);
    }
    m.advance_applied_offset(model::offset{100});
    BOOST_REQUIRE(m.get_applied_offset() == model::offset{100});

    auto [is, size] = m.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    partition_manifest restored;
    restored.update(std::move(rstr)).get();

    BOOST_REQUIRE(restored == m);
    BOOST_REQUIRE(m.get_applied_offset() == model::offset{100});
}
