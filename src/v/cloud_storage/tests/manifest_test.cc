/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage/manifest.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <exception>
#include <variant>

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
            "committed_offset": 19
        },
        "20-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 2048,
            "base_offset": 20,
            "committed_offset": 29,
            "max_timestamp": 1234567890
        },
        "01234567/test-ns/test-topic/42_1/30-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 4096,
            "base_offset": 30,
            "committed_offset": 39,
            "max_timestamp": 1234567890
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

SEASTAR_THREAD_TEST_CASE(test_manifest_path) {
    manifest m(manifest_ntp, model::revision_id(0));
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "20000000/meta/test-ns/test-topic/42_0/manifest.json");
}

SEASTAR_THREAD_TEST_CASE(test_segment_path) {
    manifest m(manifest_ntp, model::revision_id(0));
    auto path = m.get_remote_segment_path(segment_name("22-11-v1.log"));
    // use pre-calculated murmur hash value from full ntp path + file name
    BOOST_REQUIRE_EQUAL(path, "2bea9275/test-ns/test-topic/42_0/22-11-v1.log");
}

SEASTAR_THREAD_TEST_CASE(test_empty_manifest_update) {
    manifest m;
    m.update(make_manifest_stream(empty_manifest_json)).get0();
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "20000000/meta/test-ns/test-topic/42_0/manifest.json");
}

static ss::sstring key_to_string(const manifest::key& key) {
    if (std::holds_alternative<segment_name>(key)) {
        return std::get<segment_name>(key)();
    }
    return std::get<remote_segment_path>(key)().string();
}

SEASTAR_THREAD_TEST_CASE(test_complete_manifest_update) {
    manifest m;
    m.update(make_manifest_stream(complete_manifest_json)).get0();
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "60000000/meta/test-ns/test-topic/42_1/manifest.json");
    BOOST_REQUIRE_EQUAL(m.size(), 3);
    std::map<ss::sstring, manifest::segment_meta> expected = {
      {"10-1-v1.log",
       manifest::segment_meta{
         false, 1024, model::offset(10), model::offset(19)}},
      {"20-1-v1.log",
       manifest::segment_meta{
         false,
         2048,
         model::offset(20),
         model::offset(29),
         model::timestamp(1234567890),
         model::timestamp(1234567890)}},
      {"01234567/test-ns/test-topic/42_1/30-1-v1.log",
       manifest::segment_meta{
         false,
         4096,
         model::offset(30),
         model::offset(39),
         model::timestamp(1234567890),
         model::timestamp(1234567890)}},
    };
    for (const auto& actual : m) {
        auto it = expected.find(key_to_string(actual.first));
        BOOST_REQUIRE(it != expected.end());
        BOOST_REQUIRE_EQUAL(it->second.base_offset, actual.second.base_offset);
        BOOST_REQUIRE_EQUAL(
          it->second.committed_offset, actual.second.committed_offset);
        BOOST_REQUIRE_EQUAL(
          it->second.is_compacted, actual.second.is_compacted);
        BOOST_REQUIRE_EQUAL(it->second.size_bytes, actual.second.size_bytes);
        BOOST_REQUIRE_EQUAL(
          it->second.max_timestamp, actual.second.max_timestamp);
    }
}

SEASTAR_THREAD_TEST_CASE(test_manifest_serialization) {
    manifest m(manifest_ntp, model::revision_id(0));
    m.add(
      segment_name("10-1-v1.log"),
      {
        .is_compacted = false,
        .size_bytes = 1024,
        .base_offset = model::offset(10),
        .committed_offset = model::offset(19),
        .max_timestamp = model::timestamp::missing(),
      });
    m.add(
      segment_name("20-1-v1.log"),
      {
        .is_compacted = false,
        .size_bytes = 2048,
        .base_offset = model::offset(20),
        .committed_offset = model::offset(29),
        .max_timestamp = model::timestamp::missing(),
      });
    auto [is, size] = m.serialize();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    manifest restored;
    restored.update(std::move(rstr)).get0();

    BOOST_REQUIRE(m == restored);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_difference) {
    manifest a(manifest_ntp, model::revision_id(0));
    a.add(segment_name("1-1-v1.log"), {});
    a.add(segment_name("2-2-v1.log"), {});
    a.add(segment_name("3-3-v1.log"), {});
    manifest b(manifest_ntp, model::revision_id(0));
    b.add(segment_name("1-1-v1.log"), {});
    b.add(segment_name("2-2-v1.log"), {});
    {
        auto c = a.difference(b);
        BOOST_REQUIRE(c.size() == 1);
        auto res = *c.begin();
        BOOST_REQUIRE(
          std::get<segment_name>(res.first) == segment_name("3-3-v1.log"));
    }
    // check that set difference is not symmetrical
    b.add(segment_name("3-3-v1.log"), {});
    b.add(segment_name("4-4-v2.log"), {});
    {
        auto c = a.difference(b);
        BOOST_REQUIRE(c.size() == 0);
    }
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing) {
    std::filesystem::path path
      = "b0000000/meta/kafka/redpanda-test/4_2/manifest.json";
    auto res = cloud_storage::get_manifest_path_components(path);
    BOOST_REQUIRE_EQUAL(res->_origin, path);
    BOOST_REQUIRE_EQUAL(res->_ns(), "kafka");
    BOOST_REQUIRE_EQUAL(res->_topic(), "redpanda-test");
    BOOST_REQUIRE_EQUAL(res->_part(), 4);
    BOOST_REQUIRE_EQUAL(res->_rev(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing_failure_1) {
    std::filesystem::path path
      = "b0000000/meta/kafka/redpanda-test/a_b/manifest.json";
    auto res = cloud_storage::get_manifest_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing_failure_2) {
    std::filesystem::path path
      = "b0000000/kafka/redpanda-test/4_2/manifest.json";
    auto res = cloud_storage::get_manifest_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing_failure_3) {
    std::filesystem::path path
      = "b0000000/meta/kafka/redpanda-test//manifest.json";
    auto res = cloud_storage::get_manifest_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_manifest_name_parsing_failure_4) {
    std::filesystem::path path
      = "b0000000/meta/kafka/redpanda-test/4_2/foo.bar";
    auto res = cloud_storage::get_manifest_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_parsing) {
    std::filesystem::path path = "3587-1-v1.log";
    auto res = cloud_storage::get_segment_path_components(path);
    BOOST_REQUIRE_EQUAL(res->_origin, path);
    BOOST_REQUIRE_EQUAL(res->_is_full, false);
    BOOST_REQUIRE_EQUAL(res->_name, path.string());
    BOOST_REQUIRE_EQUAL(res->_base_offset(), 3587);
    BOOST_REQUIRE_EQUAL(res->_term(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_parsing_failure_1) {
    std::filesystem::path path = "-1-v1.log";
    auto res = cloud_storage::get_segment_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_segment_name_parsing_failure_2) {
    std::filesystem::path path = "abc-1-v1.log";
    auto res = cloud_storage::get_segment_path_components(path);
    BOOST_REQUIRE(res.has_value() == false);
}

SEASTAR_THREAD_TEST_CASE(test_segment_path_parsing) {
    std::filesystem::path path
      = "034b2193/kafka/redpanda-test/3_2/3587-1-v1.log";
    auto res = cloud_storage::get_segment_path_components(path);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE_EQUAL(res->_origin, path);
    BOOST_REQUIRE_EQUAL(res->_ns(), "kafka");
    BOOST_REQUIRE_EQUAL(res->_topic(), "redpanda-test");
    BOOST_REQUIRE_EQUAL(res->_part(), 3);
    BOOST_REQUIRE_EQUAL(res->_rev(), 2);
    BOOST_REQUIRE_EQUAL(res->_is_full, true);
    BOOST_REQUIRE_EQUAL(res->_name, path.filename().string());
    BOOST_REQUIRE_EQUAL(res->_base_offset(), 3587);
    BOOST_REQUIRE_EQUAL(res->_term(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_segment_path_parsing_failure_1) {
    std::filesystem::path path = "034b2193/kafka/redpanda-test/_/3587-1-v1.log";
    auto res = cloud_storage::get_segment_path_components(path);
    BOOST_REQUIRE(!res.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_segment_path_parsing_failure_2) {
    std::filesystem::path path
      = "034b2193/kafka/redpanda-test/3_2/foo-bar-v1.log";
    auto res = cloud_storage::get_segment_path_components(path);
    BOOST_REQUIRE(!res.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_segment_path_parsing_failure_3) {
    std::filesystem::path path = "034b2193/kafka/redpanda-test/3_2";
    auto res = cloud_storage::get_segment_path_components(path);
    BOOST_REQUIRE(!res.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_segment_path_parsing_failure_4) {
    std::filesystem::path path = "034b2193/redpanda-test/3_2/3587-1-v1.log";
    auto res = cloud_storage::get_segment_path_components(path);
    BOOST_REQUIRE(!res.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_segment_path_parsing_failure_5) {
    std::filesystem::path path
      = "00000000/meta/kafka/redpanda-test/3_2/manifest.log";
    auto res = cloud_storage::get_segment_path_components(path);
    BOOST_REQUIRE(!res.has_value());
}
