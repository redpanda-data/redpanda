/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/manifest.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
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

using namespace archival;

static std::string_view empty_manifest_json = R"json({
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 0
})json";
static std::string_view complete_manifest_json = R"json({
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 1,
    "segments": {
        "10-1-v1.log": { 
            "is_compacted": false,
            "size_bytes": 1024,
            "base_offset": 10,
            "committed_offset": 19,
            "deleted": false
        },
        "20-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 2048,
            "base_offset": 20,
            "committed_offset": 29,
            "deleted": false
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
      path, "a0000000/meta/test-ns/test-topic/42_0/manifest.json");
}

SEASTAR_THREAD_TEST_CASE(test_segment_path) {
    manifest m(manifest_ntp, model::revision_id(0));
    auto path = m.get_remote_segment_path(segment_name("22-11-v1.log"));
    // use pre-calculated murmur hash value from full ntp path + file name
    BOOST_REQUIRE_EQUAL(path, "7da10588/test-ns/test-topic/42_0/22-11-v1.log");
}

SEASTAR_THREAD_TEST_CASE(test_empty_manifest_update) {
    manifest m;
    m.update(make_manifest_stream(empty_manifest_json)).get0();
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "a0000000/meta/test-ns/test-topic/42_0/manifest.json");
}

SEASTAR_THREAD_TEST_CASE(test_complete_manifest_update) {
    manifest m;
    m.update(make_manifest_stream(complete_manifest_json)).get0();
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "50000000/meta/test-ns/test-topic/42_1/manifest.json");
    BOOST_REQUIRE_EQUAL(m.size(), 2);
    std::map<ss::sstring, manifest::segment_meta> expected = {
      {"10-1-v1.log",
       manifest::segment_meta{
         false, 1024, model::offset(10), model::offset(19), false}},
      {"20-1-v1.log",
       manifest::segment_meta{
         false, 2048, model::offset(20), model::offset(29), false}}};
    for (const auto& actual : m) {
        auto it = expected.find(actual.first);
        BOOST_REQUIRE(it != expected.end());
        BOOST_REQUIRE_EQUAL(it->second.base_offset, actual.second.base_offset);
        BOOST_REQUIRE_EQUAL(
          it->second.committed_offset, actual.second.committed_offset);
        BOOST_REQUIRE_EQUAL(
          it->second.is_compacted, actual.second.is_compacted);
        BOOST_REQUIRE_EQUAL(it->second.size_bytes, actual.second.size_bytes);
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
        .is_deleted_locally = false,
      });
    m.add(
      segment_name("20-1-v1.log"),
      {
        .is_compacted = false,
        .size_bytes = 2048,
        .base_offset = model::offset(20),
        .committed_offset = model::offset(29),
        .is_deleted_locally = false,
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
    a.add(segment_name("0-0-1.log"), {});
    a.add(segment_name("0-0-2.log"), {});
    a.add(segment_name("0-0-3.log"), {});
    manifest b(manifest_ntp, model::revision_id(0));
    b.add(segment_name("0-0-1.log"), {});
    b.add(segment_name("0-0-2.log"), {});
    {
        auto c = a.difference(b);
        BOOST_REQUIRE(c.size() == 1);
        auto res = *c.begin();
        BOOST_REQUIRE(res.first() == "0-0-3.log");
    }
    // check that set difference is not symmetrical
    b.add(segment_name("0-0-3.log"), {});
    b.add(segment_name("0-0-4.log"), {});
    {
        auto c = a.difference(b);
        BOOST_REQUIRE(c.size() == 0);
    }
}
