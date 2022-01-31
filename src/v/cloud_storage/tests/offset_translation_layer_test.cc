/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/manifest.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/types.h"
#include "seastarx.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"

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

using namespace std::chrono_literals;
using namespace cloud_storage;

inline ss::logger test_log("test"); // NOLINT

static constexpr std::string_view serialized_manifest = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 10,
    "last_offset": 201,
    "segments": {
        "0-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 1000,
            "committed_offset": 100,
            "base_offset": 0,
            "delta_offset": 0,
            "max_timestamp":1625597000000
        },
        "101-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 1000,
            "committed_offset": 200,
            "base_offset": 101,
            "delta_offset": 1,
            "max_timestamp":1625597000001
        },
        "201-1-v1.log": {
            "is_compacted": false,
            "size_bytes": 1000,
            "committed_offset": 300,
            "base_offset": 201,
            "delta_offset": 3,
            "max_timestamp":1625597000002
        }
    }
})json";

inline ss::input_stream<char> make_manifest_stream(std::string_view json) {
    iobuf i;
    i.append(json.data(), json.size());
    return make_iobuf_input_stream(std::move(i));
}

SEASTAR_THREAD_TEST_CASE(test_name_translation) {
    retry_chain_node fib;
    manifest m;
    m.update(make_manifest_stream(serialized_manifest)).get0();
    BOOST_REQUIRE_EQUAL(m.size(), 3);

    std::vector<std::pair<std::string, std::string>> orig2expected{
      {"0-1-v1.log", "0-1-v1.log"},
      {"101-1-v1.log", "100-1-v1.log"},
      {"201-1-v1.log", "198-1-v1.log"},
    };

    for (const auto& [orig, expected] : orig2expected) {
        segment_name orig_name{orig};
        segment_name_components key = parse_segment_name(orig_name).value();
        auto meta = m.get(orig_name);
        BOOST_REQUIRE(meta);
        offset_translator otl(meta->delta_offset);
        BOOST_REQUIRE_EQUAL(otl.get_adjusted_segment_name(key, fib), expected);
    }
}
