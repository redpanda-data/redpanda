/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/streambuf.h"
#include "json/reader.h"
#include "serde/json/parser.h"
#include "serde/json/tests/data.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/perf_tests.hh>

#include <boost/test/unit_test.hpp>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/istreamwrapper.h>

#include <utility>

using namespace serde::json;

struct json_parser_bench {};

// Simple test to ensure the parse doesn't fail on valid sample data. The
// contents and correctness is not verified in this test.
ss::future<> run_serde_bench() {
    auto parser = serde::json::parser(co_await json_test_suite_sample());

    perf_tests::start_measuring_time();

    while (co_await parser.next()) {
        perf_tests::do_not_optimize(parser.token());
        // Do nothing, just drain the parser.
        // The contents and correctness is not verified in this test.
    }

    perf_tests::stop_measuring_time();

    BOOST_TEST_REQUIRE(
      parser.token() == token::eof,
      fmt::format("Expected to reach EOF but got: {}", parser.token()));
}

ss::future<> run_rapidjson_bench() {
    auto buf = co_await json_test_suite_sample();
    iobuf_istreambuf ibuf(buf);
    std::istream stream(&ibuf);
    rapidjson::IStreamWrapper wrapper(stream);

    json::Reader reader;
    rapidjson::BaseReaderHandler<> h;

    perf_tests::start_measuring_time();
    BOOST_TEST_REQUIRE(reader.Parse(wrapper, h));
    perf_tests::stop_measuring_time();
}

PERF_TEST_C(json_parser_bench, serde) { co_await run_serde_bench(); }
PERF_TEST_C(json_parser_bench, rapidjson) { co_await run_rapidjson_bench(); }
