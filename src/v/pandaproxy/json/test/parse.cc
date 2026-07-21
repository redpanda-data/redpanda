// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "pandaproxy/json/requests/produce.h"
#include "pandaproxy/json/rjson_util.h"

#include <seastar/testing/perf_tests.hh>

namespace pp = pandaproxy;
namespace ppj = pp::json;

auto make_binary_v2_handler() {
    return ppj::produce_request_handler<>(ppj::serialization_format::binary_v2);
}
constexpr std::string_view rec
  = R"({"value": "dmVjdG9yaXplZA==","partition": 0})";

const auto data_size = 1 << 20; // 1 MB
const auto record_count = data_size / rec.size();

auto gen(size_t data_size) {
    const std::string_view beg{R"({"records": [)"};
    const std::string_view end{R"(]})"};
    std::string buf{beg};
    for (size_t i = 0; i < data_size - 1; ++i) {
        buf += rec;
        buf += ",";
    }
    buf += rec;
    buf += end;
    return buf;
}

inline void parse_test(size_t data_size) {
    auto input = gen(data_size);

    perf_tests::start_measuring_time();
    auto records = ppj::impl::rjson_parse(
      input.c_str(), make_binary_v2_handler());
    perf_tests::stop_measuring_time();
}

// Parse from an iobuf via chunked_input_stream, the path production HTTP
// bodies take (pandaproxy streams request content into an iobuf).
inline void parse_test_iobuf(size_t data_size) {
    auto input = gen(data_size);
    iobuf buf;
    buf.append(input.data(), input.size());

    perf_tests::start_measuring_time();
    auto records = ppj::rjson_parse(std::move(buf), make_binary_v2_handler());
    perf_tests::stop_measuring_time();
}

PERF_TEST(json_parse_test, binary) { parse_test(record_count); }
PERF_TEST(json_parse_test, binary_iobuf) { parse_test_iobuf(record_count); }
