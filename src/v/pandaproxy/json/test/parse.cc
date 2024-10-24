// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf_parser.h"
#include "pandaproxy/json/iobuf.h"
#include "pandaproxy/json/requests/produce.h"
#include "pandaproxy/json/rjson_util.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/perf_tests.hh>

namespace pp = pandaproxy;
namespace ppj = pp::json;

auto make_binary_v2_handler() {
    return ppj::produce_request_handler<>(ppj::serialization_format::binary_v2);
}

auto gen(size_t data_size) {
    const ss::sstring beg{R"({"records": [)"};
    const ss::sstring end{R"(]})"};
    const ss::sstring rec{R"({"value": "dmVjdG9yaXplZA==","partition": 0})"};
    ss::sstring buf{
      ss::sstring::initialized_later{},
      beg.length() + end.length() + data_size * (rec.length() + 1)};
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

PERF_TEST(json_parse_test, binary) { parse_test(1 << 20); }
