// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/json/iobuf.h"

#include "bytes/iobuf_parser.h"
#include "json/iobuf_writer.h"
#include "json/stringbuffer.h"
#include "pandaproxy/json/rjson_util.h"

#include <seastar/testing/thread_test_case.hh>

namespace pp = pandaproxy;
namespace ppj = pp::json;

SEASTAR_THREAD_TEST_CASE(test_iobuf_parse_binary) {
    auto [res, buf] = ppj::rjson_parse_impl<iobuf>{
      ppj::serialization_format::binary_v2}("cGFuZGFwcm94eQ==");
    BOOST_REQUIRE(res);
    BOOST_REQUIRE(!!buf);
    iobuf_parser p(std::move(*buf));
    BOOST_TEST(p.read_string(p.bytes_left()) == "pandaproxy");
}

SEASTAR_THREAD_TEST_CASE(test_iobuf_parse_binary_error) {
    auto [res, buf] = ppj::rjson_parse_impl<iobuf>{
      ppj::serialization_format::binary_v2}("cGFuZGFwcm94eQ=?");
    BOOST_REQUIRE(!res);
    BOOST_REQUIRE(!buf);
}

SEASTAR_THREAD_TEST_CASE(test_iobuf_serialize_binary) {
    iobuf in_buf;
    auto input = ss::sstring("pandaproxy");
    auto expected = ss::sstring("\"cGFuZGFwcm94eQ==\"");
    in_buf.append(input.data(), input.size());

    ::json::StringBuffer out_buf;
    ::json::iobuf_writer<::json::StringBuffer> w(out_buf);
    ppj::rjson_serialize_fmt(ppj::serialization_format::binary_v2)(
      w, std::move(in_buf));
    ss::sstring output{out_buf.GetString(), out_buf.GetSize()};

    BOOST_REQUIRE_EQUAL(output, expected);
}
