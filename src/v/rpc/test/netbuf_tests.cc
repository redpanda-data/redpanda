// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/parse_utils.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

// utils
#include "test_types.h"

#include <fmt/ostream.h>

namespace rpc {
/// \brief expects the inputstream to be prefixed by an rpc::header
template<typename T>
ss::future<T> parse_framed(ss::input_stream<char>& in) {
    return parse_header(in).then([&in](std::optional<header> o) {
        return parse_type<T, default_message_codec>(in, o.value());
    });
}
} // namespace rpc

SEASTAR_THREAD_TEST_CASE(netbuf_pod) {
    auto n = rpc::netbuf();
    // type to serialize out
    pod src;
    src.x = 88;
    src.y = 88;
    src.z = 88;
    n.set_correlation_id(42);
    n.set_service_method({"test::test", 66});
    reflection::async_adl<pod>{}.to(n.buffer(), src).get();
    // forces the computation of the header
    auto bufs = std::move(n).as_scattered().get().release().release();
    auto in = make_iobuf_input_stream(iobuf(std::move(bufs)));
    const pod dst = rpc::parse_framed<pod>(in).get();
    BOOST_REQUIRE_EQUAL(src.x, dst.x);
    BOOST_REQUIRE_EQUAL(src.y, dst.y);
    BOOST_REQUIRE_EQUAL(src.z, dst.z);
}
