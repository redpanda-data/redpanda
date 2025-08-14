// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/parse_utils.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/rw/sstring.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

// utils
#include "test_types.h"

#include <fmt/ostream.h>

struct envelope_pod
  : serde::envelope<envelope_pod, serde::version<0>, serde::compat_version<0>> {
    int16_t x = 1;
    int32_t y = 2;
    int64_t z = 3;
    auto serde_fields() { return std::tie(x, y, z); }
};

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
    envelope_pod src;
    src.x = 88;
    src.y = 88;
    src.z = 88;
    n.set_correlation_id(42);
    n.set_service_method({"test::test", 66});
    n.set_version(rpc::transport_version::v2);
    n.buffer() = serde::to_iobuf(src);
    // forces the computation of the header
    auto bufs = std::move(n).as_scattered().get().release().release();
    auto in = make_iobuf_input_stream(iobuf(std::move(bufs)));
    const envelope_pod dst = rpc::parse_framed<envelope_pod>(in).get();
    BOOST_REQUIRE_EQUAL(src.x, dst.x);
    BOOST_REQUIRE_EQUAL(src.y, dst.y);
    BOOST_REQUIRE_EQUAL(src.z, dst.z);
}
