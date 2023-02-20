// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/hash.h"
#include "bytes/iostream.h"
#include "bytes/tests/utils.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_reading_zero_bytes_empty_stream) {
    auto buf = iobuf();
    auto is = make_iobuf_input_stream(std::move(buf));

    auto read_buf = read_iobuf_exactly(is, 0).get0();
    BOOST_REQUIRE_EQUAL(read_buf.size_bytes(), 0);
};

SEASTAR_THREAD_TEST_CASE(test_reading_zero_bytes) {
    auto buf = iobuf();
    append_sequence(buf, 5);
    auto is = make_iobuf_input_stream(std::move(buf));

    auto read_buf = read_iobuf_exactly(is, 0).get0();
    BOOST_REQUIRE_EQUAL(read_buf.size_bytes(), 0);
};

SEASTAR_THREAD_TEST_CASE(test_reading_some_bytes) {
    auto buf = iobuf();
    append_sequence(buf, 5);
    auto is = make_iobuf_input_stream(std::move(buf));

    auto read_buf = read_iobuf_exactly(is, 16).get0();
    BOOST_REQUIRE_EQUAL(read_buf.size_bytes(), 16);
};

SEASTAR_THREAD_TEST_CASE(test_bytes_conversion) {
    static constexpr std::string_view key = "magic_key";
    iobuf buf;
    buf.append(key.data(), key.size());
    // convert to bytes
    bytes bytes_buf = iobuf_to_bytes(buf);

    // convert bytes back
    iobuf converted_back = bytes_to_iobuf(bytes_buf);
    bytes roundtrip_buf = iobuf_to_bytes(converted_back);
    BOOST_REQUIRE_EQUAL(buf, converted_back);
    BOOST_REQUIRE_EQUAL(bytes_type_eq{}(bytes_buf, buf), true);
    BOOST_REQUIRE_EQUAL(
      absl::Hash<bytes>{}(roundtrip_buf), absl::Hash<bytes>{}(bytes_buf));
    BOOST_REQUIRE_EQUAL(
      absl::Hash<iobuf>{}(buf), absl::Hash<iobuf>{}(converted_back));
}
