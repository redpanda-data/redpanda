// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "random/generators.h"
#include "utils/vint.h"
#include "utils/vint_iostream.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <iostream>
#include <random>

namespace {

void check_roundtrip_sweep(int64_t count) {
    for (int64_t i = -count; i < count; i += 100000) {
        const auto b = vint::to_bytes(i);
        const auto view = bytes_view(b);
        const auto [deserialized, _] = vint::deserialize(view);
        BOOST_REQUIRE_EQUAL(deserialized, i);
        BOOST_REQUIRE_EQUAL(b.size(), vint::vint_size(i));
    }
}

void check_roundtrip_sweep_unsigned(const uint64_t count) {
    for (uint64_t i = 0; i < count; i += 100000) {
        const auto b = unsigned_vint::to_bytes(i);
        const auto view = bytes_view(b);
        const auto [deserialized, _] = unsigned_vint::deserialize(view);
        BOOST_REQUIRE_EQUAL(deserialized, i);
    }
}

} // namespace

SEASTAR_THREAD_TEST_CASE(sanity_signed_sweep_64) {
    check_roundtrip_sweep(100000000);
}

SEASTAR_THREAD_TEST_CASE(sanity_unsigned_sweep_32) {
    check_roundtrip_sweep_unsigned(100000000);
}

SEASTAR_THREAD_TEST_CASE(test_unsigned_stream_deserializer) {
    /// Final byte must have 0b0xxx, the 0 denotes to stop reading
    static constexpr uint64_t max_unsigned_vint
      = ((1 << (4 * unsigned_vint::max_length)) - 1) ^ 8;
    const auto test_number = random_generators::get_int<uint64_t>(
      0, max_unsigned_vint - 1);
    const bytes b = unsigned_vint::to_bytes(test_number);
    auto istream = make_iobuf_input_stream(bytes_to_iobuf(b));
    auto [result, result_bytes_read]
      = unsigned_vint::stream_deserialize(istream).get();
    BOOST_CHECK_EQUAL(result, test_number);
}
