// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

SEASTAR_THREAD_TEST_CASE(test_xor) {
    const std::array<char, 4> data = {33, 17, 27, 103};
    const std::array<char, 4> data2 = {13, 3, 125, 73};

    const auto data3 = data ^ data2;
    const std::array<char, 4> computed_xor = {44, 18, 102, 46};
    BOOST_REQUIRE(data3 == computed_xor);

    const auto data4
      = bytes(reinterpret_cast<const uint8_t*>(data.data()), data.size())
        ^ bytes(reinterpret_cast<const uint8_t*>(data2.data()), data2.size());

    BOOST_REQUIRE_EQUAL(
      data4,
      bytes(reinterpret_cast<const uint8_t*>(data3.data()), data3.size()));
}

SEASTAR_THREAD_TEST_CASE(bytes_lt) {
    for (int a = 0; a <= 255; ++a) {
        for (int b = 0; b <= 255; ++b) {
            uint8_t real_a = a;
            uint8_t real_b = b;
            bytes bytes_a(&real_a, 1);
            bytes bytes_b(&real_b, 1);
            BOOST_REQUIRE_EQUAL(bytes_a < bytes_b, real_a < real_b);
            BOOST_REQUIRE_EQUAL(bytes_a == bytes_b, real_a == real_b);
        }
    }
}
