// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "random/generators.h"
#include "utils/base64.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(bytes_type) {
    auto encdec = [](const bytes& input, const auto expected) {
        auto encoded = bytes_to_base64(input);
        BOOST_REQUIRE_EQUAL(encoded, expected);
        auto decoded = base64_to_bytes(encoded);
        BOOST_REQUIRE_EQUAL(decoded, input);
    };

    encdec("", "");
    encdec("this is a string", "dGhpcyBpcyBhIHN0cmluZw==");
    encdec("a", "YQ==");
}

BOOST_AUTO_TEST_CASE(iobuf_type) {
    auto encdec = [](const iobuf& input, const auto expected) {
        auto encoded = iobuf_to_base64(input);
        BOOST_REQUIRE_EQUAL(encoded, expected);
        auto decoded = base64_to_bytes(encoded);
        BOOST_REQUIRE_EQUAL(decoded, iobuf_to_bytes(input));
    };

    encdec(bytes_to_iobuf(""), "");
    encdec(bytes_to_iobuf("this is a string"), "dGhpcyBpcyBhIHN0cmluZw==");
    encdec(bytes_to_iobuf("a"), "YQ==");

    // test with multiple iobuf fragments
    iobuf buf;
    while (std::distance(buf.begin(), buf.end()) < 3) {
        auto data = random_generators::get_bytes(128);
        buf.append(data.data(), data.size());
    }

    auto encoded = iobuf_to_base64(buf);
    auto decoded = base64_to_bytes(encoded);
    BOOST_REQUIRE_EQUAL(decoded, iobuf_to_bytes(buf));
}
