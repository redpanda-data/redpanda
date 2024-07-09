// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf_parser.h"
#include "bytes/random.h"
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

BOOST_AUTO_TEST_CASE(test_base64_to_iobuf) {
    const std::string_view a_string = "dGhpcyBpcyBhIHN0cmluZw==";
    iobuf buf;
    const size_t half = a_string.size() / 2;
    buf.append_fragments(iobuf::from(a_string.substr(0, half)));
    buf.append_fragments(iobuf::from(a_string.substr(half)));
    BOOST_REQUIRE_EQUAL(std::distance(buf.begin(), buf.end()), 2);

    auto decoded = base64_to_iobuf(buf);
    iobuf_parser p{std::move(decoded)};
    auto decoded_str = p.read_string(p.bytes_left());
    BOOST_REQUIRE_EQUAL(decoded_str, "this is a string");
}
