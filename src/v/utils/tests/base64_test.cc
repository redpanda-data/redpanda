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
    auto encdec = [](std::string_view input, std::string_view expected) {
        auto encoded = bytes_to_base64(bytes::from_string(input));
        BOOST_REQUIRE_EQUAL(encoded, expected);
        auto decoded = base64_to_bytes(encoded);
        BOOST_REQUIRE_EQUAL(decoded, bytes::from_string(input));
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

    encdec(iobuf::from(""), "");
    encdec(iobuf::from("this is a string"), "dGhpcyBpcyBhIHN0cmluZw==");
    encdec(iobuf::from("a"), "YQ==");

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

BOOST_AUTO_TEST_CASE(base64_url_decode_test_basic) {
    auto dec = [](std::string_view input, std::string_view expected) {
        auto decoded = base64url_to_bytes(input);
        BOOST_REQUIRE_EQUAL(decoded, bytes::from_string(expected));
    };

    dec("UmVkcGFuZGEgUm9ja3M", "Redpanda Rocks");
    // ChatGPT was asked to describe the Redpanda product
    dec(
      "UmVkcGFuZGEgaXMgYSBjdXR0aW5nLWVkZ2UgZGF0YSBzdHJlYW1pbmcgcGxhdGZvcm0gZGVz"
      "aWduZWQgdG8gb2ZmZXIgYSBoaWdoLXBlcmZvcm1hbmNlIGFsdGVybmF0aXZlIHRvIEFwYWNo"
      "ZSBLYWZrYS4gSXQncyBjcmFmdGVkIHRvIGhhbmRsZSB2YXN0IGFtb3VudHMgb2YgcmVhbC10"
      "aW1lIGRhdGEgZWZmaWNpZW50bHksIG1ha2luZyBpdCBhbiBleGNlbGxlbnQgY2hvaWNlIGZv"
      "ciBtb2Rlcm4gZGF0YS1kcml2ZW4gYXBwbGljYXRpb25zLiAgT3ZlcmFsbCwgUmVkcGFuZGEg"
      "cmVwcmVzZW50cyBhIGNvbXBlbGxpbmcgb3B0aW9uIGZvciBvcmdhbml6YXRpb25zIHNlZWtp"
      "bmcgYSBoaWdoLXBlcmZvcm1hbmNlLCBzY2FsYWJsZSwgYW5kIHJlbGlhYmxlIGRhdGEgc3Ry"
      "ZWFtaW5nIHNvbHV0aW9uLiBXaGV0aGVyIHlvdSdyZSBidWlsZGluZyByZWFsLXRpbWUgYW5h"
      "bHl0aWNzIGFwcGxpY2F0aW9ucywgcHJvY2Vzc2luZyBJb1QgZGF0YSBzdHJlYW1zLCBvciBt"
      "YW5hZ2luZyBldmVudC1kcml2ZW4gbWljcm9zZXJ2aWNlcywgUmVkcGFuZGEgaGFzIHlvdSBj"
      "b3ZlcmVkLg",
      "Redpanda is a cutting-edge data streaming platform designed to offer a "
      "high-performance alternative to Apache Kafka. It's crafted to handle "
      "vast amounts of real-time data efficiently, making it an excellent "
      "choice for modern data-driven applications.  Overall, Redpanda "
      "represents a compelling option for organizations seeking a "
      "high-performance, scalable, and reliable data streaming solution. "
      "Whether you're building real-time analytics applications, processing "
      "IoT data streams, or managing event-driven microservices, Redpanda has "
      "you covered.");

    dec("YQ", "a");
    dec("YWI", "ab");
    dec("YWJj", "abc");
    dec("", "");
}

BOOST_AUTO_TEST_CASE(base64_url_decode_invalid_character) {
    const std::string invalid_encode = "abc+/";
    BOOST_REQUIRE_THROW(
      base64url_to_bytes(invalid_encode), base64_url_decoder_exception);
    BOOST_REQUIRE_THROW(base64url_to_bytes("A"), base64_url_decoder_exception);
}
