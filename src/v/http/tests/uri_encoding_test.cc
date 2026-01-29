/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf_parser.h"
#include "http/utils.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace testing;

TEST(uri_encoding, encode_special_chars) {
    constexpr auto input = "afgz0119!#$&'()*+,/:;=?@[]afgz0119";
    ASSERT_EQ(
      http::uri_encode(input, http::uri_encode_slash::yes),
      "afgz0119%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%"
      "40%5B%5Dafgz0119");

    ASSERT_EQ(
      http::uri_decode(http::uri_encode(input, http::uri_encode_slash::yes)),
      input);

    ASSERT_EQ(
      http::uri_encode(input, http::uri_encode_slash::no),
      "afgz0119%21%23%24%26%27%28%29%2A%2B%2C/"
      "%3A%3B%3D%3F%40%5B%5Dafgz0119");

    ASSERT_EQ(
      http::uri_decode(http::uri_encode(input, http::uri_encode_slash::no)),
      input);
}

TEST(uri_encoding, form_encoded_data) {
    const absl::flat_hash_map<ss::sstring, ss::sstring> input{
      {"afgz0119!#$&'()*+,/:;=?@[]afgz0119",
       "afgz0119!#$&'()*+,/:;=?@[]afgz0119"},
      {"foo", "bar"}};
    iobuf_parser p{http::form_encode_data(input)};

    // Order of kv pairs is not guaranteed due to map iteration order
    auto s = p.read_string(p.bytes_left());
    fmt::print(">>> {}\n", s);
    ASSERT_THAT(
      s,
      AnyOf(
        "foo=bar&afgz0119%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%"
        "5Dafgz0119=afgz0119%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%"
        "5B%5Dafgz0119",
        "afgz0119%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%"
        "40%5B%5Dafgz0119=afgz0119%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%"
        "3B%3D%3F%40%5B%5Dafgz0119&foo=bar"));
}

TEST(uri_decoding, invalid_hex) {
    constexpr auto input = "afgz0119%2G"; // invalid hex '2G'
    ASSERT_EQ(http::uri_decode(input), "afgz0119%2G");

    constexpr auto short_input = "afgz0119%2"; // incomplete hex '2'
    ASSERT_EQ(http::uri_decode(short_input), "afgz0119%2");
}
