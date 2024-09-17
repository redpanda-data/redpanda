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
      "afgz0119%21%23%24%26%27%28%29%2a%2b%2c%2F%3a%3b%3d%3f%"
      "40%5b%5dafgz0119");

    ASSERT_EQ(
      http::uri_encode(input, http::uri_encode_slash::no),
      "afgz0119%21%23%24%26%27%28%29%2a%2b%2c/"
      "%3a%3b%3d%3f%40%5b%5dafgz0119");
}

TEST(uri_encoding, form_encoded_data) {
    const absl::flat_hash_map<ss::sstring, ss::sstring> input{
      {"afgz0119!#$&'()*+,/:;=?@[]afgz0119",
       "afgz0119!#$&'()*+,/:;=?@[]afgz0119"},
      {"foo", "bar"}};
    iobuf_parser p{http::form_encode_data(input)};

    // Order of kv pairs is not guaranteed due to map iteration order
    ASSERT_THAT(
      p.read_string(p.bytes_left()),
      AnyOf(
        "foo=bar&afgz0119%21%23%24%26%27%28%29%2a%2b%2c%2F%3a%3b%3d%3f%40%"
        "5b%"
        "5dafgz0119=afgz0119%21%23%24%26%27%28%29%2a%2b%2c%2F%3a%3b%3d%3f%"
        "40%"
        "5b%5dafgz0119",
        "afgz0119%21%23%24%26%27%28%29%2a%2b%2c%2F%3a%3b%3d%3f%"
        "40%5b%5dafgz0119=afgz0119%21%23%24%26%27%28%29%2a%2b%2c%2F%3a%"
        "3b%3d%3f%40%5b%5dafgz0119&foo=bar"));
}
