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

#include "http/utils.h"

#include <gtest/gtest.h>

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
