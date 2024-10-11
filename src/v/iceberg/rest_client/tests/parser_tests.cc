/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_client/parsers.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace r = iceberg::rest_client;
using namespace testing;

json::Document make_doc(ss::sstring s) {
    json::Document d;
    d.Parse(s);
    return d;
}

TEST(oauth_token, invalid_doc_fails_schema) {
    auto result = r::parse_oauth_token(make_doc("{}"));
    ASSERT_FALSE(result.has_value());
    ASSERT_THAT(
      result.error(),
      VariantWith<r::json_parse_error>(Field(
        &r::json_parse_error::context,
        "parse_oauth_token::response_does_not_match_schema")));
}

TEST(oauth_token, type_mismatch_fails_schema) {
    auto result = r::parse_oauth_token(
      make_doc(R"J({"access_token": "", "expires_in": "42"})J"));
    ASSERT_FALSE(result.has_value());
    ASSERT_THAT(
      result.error(),
      VariantWith<r::json_parse_error>(Field(
        &r::json_parse_error::context,
        "parse_oauth_token::response_does_not_match_schema")));
}

TEST(oauth_token, valid_doc_parse) {
    using namespace std::chrono_literals;
    auto result = r::parse_oauth_token(
      make_doc(R"J({"access_token": "", "expires_in": 42})J"));
    ASSERT_TRUE(result.has_value());
    const auto& token = result.value();
    ASSERT_EQ(token.token, "");
    // Assume that it doesn't take more than 2 seconds to get to this assertion
    ASSERT_THAT(
      token.expires_at - ss::lowres_clock::now(), AllOf(Le(42s), Ge(40s)));
}
