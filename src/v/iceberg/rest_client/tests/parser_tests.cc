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

#include "iceberg/rest_client/parsers.h"

#include <gtest/gtest.h>

namespace r = iceberg::rest_client;

json::Document make_doc(ss::sstring s) {
    json::Document d;
    d.Parse(s);
    return d;
}

TEST(oauth_token, invalid_doc_fails) {
    {
        auto result = r::parse_oauth_token(make_doc("{}"));
        ASSERT_FALSE(result.has_value());
        auto error = result.error();
        ASSERT_TRUE(std::holds_alternative<r::json_parse_error>(error));

        const auto& parse_error = std::get<r::json_parse_error>(error);
        ASSERT_EQ(
          parse_error.context,
          "parse_oauth_token::response_does_not_match_schema");
    }

    {
        auto result = r::parse_oauth_token(
          make_doc(R"J({"access_token": "", "expires_in": "42"})J"));
        ASSERT_FALSE(result.has_value());
        auto error = result.error();
        ASSERT_TRUE(std::holds_alternative<r::json_parse_error>(error));

        const auto& parse_error = std::get<r::json_parse_error>(error);
        ASSERT_EQ(
          parse_error.context,
          "parse_oauth_token::response_does_not_match_schema");
    }
}

TEST(oauth_token, valid_doc_parse) {
    using namespace std::chrono_literals;
    auto result = r::parse_oauth_token(
      make_doc(R"J({"access_token": "", "expires_in": 42})J"));
    ASSERT_TRUE(result.has_value());
    auto token = result.value();
    ASSERT_EQ(token.token, "");
    ASSERT_EQ(token.expiry, 42s);
}
