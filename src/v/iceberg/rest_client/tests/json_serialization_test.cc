/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_client/json.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace testing;

json::Document make_doc(ss::sstring s) {
    json::Document d;
    d.Parse(s);
    return d;
}

TEST(oauth_token, invalid_doc_fails_schema) {
    ASSERT_THROW(
      iceberg::rest_client::parse_oauth_token(make_doc("{}")),
      std::invalid_argument);
}

TEST(oauth_token, type_mismatch_fails_schema) {
    ASSERT_THROW(
      iceberg::rest_client::parse_oauth_token(
        make_doc(R"J({"access_token": "", "expires_in": "42"})J")),
      std::invalid_argument);
}

TEST(oauth_token, invalid_token_type) {
    ASSERT_THROW(
      iceberg::rest_client::parse_oauth_token(make_doc(
        R"J({"access_token": "", "token_type" : "haha", "expires_in": 42})J")),
      std::invalid_argument);
}

TEST(oauth_token, valid_doc_parse) {
    using namespace std::chrono_literals;
    auto token = iceberg::rest_client::parse_oauth_token(make_doc(
      R"J({"access_token": "", "token_type" : "bearer", "expires_in": 42})J"));

    // Assume that it doesn't take more than 2 seconds to get to this assertion
    ASSERT_THAT(
      token.expires_at - ss::lowres_clock::now(), AllOf(Le(42s), Ge(40s)));
}
