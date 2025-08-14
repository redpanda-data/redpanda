/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/json/tests/data.h"
#include "serde/json/tests/dom_rapidjson.h"
#include "serde/json/tests/dom_serde.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

using namespace serde::json;

// Parse DOM model using both serde::json and rapidjson
// and compare the results.
TEST_CORO(json_test_suite, conformance) {
    auto doc = co_await test::dom::parse_document_serde(
      co_await json_test_suite_sample());

    ASSERT_TRUE_CORO(
      std::holds_alternative<test::dom::json_object>(doc.data()));
    const auto& obj = std::get<test::dom::json_object>(doc.data());
    ASSERT_EQ_CORO(obj.size(), 3);

    auto rapidjson_doc = co_await test::dom::parse_document_rapidjson(
      co_await json_test_suite_sample());

    ASSERT_TRUE_CORO(
      std::holds_alternative<test::dom::json_object>(rapidjson_doc.data()));
    const auto& robj = std::get<test::dom::json_object>(rapidjson_doc.data());
    ASSERT_EQ_CORO(robj.size(), 3);

    ASSERT_EQ_CORO(rapidjson_doc, doc);
}
