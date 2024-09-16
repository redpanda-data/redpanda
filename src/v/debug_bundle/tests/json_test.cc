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

#include "debug_bundle/json.h"
#include "debug_bundle/types.h"
#include "json/document.h"

#include <gtest/gtest-typed-test.h>
#include <gtest/gtest.h>
#include <rapidjson/error/en.h>

#include <type_traits>

using namespace debug_bundle;

template<typename T>
class JsonTypeTest : public testing::Test {
public:
    ss::sstring json_input;
    T expected;
};

using JsonTestTypes = ::testing::Types<int>;
TYPED_TEST_SUITE(JsonTypeTest, JsonTestTypes);

TYPED_TEST(JsonTypeTest, BasicType) {
    if constexpr (std::is_same_v<TypeParam, int>) {
        this->json_input = R"(42)";
        this->expected = 42;
    } else {
        static_assert(always_false_v<TypeParam>, "not implemented");
    }

    json::Document doc;
    ASSERT_NO_THROW(doc.Parse(this->json_input));
    ASSERT_FALSE(doc.HasParseError()) << fmt::format(
      "Malformed json schema: {} at offset {}",
      rapidjson::GetParseError_En(doc.GetParseError()),
      doc.GetErrorOffset());

    debug_bundle::result<TypeParam> res{outcome::success()};

    ASSERT_NO_THROW(res = from_json<TypeParam>(doc));
    ASSERT_TRUE(res.has_value()) << res.assume_error().message();
    ASSERT_EQ(res.assume_value(), this->expected);
}

TYPED_TEST(JsonTypeTest, TypeIsInvalid) {
    if constexpr (std::is_same_v<TypeParam, int>) {
        this->json_input = R"("42")";
        this->expected = 42;
    } else {
        static_assert(always_false_v<TypeParam>, "not implemented");
    }

    json::Document doc;
    ASSERT_NO_THROW(doc.Parse(this->json_input));
    ASSERT_FALSE(doc.HasParseError()) << fmt::format(
      "Malformed json schema: {} at offset {}",
      rapidjson::GetParseError_En(doc.GetParseError()),
      doc.GetErrorOffset());

    debug_bundle::result<TypeParam> res{outcome::success()};

    ASSERT_NO_THROW(res = from_json<TypeParam>(doc));
    ASSERT_TRUE(res.has_error());
    ASSERT_FALSE(res.has_exception());
    EXPECT_EQ(res.assume_error().code(), error_code::invalid_parameters);
    EXPECT_TRUE(res.assume_error().message().starts_with("Failed to parse"))
      << res.assume_error().message();
}
