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

#include "gmock/gmock.h"
#include "serde/json/parser.h"
#include "serde/json/tests/data.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

using namespace testing;
using namespace serde::json;

constexpr std::string_view json_checker_pass1 = R"([
    "JSON Test Pattern pass1",
    {"object with 1 member":["array with 1 element"]},
    {},
    [],
    -42,
    true,
    false,
    null,
    {
        "integer": 1234567890,
        "real": -9876.543210,
        "e": 0.123456789e-12,
        "E": 1.234567890E+34,
        "":  23456789012E66,
        "zero": 0,
        "one": 1,
        "space": " ",
        "quote": "\"",
        "backslash": "\\",
        "controls": "\b\f\n\r\t",
        "slash": "/ & \/",
        "alpha": "abcdefghijklmnopqrstuvwyz",
        "ALPHA": "ABCDEFGHIJKLMNOPQRSTUVWYZ",
        "digit": "0123456789",
        "0123456789": "digit",
        "special": "`1~!@#$%^&*()_+-={':[,]}|;.</>?",
        "hex": "\u0123\u4567\u89AB\uCDEF\uabcd\uef4A",
        "true": true,
        "false": false,
        "null": null,
        "array":[  ],
        "object":{  },
        "address": "50 St. James Street",
        "url": "http://www.JSON.org/",
        "comment": "// /* <!-- --",
        "# -- --> */": " ",
        " s p a c e d " :[1,2 , 3

,

4 , 5        ,          6           ,7        ],"compact":[1,2,3,4,5,6,7],
        "jsontext": "{\"object with 1 member\":[\"array with 1 element\"]}",
        "quotes": "&#34; \u0022 %22 0x22 034 &#x22;",
        "\/\\\"\uCAFE\uBABE\uAB98\uFCDE\ubcda\uef4A\b\f\n\r\t`1~!@#$%^&*()_+-=[]{}|;:',./<>?"
: "A key can be any string"
    },
    0.5 ,98.6
,
99.44
,

1066,
1e1,
0.1e1,
1e-1,
1e00,2e+00,2e-00
,"rosebud"])";

// Simple test to ensure the parse doesn't fail on valid sample data. The
// contents and correctness is not verified in this test.
TEST_CORO(json_test_suite, parse) {
    auto parser = serde::json::parser(co_await json_test_suite_sample());

    while (co_await parser.next()) {
        // Do nothing, just drain the parser.
        // The contents and correctness is not verified in this test.
    }

    EXPECT_EQ(parser.token(), token::eof) << "Expected to reach EOF but got: "
                                          << std::to_underlying(parser.token());
}

ss::future<> skip_tokens(parser& p, const std::vector<token>& tokens) {
    for (const auto& t : tokens) {
        ASSERT_TRUE_CORO(co_await p.next())
          << "Expected next() to return true for token: " << t;
        ASSERT_EQ_CORO(p.token(), t) << "Unexpected token, expected: " << t;
    }
}

struct token_seq_test_case {
    std::string_view input;
    std::vector<token> expected_tokens;
};

ss::future<> run_test_case(const token_seq_test_case& tc) {
    auto parser = serde::json::parser(iobuf::from(tc.input));
    ASSERT_NO_FATAL_FAILURE_CORO(
      co_await skip_tokens(parser, tc.expected_tokens));
    ASSERT_FALSE_CORO(co_await parser.next())
      << "Expected next() to return false after all tokens for input: "
      << tc.input;
}

TEST_CORO(json_parser, parse_empty) {
    constexpr auto empty_documents = std::to_array<std::string_view>(
      {"", " ", "\n", "\t", "\r\n"});

    for (const auto& doc : empty_documents) {
        SCOPED_TRACE(fmt::format("Testing empty document: {}", doc));
        ASSERT_NO_FATAL_FAILURE_CORO(co_await run_test_case({
          .input = doc,
          .expected_tokens = {token::error},
        }));
    }
};

TEST_CORO(json_parser, leading_trailing_whitespace) {
    ASSERT_NO_FATAL_FAILURE_CORO(co_await run_test_case({
        .input = "   [   ]   ",
        .expected_tokens = {
            token::start_array,
            token::end_array,
            token::eof,
        },
    }));
}

TEST_CORO(json_parser, truncated_json_always_errors) {
    for (size_t i = 0; i <= json_checker_pass1.size(); i++) {
        SCOPED_TRACE(fmt::format(
          "Testing truncated JSON at position: {} out of {}",
          i,
          json_checker_pass1.size()));

        auto p = serde::json::parser(
          iobuf::from(json_checker_pass1.substr(0, i)));

        while (co_await p.next()) {
            // Consume tokens.
        }

        bool should_error = i < json_checker_pass1.size();

        ASSERT_EQ_CORO(p.token(), should_error ? token::error : token::eof);
    }
}

// Common input for skip_value tests
constexpr std::string_view skip_value_input = R"({
  "foo": {"a": 1, "b": [{"c": true, "d": null}]},
  "bar": true
})";

TEST_CORO(json_parser, skip_value_whole_document) {
    auto p = serde::json::parser(iobuf::from(skip_value_input));
    ASSERT_TRUE_CORO(co_await p.next());
    ASSERT_EQ_CORO(p.token(), token::start_object);
    co_await p.skip_value();

    ASSERT_EQ_CORO(p.token(), token::end_object);

    SCOPED_TRACE("After skipping whole document");
    ASSERT_NO_FATAL_FAILURE_CORO(co_await skip_tokens(
      p,
      {
        token::eof,
      }));
}

TEST_CORO(json_parser, skip_value_whole_without_next_call) {
    auto p = serde::json::parser(iobuf::from(skip_value_input));
    co_await p.skip_value();

    SCOPED_TRACE("After skipping whole document");
    ASSERT_NO_FATAL_FAILURE_CORO(co_await skip_tokens(
      p,
      {
        token::eof,
      }));
}

TEST_CORO(json_parser, skip_value_whole_without_next_call_bad_input) {
    auto p = serde::json::parser(iobuf::from(R"({"bad", ["input)"));
    co_await p.skip_value();
    ASSERT_EQ_CORO(p.token(), token::error);
}

TEST_CORO(json_parser, skip_value_whole_without_next_call_bad_input_2) {
    auto p = serde::json::parser(iobuf::from(R"(["bad input])"));
    co_await p.skip_value();
    ASSERT_EQ_CORO(p.token(), token::error);
}

TEST_CORO(json_parser, skip_value_whole_without_next_call_bad_input_3) {
    auto p = serde::json::parser(iobuf::from(R"({"key"})"));

    ASSERT_NO_FATAL_FAILURE_CORO(co_await skip_tokens(
      p,
      {
        token::start_object,
        token::key,
      }));

    co_await p.skip_value();

    ASSERT_EQ_CORO(p.token(), token::error);
}

TEST_CORO(json_parser, skip_value_key_with_value) {
    auto p = serde::json::parser(iobuf::from(skip_value_input));

    ASSERT_NO_FATAL_FAILURE_CORO(co_await skip_tokens(
      p,
      {
        token::start_object,
        token::key,
      }));

    // Skip over the value of "foo".
    co_await p.skip_value();

    ASSERT_EQ_CORO(p.token(), token::end_object);
    ASSERT_TRUE_CORO(co_await p.next());

    ASSERT_EQ_CORO(p.token(), token::key);
    ASSERT_EQ_CORO(p.value_string(), iobuf::from("bar"));
}

TEST_CORO(json_parser, skip_value_object_value) {
    auto p = serde::json::parser(iobuf::from(skip_value_input));

    ASSERT_NO_FATAL_FAILURE_CORO(co_await skip_tokens(
      p,
      {
        token::start_object,
        token::key,
        token::start_object,
      }));

    // Skip over just the object value.
    co_await p.skip_value();

    ASSERT_EQ_CORO(p.token(), token::end_object);
    ASSERT_TRUE_CORO(co_await p.next());

    ASSERT_EQ_CORO(p.token(), token::key);
    ASSERT_EQ_CORO(p.value_string(), iobuf::from("bar"));
}

TEST_CORO(json_parser, skip_value_primitive_key_value) {
    auto p = serde::json::parser(iobuf::from(skip_value_input));

    ASSERT_NO_FATAL_FAILURE_CORO(co_await skip_tokens(
      p,
      {
        token::start_object,
        token::key,
        token::start_object,
        token::key,
      }));

    co_await p.skip_value();
    ASSERT_EQ_CORO(p.token(), token::value_int);
    ASSERT_TRUE_CORO(co_await p.next());

    ASSERT_EQ_CORO(p.token(), token::key);
    ASSERT_EQ_CORO(p.value_string(), iobuf::from("b"));
}

TEST_CORO(json_parser, skip_value_primitive_value) {
    auto p = serde::json::parser(iobuf::from(skip_value_input));

    ASSERT_NO_FATAL_FAILURE_CORO(co_await skip_tokens(
      p,
      {
        token::start_object,
        token::key,
        token::start_object,
        token::key,
        token::value_int,
      }));

    co_await p.skip_value();
    ASSERT_EQ_CORO(p.token(), token::value_int);
    ASSERT_TRUE_CORO(co_await p.next());

    ASSERT_EQ_CORO(p.token(), token::key);
    ASSERT_EQ_CORO(p.value_string(), iobuf::from("b"));
}

TEST_CORO(json_parser, skip_value_entire_array) {
    auto p = serde::json::parser(iobuf::from(R"(
      [1, 2, [[]], [1, {"a": 1, "b": 2}]]
    )"));

    ASSERT_TRUE_CORO(co_await p.next());
    ASSERT_EQ_CORO(p.token(), token::start_array);
    co_await p.skip_value();
    ASSERT_EQ_CORO(p.token(), token::end_array);
    SCOPED_TRACE("After skipping entire array");
    ASSERT_NO_FATAL_FAILURE_CORO(co_await skip_tokens(
      p,
      {
        token::eof,
      }));
}

TEST_CORO(json_parser, skip_value_nested_array) {
    auto p = serde::json::parser(iobuf::from(R"(
      [1, 2, [[[]]], [1, {"a": 1, "b": 2}]]
    )"));

    SCOPED_TRACE("Skip nested array");
    ASSERT_NO_FATAL_FAILURE_CORO(co_await skip_tokens(
      p,
      {
        token::start_array,
        token::value_int,
        token::value_int,
        token::start_array,
        token::start_array,
      }));

    SCOPED_TRACE("Skip nested array value");
    co_await p.skip_value();

    ASSERT_EQ_CORO(p.token(), token::end_array);

    SCOPED_TRACE("After skipping nested array value");
    ASSERT_NO_FATAL_FAILURE_CORO(co_await skip_tokens(
      p,
      {
        token::end_array,
        token::start_array,
        token::value_int,
      }));
}

TEST_CORO(json_parser, skip_value_bad_precondition) {
    auto p = serde::json::parser(iobuf::from("[]"));

    ASSERT_NO_FATAL_FAILURE_CORO(co_await skip_tokens(
      p,
      {
        token::start_array,
        token::end_array,
      }));

    co_await ss::async([&] {
        EXPECT_THAT(
          [&]() { p.skip_value().get(); },
          ThrowsMessage<std::runtime_error>(
            StrEq("skip_value called with unexpected token: end_array")));
    });
}

TEST_CORO(json_parser, invalid_array_missing_comma) {
    auto p = parser(iobuf::from(R"([42.1"zz"])"));

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::start_array);

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::value_double);

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::error);
}

TEST_CORO(json_parser, invalid_object_missing_comma) {
    auto p = parser(iobuf::from(R"({"a": 1 "b": 2})"));

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::start_object);

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::key);

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::value_int);

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::error);
}

TEST_CORO(json_parser, top_level_string) {
    auto p = parser(iobuf::from(R"("A simple string at top level")"));

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::value_string);
    EXPECT_EQ(p.value_string(), iobuf::from("A simple string at top level"));

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::eof);
}

TEST_CORO(json_parser, top_level_integer) {
    auto p = parser(iobuf::from("42"));

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::value_int);
    EXPECT_EQ(p.value_int(), 42);

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::eof);
}

TEST_CORO(json_parser, top_level_double) {
    auto p = parser(iobuf::from("3.14159"));

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::value_double);
    EXPECT_DOUBLE_EQ(p.value_double(), 3.14159);

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::eof);
}

TEST_CORO(json_parser, top_level_true) {
    auto p = parser(iobuf::from("true"));

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::value_true);

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::eof);
}

TEST_CORO(json_parser, top_level_false) {
    auto p = parser(iobuf::from("false"));

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::value_false);

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::eof);
}

TEST_CORO(json_parser, top_level_null) {
    auto p = parser(iobuf::from("null"));

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::value_null);

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::eof);
}

TEST_CORO(json_parser, top_level_extra_number) {
    auto p = parser(iobuf::from(R"("hello"42)"));

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::value_string);
    EXPECT_EQ(p.value_string(), iobuf::from("hello"));

    EXPECT_TRUE(co_await p.next());
    EXPECT_EQ(p.token(), token::error);
}
