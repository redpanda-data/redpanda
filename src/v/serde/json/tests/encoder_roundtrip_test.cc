/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "absl/strings/str_cat.h"
#include "bytes/iobuf.h"
#include "serde/json/parser.h"
#include "serde/json/writer.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <limits>
#include <string_view>

using namespace serde::json;

namespace {

/// Drive the parser through all tokens and re-emit them through the writer,
/// producing a semantically equivalent JSON document. This is the same approach
/// the JSON field transformer will use: parse with serde::json::parser, re-emit
/// through serde::json::writer, optionally substituting values for encrypted
/// fields.
ss::future<iobuf> roundtrip_json(iobuf input) {
    auto p = parser(std::move(input));
    writer w;

    while (co_await p.next()) {
        switch (p.token()) {
        case token::start_object:
            w.begin_object();
            break;
        case token::end_object:
            w.end_object();
            break;
        case token::start_array:
            w.begin_array();
            break;
        case token::end_array:
            w.end_array();
            break;
        case token::key:
            w.key(p.value_string());
            break;
        case token::value_string:
            w.string(p.value_string());
            break;
        case token::value_int: {
            auto val = p.value_int();
            // The writer only has integer(int32_t) and integer(uint32_t).
            // For values outside that range, emit via append_raw_json.
            if (
              val >= std::numeric_limits<int32_t>::min()
              && val <= std::numeric_limits<int32_t>::max()) {
                w.integer(static_cast<int32_t>(val));
            } else {
                w.append_raw_json(
                  iobuf::from(absl::StrCat(absl::AlphaNum(val))));
            }
            break;
        }
        case token::value_double:
            w.number(p.value_double());
            break;
        case token::value_null:
            w.null();
            break;
        case token::value_true:
            w.boolean(true);
            break;
        case token::value_false:
            w.boolean(false);
            break;
        case token::eof:
            break;
        case token::error:
            throw std::runtime_error("roundtrip_json: parser error");
        }
    }

    if (p.token() == token::error) {
        throw std::runtime_error("roundtrip_json: parser error at end");
    }

    co_return std::move(w).finish();
}

std::string iobuf_to_string(iobuf buf) { return buf.linearize_to_string(); }

} // namespace

TEST_CORO(encoder_roundtrip, simple_object) {
    constexpr std::string_view input = R"({"a":"b","c":1})";
    auto result = iobuf_to_string(co_await roundtrip_json(iobuf::from(input)));
    EXPECT_EQ(result, input);
}

TEST_CORO(encoder_roundtrip, nested) {
    constexpr std::string_view input = R"({"outer":{"inner":"value"}})";
    auto result = iobuf_to_string(co_await roundtrip_json(iobuf::from(input)));
    EXPECT_EQ(result, input);
}

TEST_CORO(encoder_roundtrip, array) {
    constexpr std::string_view input = R"({"arr":[1,2,3]})";
    auto result = iobuf_to_string(co_await roundtrip_json(iobuf::from(input)));
    EXPECT_EQ(result, input);
}

TEST_CORO(encoder_roundtrip, null_and_bool) {
    constexpr std::string_view input = R"({"n":null,"b":true,"f":false})";
    auto result = iobuf_to_string(co_await roundtrip_json(iobuf::from(input)));
    EXPECT_EQ(result, input);
}

TEST_CORO(encoder_roundtrip, mixed) {
    // Complex nested object with arrays, objects, strings, numbers, bools,
    // and nulls.
    constexpr std::string_view input
      = R"({"str":"hello","num":42,"flag":true,"nothing":null,)"
        R"("nested":{"a":[1,2,3],"b":{"deep":"val"}},)"
        R"("list":[null,false,true,"x",99,{"k":"v"},[1]]})";
    auto result = iobuf_to_string(co_await roundtrip_json(iobuf::from(input)));
    EXPECT_EQ(result, input);
}

TEST_CORO(encoder_roundtrip, empty_object) {
    constexpr std::string_view input = R"({})";
    auto result = iobuf_to_string(co_await roundtrip_json(iobuf::from(input)));
    EXPECT_EQ(result, input);
}

TEST_CORO(encoder_roundtrip, empty_array) {
    constexpr std::string_view input = R"({"a":[]})";
    auto result = iobuf_to_string(co_await roundtrip_json(iobuf::from(input)));
    EXPECT_EQ(result, input);
}

TEST_CORO(encoder_roundtrip, nested_arrays) {
    constexpr std::string_view input = R"({"a":[[1],[2,3],[]]})";
    auto result = iobuf_to_string(co_await roundtrip_json(iobuf::from(input)));
    EXPECT_EQ(result, input);
}

TEST_CORO(encoder_roundtrip, string_escaping) {
    // The writer escapes strings (e.g. quotes, backslashes, control chars),
    // so we provide input that is already in the writer's canonical escaped
    // form.
    constexpr std::string_view input
      = R"({"msg":"line1\nline2","q":"say \"hi\""})";
    auto result = iobuf_to_string(co_await roundtrip_json(iobuf::from(input)));

    // Parse both through the serde parser and compare the value_string tokens
    // to verify semantic equivalence even if escape forms differ.
    auto parse_first_value = [](std::string_view json) -> ss::future<iobuf> {
        auto p = parser(iobuf::from(json));
        while (co_await p.next()) {
            if (p.token() == token::value_string) {
                co_return p.value_string();
            }
        }
        throw std::runtime_error("no string value found");
    };

    auto orig_val = iobuf_to_string(co_await parse_first_value(input));
    auto rt_val = iobuf_to_string(co_await parse_first_value(result));
    EXPECT_EQ(orig_val, rt_val);
}

TEST_CORO(encoder_roundtrip, negative_integer) {
    constexpr std::string_view input = R"({"val":-42})";
    auto result = iobuf_to_string(co_await roundtrip_json(iobuf::from(input)));
    EXPECT_EQ(result, input);
}
