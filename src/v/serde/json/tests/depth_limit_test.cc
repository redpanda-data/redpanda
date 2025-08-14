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

#include "serde/json/parser.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

using namespace serde::json;

namespace {

std::string generate_nested_array(size_t depth) {
    std::string json;
    json.reserve(depth * 2 + 1);

    for (size_t i = 0; i < depth; ++i) {
        json += '[';
    }

    json += "0";

    for (size_t i = 0; i < depth; ++i) {
        json += ']';
    }

    return json;
}

std::string generate_nested_object(size_t depth) {
    std::string json;
    json.reserve(depth * 20 + 20);

    for (size_t i = 0; i < depth; ++i) {
        json += "{\"key" + std::to_string(i) + "\":";
    }

    json += "0";

    for (size_t i = 0; i < depth; ++i) {
        json += '}';
    }

    return json;
}

std::string generate_mixed_nested(size_t depth) {
    std::string json;
    json.reserve(depth * 25 + 20);

    for (size_t i = 0; i < depth; ++i) {
        if (i % 2 == 0) {
            json += "[{\"key\":";
        } else {
            json += "{\"arr\":[";
        }
    }

    json += "0";

    for (size_t i = 0; i < depth; ++i) {
        if (i % 2 == 0) {
            json += "}]";
        } else {
            json += "]}";
        }
    }

    return json;
}

} // anonymous namespace

TEST_CORO(depth_limit_test, default_depth_limit) {
    // Should work exactly at default limit.
    auto json_str = generate_nested_array(parser_config::default_max_depth);
    auto buf = iobuf{};
    buf.append(json_str.data(), json_str.size());

    auto parser = serde::json::parser(std::move(buf));

    size_t token_count = 0;
    while (co_await parser.next()) {
        token_count++;
    }

    EXPECT_EQ(parser.token(), token::eof);
    EXPECT_GT(token_count, 0);
}

TEST_CORO(depth_limit_test, default_depth_limit_exceeded) {
    // Should fail one past the default limit.
    auto json_str = generate_nested_array(parser_config::default_max_depth + 1);
    auto buf = iobuf{};
    buf.append(json_str.data(), json_str.size());

    auto parser = serde::json::parser(std::move(buf));

    while (co_await parser.next()) {
        // Continue parsing until error.
    }

    EXPECT_EQ(parser.token(), token::error);
}

TEST_CORO(depth_limit_test, array_depth_limit_exceeded) {
    const size_t depth_limit = 50;
    auto json_str = generate_nested_array(depth_limit + 1);
    auto buf = iobuf{};
    buf.append(json_str.data(), json_str.size());

    auto config = parser_config{.max_depth = depth_limit};
    auto parser = serde::json::parser(std::move(buf), config);

    while (co_await parser.next()) {
        // Continue parsing until error.
    }

    EXPECT_EQ(parser.token(), token::error);
}

TEST_CORO(depth_limit_test, object_depth_limit_exceeded) {
    const size_t depth_limit = 50;
    auto json_str = generate_nested_object(depth_limit + 1);
    auto buf = iobuf{};
    buf.append(json_str.data(), json_str.size());

    auto config = parser_config{.max_depth = depth_limit};
    auto parser = serde::json::parser(std::move(buf), config);

    while (co_await parser.next()) {
        // Continue parsing until error.
    }

    EXPECT_EQ(parser.token(), token::error);
}

TEST_CORO(depth_limit_test, mixed_depth_limit_exceeded) {
    const size_t depth_limit = 50;
    auto json_str = generate_mixed_nested(depth_limit + 1);
    auto buf = iobuf{};
    buf.append(json_str.data(), json_str.size());

    auto config = parser_config{.max_depth = depth_limit};
    auto parser = serde::json::parser(std::move(buf), config);

    while (co_await parser.next()) {
        // Continue parsing until error.
    }

    EXPECT_EQ(parser.token(), token::error);
}

TEST_CORO(depth_limit_test, zero_depth_limit) {
    const size_t depth_limit = 0;
    auto json_str = std::string{"[0]"};
    auto buf = iobuf{};
    buf.append(json_str.data(), json_str.size());

    auto config = parser_config{.max_depth = depth_limit};
    auto parser = serde::json::parser(std::move(buf), config);

    while (co_await parser.next()) {
        // Continue parsing until error.
    }

    EXPECT_EQ(parser.token(), token::error);
}
