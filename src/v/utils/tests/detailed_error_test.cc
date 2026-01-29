// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/detailed_error.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <fmt/core.h>
#include <gtest/gtest.h>

#include <expected>

enum class errc { foo, bar };
enum class other_errc { baz };

template<>
struct fmt::formatter<errc> : fmt::formatter<std::string_view> {
    auto format(errc e, format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", e == errc::foo ? "foo" : "bar");
    }
};

template<>
struct fmt::formatter<other_errc> : fmt::formatter<std::string_view> {
    auto format(other_errc, format_context& ctx) const {
        return fmt::format_to(ctx.out(), "baz");
    }
};

using error = detailed_error<errc>;

TEST(DetailedErrorTest, TestErrorOnly) {
    EXPECT_EQ(fmt::format("{}", error(errc::foo)), "[foo]");
}

TEST(DetailedErrorTest, TestErrorAndDetails) {
    EXPECT_EQ(fmt::format("{}", error(errc::foo, "msg")), "[foo]: msg");
}

TEST(DetailedErrorTest, TestErrorAndFmtDetails) {
    EXPECT_EQ(fmt::format("{}", error(errc::foo, "{} msg", 1)), "[foo]: 1 msg");
}

TEST(DetailedErrorTest, TestWrapFmt) {
    auto inner = error(errc::bar, "inner");
    auto outer = error::wrap(std::move(inner), errc::foo, "{} outer", 1);
    EXPECT_EQ(fmt::format("{}", outer), "[foo]: 1 outer: inner");
}

TEST(DetailedErrorTest, TestWrap) {
    auto inner = error(errc::bar, "inner");
    auto outer = error::wrap(std::move(inner), errc::foo, "outer");
    EXPECT_EQ(fmt::format("{}", outer), "[foo]: outer: inner");
}

TEST(DetailedErrorTest, TestWrapCall) {
    auto inner = error(errc::bar, "inner");
    auto outer = std::move(inner).wrap(errc::foo, "outer");
    EXPECT_EQ(fmt::format("{}", outer), "[foo]: outer: inner");
}

TEST(DetailedErrorTest, TestWrapCallFmt) {
    auto inner = error(errc::bar, "inner");
    auto outer = std::move(inner).wrap(errc::foo, "{} outer", 1);
    EXPECT_EQ(fmt::format("{}", outer), "[foo]: 1 outer: inner");
}

TEST(DetailedErrorTest, TestWrapChain) {
    auto a = error(errc::foo, "a");
    auto b = error::wrap(std::move(a), errc::bar, "b");
    auto c = error::wrap(std::move(b), errc::foo, "c");
    EXPECT_EQ(fmt::format("{}", c), "[foo]: c: b: a");
}

TEST(DetailedErrorTest, TestWrapDifferentTypes) {
    auto inner = detailed_error<other_errc>(other_errc::baz, "inner");
    auto outer = error::wrap(std::move(inner), errc::foo, "outer");
    EXPECT_EQ(fmt::format("{}", outer), "[foo]: outer: inner");
}

ss::future<std::expected<int, error>> make_error() {
    co_return std::unexpected(error(errc::foo, "async"));
}

ss::future<std::expected<int, error>> make_wrapped_error() {
    auto res = co_await make_error();
    co_return std::unexpected(
      error::wrap(std::move(res.error()), errc::bar, "wrapped"));
}

TEST(DetailedErrorTest, TestFunctionError) {
    auto r1 = make_error().get();
    EXPECT_EQ(fmt::format("{}", r1.error()), "[foo]: async");
    auto r2 = make_wrapped_error().get();
    EXPECT_EQ(fmt::format("{}", r2.error()), "[bar]: wrapped: async");
}
