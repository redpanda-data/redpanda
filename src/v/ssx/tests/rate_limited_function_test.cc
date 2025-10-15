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

#include "config/property.h"
#include "ssx/rate_limited_function.h"

#include <seastar/core/manual_clock.hh>
#include <seastar/util/noncopyable_function.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace ssx {

using namespace std::chrono_literals;

TEST(Event, BasicUsage) {
    rate_limited_function<size_t(), seastar::manual_clock> fn(
      [] { return 1; }, 1s);
    EXPECT_EQ(fn(), 1);
}

TEST(Event, Expire) {
    rate_limited_function<
      seastar::manual_clock::time_point(),
      seastar::manual_clock>
      fn([] { return seastar::manual_clock::now(); }, 1s);

    auto baseline = seastar::manual_clock::now();
    EXPECT_TRUE(fn() == baseline);

    seastar::manual_clock::advance(100ms);
    EXPECT_FALSE(seastar::manual_clock::now() == baseline);
    EXPECT_TRUE(fn() == baseline);

    // Should expire
    seastar::manual_clock::advance(1s);
    baseline = seastar::manual_clock::now();
    EXPECT_TRUE(fn() == baseline);
}

TEST(Event, MockBinding) {
    auto interval = config::mock_binding(std::chrono::seconds(1));
    rate_limited_function<
      seastar::manual_clock::time_point(),
      seastar::manual_clock,
      decltype(interval)>
      fn([] { return seastar::manual_clock::now(); }, interval);

    auto baseline = seastar::manual_clock::now();
    EXPECT_TRUE(fn() == baseline);

    seastar::manual_clock::advance(100ms);
    EXPECT_FALSE(seastar::manual_clock::now() == baseline);
    EXPECT_TRUE(fn() == baseline);

    // Should expire
    seastar::manual_clock::advance(1s);
    baseline = seastar::manual_clock::now();
    EXPECT_TRUE(fn() == baseline);
}

TEST(Event, EmptyOptional) {
    auto interval = std::optional<std::chrono::seconds>{std::nullopt};
    rate_limited_function<
      seastar::manual_clock::time_point(),
      seastar::manual_clock,
      decltype(interval)>
      fn([] { return seastar::manual_clock::now(); }, interval);

    auto baseline = seastar::manual_clock::now();
    EXPECT_TRUE(fn() == baseline);

    seastar::manual_clock::advance(100ms);
    EXPECT_FALSE(seastar::manual_clock::now() == baseline);
    EXPECT_TRUE(fn() == baseline);

    // Shouldn't expire
    seastar::manual_clock::advance(1s);
    EXPECT_TRUE(fn() == baseline);
}

TEST(Event, EngagedOptional) {
    auto interval = std::optional<std::chrono::seconds>{1s};
    rate_limited_function<
      seastar::manual_clock::time_point(),
      seastar::manual_clock,
      decltype(interval)>
      fn([] { return seastar::manual_clock::now(); }, interval);

    auto baseline = seastar::manual_clock::now();
    EXPECT_TRUE(fn() == baseline);

    seastar::manual_clock::advance(100ms);
    EXPECT_FALSE(seastar::manual_clock::now() == baseline);
    EXPECT_TRUE(fn() == baseline);

    // Should expire
    seastar::manual_clock::advance(1s);
    baseline = seastar::manual_clock::now();
    EXPECT_TRUE(fn() == baseline);
}

TEST(Event, MockBindingWithOptional) {
    auto interval = config::mock_binding(
      std::optional<std::chrono::seconds>(1s));
    rate_limited_function<
      seastar::manual_clock::time_point(),
      seastar::manual_clock,
      decltype(interval)>
      fn([] { return seastar::manual_clock::now(); }, interval);

    auto baseline = seastar::manual_clock::now();
    EXPECT_TRUE(fn() == baseline);

    seastar::manual_clock::advance(100ms);
    EXPECT_FALSE(seastar::manual_clock::now() == baseline);
    EXPECT_TRUE(fn() == baseline);

    // Should expire
    seastar::manual_clock::advance(1s);
    baseline = seastar::manual_clock::now();
    EXPECT_TRUE(fn() == baseline);
}

} // namespace ssx
