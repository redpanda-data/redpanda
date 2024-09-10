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

#include "test_utils/test.h"
#include "utils/hybrid_clock.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <system_error>

TEST_CORO(hybrid_clock, lowres_clock_mode) {
    auto h_now = hybrid_clock::now();
    ASSERT_FALSE_CORO(hybrid_clock::is_manual(h_now));
    auto l_now = seastar::lowres_clock::now();
    // The clocks could be off by 1ns
    ASSERT_EQ_CORO(
      h_now.time_since_epoch().count() / 2,
      l_now.time_since_epoch().count() / 2);
    co_await seastar::sleep(std::chrono::milliseconds(100));
    h_now = hybrid_clock::now();
    l_now = seastar::lowres_clock::now();
    ASSERT_EQ_CORO(
      h_now.time_since_epoch().count() / 2,
      l_now.time_since_epoch().count() / 2);
    co_return;
}

TEST(hybrid_clock, manual_clock_mode) {
    manual_clock_scope scope;
    auto h_now = hybrid_clock::now();
    ASSERT_TRUE(hybrid_clock::is_manual(h_now));
    auto m_now = seastar::manual_clock::now();
    ASSERT_EQ(
      h_now.time_since_epoch().count() / 2,
      m_now.time_since_epoch().count() / 2);
    hybrid_clock::advance(std::chrono::seconds(10));
    h_now = hybrid_clock::now();
    m_now = seastar::manual_clock::now();
    ASSERT_EQ(
      h_now.time_since_epoch().count() / 2,
      m_now.time_since_epoch().count() / 2);
}

TEST(hybrid_clock, operators_throw) {
    // Check that manual and real clock timestamps can't be mixed in one
    // expression
    auto real = hybrid_clock::now();
    manual_clock_scope scope;
    auto manual = hybrid_clock::now();
    ASSERT_THROW((void)(real < manual), std::logic_error);
    ASSERT_THROW((void)(real > manual), std::logic_error);
    ASSERT_THROW((void)(real <= manual), std::logic_error);
    ASSERT_THROW((void)(real >= manual), std::logic_error);
    ASSERT_THROW((void)(real == manual), std::logic_error);
    ASSERT_THROW((void)(real != manual), std::logic_error);
    ASSERT_THROW((void)(real - manual), std::logic_error);
    // Time shifted hybrid clock shouldn't loose information about its origin
    ASSERT_THROW(
      (void)((manual + std::chrono::seconds(1)) < real), std::logic_error);
    ASSERT_THROW(
      (void)((manual + std::chrono::seconds(1)) < real), std::logic_error);
}

TEST(hybrid_clock, operators_correctness_manual) {
    // Check that operators yield correct results
    manual_clock_scope scope;
    auto lhs = hybrid_clock::now();
    auto lhe = seastar::manual_clock::now();
    hybrid_clock::duration diff(std::chrono::seconds(10));
    hybrid_clock::advance(diff);
    auto rhs = hybrid_clock::now();
    auto rhe = seastar::manual_clock::now();
    ASSERT_TRUE(lhs < rhs);
    ASSERT_TRUE(rhs > lhs);
    ASSERT_EQ((rhs - lhs).count() / 2, (rhe - lhe).count() / 2);
}

TEST_CORO(hybrid_clock, operators_correctness_system) {
    // Check that operators yield correct results
    auto lhs = hybrid_clock::now();
    auto lhe = seastar::lowres_clock::now();
    hybrid_clock::duration diff(std::chrono::milliseconds(100));
    co_await seastar::sleep(diff);
    auto rhs = hybrid_clock::now();
    auto rhe = seastar::lowres_clock::now();
    ASSERT_TRUE_CORO(lhs < rhs);
    ASSERT_TRUE_CORO(rhs > lhs);
    ASSERT_EQ_CORO((rhs - lhs).count() / 2, (rhe - lhe).count() / 2);
}
