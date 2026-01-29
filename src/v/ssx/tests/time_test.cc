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

#include "ssx/time.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <limits>

namespace ssx {

constexpr int64_t int64_max = std::numeric_limits<int64_t>::max();
constexpr int64_t int64_min = std::numeric_limits<int64_t>::min();

// ============================================================================
// Duration Tests
// ============================================================================

TEST(Duration, Construction) {
    // Default construction
    constexpr duration d1;
    EXPECT_EQ(d1.to_nanoseconds(), 0);

    // From nanoseconds
    auto d2 = duration::nanoseconds(1000);
    EXPECT_EQ(d2.to_nanoseconds(), 1000);

    // Static factory methods
    constexpr auto zero = duration::zero();
    constexpr auto inf = duration::infinite();
    EXPECT_EQ(zero.to_nanoseconds(), 0);
    EXPECT_EQ(inf.to_nanoseconds(), int64_max);
    EXPECT_TRUE(inf.is_infinite());
}

TEST(Duration, Addition) {
    EXPECT_EQ(
      (duration::nanoseconds(100) + duration::nanoseconds(200))
        .to_nanoseconds(),
      300);
    EXPECT_EQ(
      (duration::nanoseconds(-100) + duration::nanoseconds(-200))
        .to_nanoseconds(),
      -300);
    EXPECT_EQ(
      (duration::nanoseconds(100) + duration::nanoseconds(-50))
        .to_nanoseconds(),
      50);

    // Saturating overflow
    auto d1 = duration::nanoseconds(int64_max - 10);
    auto d2 = duration::nanoseconds(20);
    auto result = d1 + d2;
    EXPECT_EQ(result.to_nanoseconds(), int64_max);
    EXPECT_EQ(
      result.to_chrono<std::chrono::nanoseconds>(),
      std::chrono::nanoseconds::max());
    EXPECT_LT(
      result.to_chrono<std::chrono::microseconds>(),
      std::chrono::microseconds::max());
}

TEST(Duration, Subtraction) {
    EXPECT_EQ(
      (duration::nanoseconds(200) - duration::nanoseconds(100))
        .to_nanoseconds(),
      100);
    EXPECT_EQ(
      (duration::nanoseconds(-100) - duration::nanoseconds(-200))
        .to_nanoseconds(),
      100);

    // Saturating underflow
    auto d1 = duration::nanoseconds(int64_min + 10);
    auto d2 = duration::nanoseconds(20);
    auto result = d1 - d2;
    EXPECT_EQ(result.to_nanoseconds(), int64_min);
    EXPECT_EQ(
      result.to_chrono<std::chrono::nanoseconds>(),
      std::chrono::nanoseconds::min());
    EXPECT_GT(
      result.to_chrono<std::chrono::microseconds>(),
      std::chrono::microseconds::min());
}

TEST(Duration, Multiplication) {
    EXPECT_EQ((duration::nanoseconds(100) * 3).to_nanoseconds(), 300);
    EXPECT_EQ((duration::nanoseconds(100) * -2).to_nanoseconds(), -200);
    EXPECT_EQ(
      (3 * duration::nanoseconds(100)).to_nanoseconds(), 300); // Commutative

    // Saturating overflow
    auto d = duration::nanoseconds(int64_max / 2);
    auto result = d * 3;
    EXPECT_EQ(
      result.to_chrono<std::chrono::nanoseconds>(),
      std::chrono::nanoseconds::max());
}

TEST(Duration, Division) {
    EXPECT_EQ((duration::nanoseconds(300) / 3).to_nanoseconds(), 100);
    EXPECT_EQ((duration::nanoseconds(100) / -2).to_nanoseconds(), -50);

    // Saturating division: INT64_MIN / -1 should saturate to INT64_MAX
    auto d_min = duration::nanoseconds(int64_min);
    auto result = d_min / -1;
    EXPECT_EQ(result.to_nanoseconds(), int64_max);
}

TEST(Duration, Negation) {
    EXPECT_EQ((-duration::nanoseconds(100)).to_nanoseconds(), -100);
    EXPECT_EQ((-duration::nanoseconds(-100)).to_nanoseconds(), 100);

    // Negating INT64_MIN saturates to MAX
    auto d_min = duration::nanoseconds(int64_min);
    auto neg = -d_min;
    EXPECT_EQ(neg.to_nanoseconds(), int64_max);

    // Negating infinite saturates to MIN
    auto d_inf = duration::infinite();
    auto neg_inf = -d_inf;
    EXPECT_TRUE(neg_inf.to_nanoseconds() == int64_min || neg_inf.is_infinite());
}

TEST(Duration, CompoundAssignment) {
    duration d = duration::nanoseconds(100);
    d += duration::nanoseconds(50);
    EXPECT_EQ(d.to_nanoseconds(), 150);

    d -= duration::nanoseconds(25);
    EXPECT_EQ(d.to_nanoseconds(), 125);

    d *= 2;
    EXPECT_EQ(d.to_nanoseconds(), 250);

    d /= 5;
    EXPECT_EQ(d.to_nanoseconds(), 50);

    // Saturating division assignment
    duration d_min = duration::nanoseconds(int64_min);
    d_min /= -1;
    EXPECT_EQ(d_min.to_nanoseconds(), int64_max);
}

TEST(Duration, Comparison) {
    EXPECT_TRUE(duration::nanoseconds(100) == duration::nanoseconds(100));
    EXPECT_FALSE(duration::nanoseconds(100) == duration::nanoseconds(200));

    EXPECT_TRUE(duration::nanoseconds(100) < duration::nanoseconds(200));
    EXPECT_TRUE(duration::nanoseconds(100) <= duration::nanoseconds(100));
    EXPECT_TRUE(duration::nanoseconds(200) > duration::nanoseconds(100));
    EXPECT_TRUE(duration::nanoseconds(200) >= duration::nanoseconds(200));
}

TEST(Duration, SpecialValues) {
    auto inf = duration::infinite();
    auto zero = duration::zero();

    // Infinite + anything = infinite
    EXPECT_EQ(inf + duration::nanoseconds(1000), inf);
    EXPECT_EQ(inf + zero, inf);

    // Infinite is the maximum
    EXPECT_TRUE(inf > duration::nanoseconds(int64_max - 1));
}

// ============================================================================
// Instant Tests
// ============================================================================

TEST(Instant, Construction) {
    // Default construction (epoch)
    constexpr instant i1;
    EXPECT_EQ(i1, instant());

    // Static factory methods
    constexpr auto future = instant::infinite_future();
    constexpr auto past = instant::infinite_past();
    EXPECT_TRUE(future.is_infinite_future());
    EXPECT_TRUE(past.is_infinite_past());
}

TEST(Instant, AddDuration) {
    auto i = instant() + duration::nanoseconds(1000);
    auto d = duration::nanoseconds(500);
    auto result = i + d;
    auto expected = instant() + duration::nanoseconds(1500);
    EXPECT_EQ(result, expected);

    // Adding to infinite_future stays at infinite_future
    auto i_max = instant::infinite_future();
    auto result2 = i_max + duration::nanoseconds(20);
    EXPECT_EQ(result2, instant::infinite_future());
}

TEST(Instant, SubtractDuration) {
    auto i = instant() + duration::nanoseconds(1000);
    auto d = duration::nanoseconds(300);
    auto result = i - d;
    auto expected = instant() + duration::nanoseconds(700);
    EXPECT_EQ(result, expected);

    // Subtracting from infinite_past stays at infinite_past
    auto i_min = instant::infinite_past();
    auto result2 = i_min - duration::nanoseconds(20);
    EXPECT_EQ(result2, instant::infinite_past());
}

TEST(Instant, SubtractInstants) {
    auto i1 = instant() + duration::nanoseconds(2000);
    auto i2 = instant() + duration::nanoseconds(500);
    auto elapsed = i1 - i2;
    EXPECT_EQ(elapsed.to_nanoseconds(), 1500);

    // Saturating when result would overflow
    auto max_i = instant::infinite_future();
    auto min_i = instant::infinite_past();
    auto huge_duration = max_i - min_i;
    EXPECT_TRUE(huge_duration.is_infinite()); // Saturated
}

TEST(Instant, CompoundAssignment) {
    instant i = instant() + duration::nanoseconds(1000);
    i += duration::nanoseconds(500);
    EXPECT_EQ(i, instant() + duration::nanoseconds(1500));

    i -= duration::nanoseconds(300);
    EXPECT_EQ(i, instant() + duration::nanoseconds(1200));
}

TEST(Instant, Comparison) {
    auto i100 = instant() + duration::nanoseconds(100);
    auto i200 = instant() + duration::nanoseconds(200);

    EXPECT_TRUE(i100 == i100);
    EXPECT_FALSE(i100 == i200);

    EXPECT_TRUE(i100 < i200);
    EXPECT_TRUE(i100 <= i100);
    EXPECT_TRUE(i200 > i100);
    EXPECT_TRUE(i200 >= i200);
}

TEST(Instant, SpecialValues) {
    auto future = instant::infinite_future();
    auto past = instant::infinite_past();

    // Future + duration = still future (saturated)
    EXPECT_EQ(future + duration::nanoseconds(1000), future);

    // Past - duration = still past (saturated)
    EXPECT_EQ(past - duration::nanoseconds(1000), past);

    // Future is maximum, past is minimum
    auto almost_max = instant() + duration::nanoseconds(int64_max - 1);
    auto almost_min = instant() + duration::nanoseconds(int64_min + 1);
    EXPECT_TRUE(future > almost_max);
    EXPECT_TRUE(past < almost_min);
}

// ============================================================================
// Wall Time Tests
// ============================================================================

TEST(WallTime, Construction) {
    // Default construction (epoch)
    constexpr wall_time wt1;
    EXPECT_EQ(wt1, wall_time());

    // Static factory methods
    constexpr auto future = wall_time::infinite_future();
    constexpr auto past = wall_time::infinite_past();
    EXPECT_TRUE(future.is_infinite_future());
    EXPECT_TRUE(past.is_infinite_past());
}

TEST(WallTime, AddDuration) {
    auto wt = wall_time() + duration::nanoseconds(1000);
    auto d = duration::nanoseconds(500);
    auto result = wt + d;
    auto expected = wall_time() + duration::nanoseconds(1500);
    EXPECT_EQ(result, expected);

    // Adding to infinite_future stays at infinite_future
    auto wt_max = wall_time::infinite_future();
    auto result2 = wt_max + duration::nanoseconds(20);
    EXPECT_EQ(result2, wall_time::infinite_future());
}

TEST(WallTime, SubtractDuration) {
    auto wt = wall_time() + duration::nanoseconds(1000);
    auto d = duration::nanoseconds(300);
    auto result = wt - d;
    auto expected = wall_time() + duration::nanoseconds(700);
    EXPECT_EQ(result, expected);

    // Subtracting from infinite_past stays at infinite_past
    auto wt_min = wall_time::infinite_past();
    auto result2 = wt_min - duration::nanoseconds(20);
    EXPECT_EQ(result2, wall_time::infinite_past());
}

TEST(WallTime, SubtractWallTimes) {
    auto wt1 = wall_time() + duration::nanoseconds(2000);
    auto wt2 = wall_time() + duration::nanoseconds(500);
    auto elapsed = wt1 - wt2;
    EXPECT_EQ(elapsed.to_nanoseconds(), 1500);

    // Saturating when result would overflow
    auto max_wt = wall_time::infinite_future();
    auto min_wt = wall_time::infinite_past();
    auto huge_duration = max_wt - min_wt;
    EXPECT_TRUE(huge_duration.is_infinite()); // Saturated
}

TEST(WallTime, CompoundAssignment) {
    wall_time wt = wall_time() + duration::nanoseconds(1000);
    wt += duration::nanoseconds(500);
    EXPECT_EQ(wt, wall_time() + duration::nanoseconds(1500));

    wt -= duration::nanoseconds(300);
    EXPECT_EQ(wt, wall_time() + duration::nanoseconds(1200));
}

TEST(WallTime, Comparison) {
    auto wt100 = wall_time() + duration::nanoseconds(100);
    auto wt200 = wall_time() + duration::nanoseconds(200);

    EXPECT_TRUE(wt100 == wt100);
    EXPECT_FALSE(wt100 == wt200);

    EXPECT_TRUE(wt100 < wt200);
    EXPECT_TRUE(wt100 <= wt100);
    EXPECT_TRUE(wt200 > wt100);
    EXPECT_TRUE(wt200 >= wt200);
}

TEST(WallTime, SpecialValues) {
    auto future = wall_time::infinite_future();
    auto past = wall_time::infinite_past();

    // Future + duration = still future (saturated)
    EXPECT_EQ(future + duration::nanoseconds(1000), future);

    // Past - duration = still past (saturated)
    EXPECT_EQ(past - duration::nanoseconds(1000), past);

    // Future is maximum, past is minimum
    auto almost_max = wall_time() + duration::nanoseconds(int64_max - 1);
    auto almost_min = wall_time() + duration::nanoseconds(int64_min + 1);
    EXPECT_TRUE(future > almost_max);
    EXPECT_TRUE(past < almost_min);
}

} // namespace ssx
