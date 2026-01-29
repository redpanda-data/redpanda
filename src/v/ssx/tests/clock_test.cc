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

#include "ssx/clock.h"
#include "ssx/time.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>

#include <gtest/gtest.h>

#include <chrono>

namespace ssx {

using namespace std::chrono_literals;

TEST(ClockSingleton, SteadyClock) {
    auto& clock1 = hires_steady_clock();
    auto& clock2 = hires_steady_clock();

    // Should return the same instance (singleton)
    EXPECT_EQ(&clock1, &clock2);

    // Should return valid instants
    auto now = clock1.now();
    EXPECT_GT(
      now.to_chrono<std::chrono::steady_clock>().time_since_epoch().count(), 0);

    // Clock should have a name
    EXPECT_NE(clock1.name(), nullptr);
}

TEST(ClockSingleton, WallClock) {
    auto& clock1 = lowres_wall_clock();
    auto& clock2 = lowres_wall_clock();

    // Should return the same instance (singleton)
    EXPECT_EQ(&clock1, &clock2);

    // Should return valid wall times
    auto now = clock1.now();
    // Wall clock should be reasonably close to current time (not zero)
    EXPECT_GT(
      now.to_chrono<std::chrono::system_clock>().time_since_epoch().count(), 0);

    // Clock should have a name
    EXPECT_NE(clock1.name(), nullptr);
}

TEST(ClockSingleton, ManualWallClock) {
    auto& clock1 = manual_wall_clock();
    auto& clock2 = manual_wall_clock();

    // Should return the same instance (singleton)
    EXPECT_EQ(&clock1, &clock2);

    // Should support polymorphism
    wall_clock& clock_ref = clock1;
    auto now = clock_ref.now();
    EXPECT_EQ(
      now.to_chrono<std::chrono::system_clock>().time_since_epoch().count(), 0);
}

TEST(ClockSingleton, ManualSteadyClock) {
    auto& clock1 = manual_steady_clock();
    auto& clock2 = manual_steady_clock();

    // Should return the same instance (singleton)
    EXPECT_EQ(&clock1, &clock2);

    // Should support polymorphism
    steady_clock& clock_ref = clock1;
    auto now = clock_ref.now();
    EXPECT_EQ(
      now.to_chrono<std::chrono::steady_clock>().time_since_epoch().count(), 0);
}

TEST(ManualClock, TimeAdvances) {
    auto& clock = manual_wall_clock();

    auto t1 = clock.now();
    seastar::manual_clock::advance(100ms);
    auto t2 = clock.now();

    auto elapsed = t2 - t1;
    EXPECT_GE(elapsed.to_nanoseconds(), 100'000'000); // At least 100ms
}

TEST(ManualClock, SteadyClockAdvances) {
    auto& clock = manual_steady_clock();

    auto t1 = clock.now();
    seastar::manual_clock::advance(50ms);
    auto t2 = clock.now();

    auto elapsed = t2 - t1;
    EXPECT_GE(elapsed.to_nanoseconds(), 50'000'000); // At least 50ms
}

TEST(ClockPolymorphism, WallClockReference) {
    wall_clock& clock_ref = manual_wall_clock();

    // Should work through base class reference
    auto now1 = clock_ref.now();

    seastar::manual_clock::advance(10ms);

    auto now2 = clock_ref.now();
    EXPECT_GT(now2, now1);
}

TEST(ClockPolymorphism, SteadyClockReference) {
    steady_clock& clock_ref = manual_steady_clock();

    // Should work through base class reference
    auto now1 = clock_ref.now();

    seastar::manual_clock::advance(10ms);

    auto now2 = clock_ref.now();
    EXPECT_GT(now2, now1);
}

TEST(Conversions, DurationToChronoRoundTrip) {
    auto original = duration::nanoseconds(123456789);
    auto chrono_dur = original.to_chrono<std::chrono::nanoseconds>();
    auto roundtrip = duration::from_chrono(chrono_dur);

    EXPECT_EQ(original, roundtrip);
}

TEST(Conversions, ChronoSecondsToDuration) {
    auto chrono_dur = 5s;
    auto ssx_dur = duration::from_chrono(chrono_dur);

    EXPECT_EQ(ssx_dur.to_nanoseconds(), 5'000'000'000);
}

TEST(Conversions, ChronoMillisecondsToDuration) {
    auto chrono_dur = 250ms;
    auto ssx_dur = duration::from_chrono(chrono_dur);

    EXPECT_EQ(ssx_dur.to_nanoseconds(), 250'000'000);
}

} // namespace ssx
