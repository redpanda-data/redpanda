/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/reconciler/adaptive_interval.h"
#include "config/property.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace cloud_topics::reconciler;
using namespace std::chrono_literals;

namespace {

// Default test parameters.
// These come from some ad hoc simulations testing the algorithm.
constexpr size_t max_object_size = 80_MiB;
constexpr auto default_min_interval = 250ms;
constexpr auto default_max_interval = 60000ms;
constexpr double default_target_fill = 0.8;
constexpr double default_speedup_blend = 0.9;
constexpr double default_slowdown_blend = 0.4;

// Number of rounds to run when expecting the interval to stabilize.
constexpr int many_rounds = 10000;

adaptive_interval make_test_scheduler(
  std::chrono::milliseconds min_interval = default_min_interval,
  std::chrono::milliseconds max_interval = default_max_interval,
  double target_fill = default_target_fill,
  double speedup_blend = default_speedup_blend,
  double slowdown_blend = default_slowdown_blend) {
    return {
      config::mock_binding<std::chrono::milliseconds>(min_interval),
      config::mock_binding<std::chrono::milliseconds>(max_interval),
      config::mock_binding<double>(target_fill),
      config::mock_binding<double>(speedup_blend),
      config::mock_binding<double>(slowdown_blend),
      config::mock_binding<size_t>(max_object_size)};
}

double interval_ms(const adaptive_interval& scheduler) {
    return std::chrono::duration<double, std::milli>(
             scheduler.current_interval())
      .count();
}

// Run many adaptation rounds at a fixed data rate until interval stabilizes.
// Each round produces bytes = data_rate * interval, capped at max_object_size.
void stabilize(adaptive_interval& scheduler, double data_rate_mib_per_sec) {
    constexpr double MiB = 1024 * 1024;
    for (int i = 0; i < many_rounds; i++) {
        double interval_sec = interval_ms(scheduler) / 1000.0;
        double bytes_mib = data_rate_mib_per_sec * interval_sec;
        double max_mib = static_cast<double>(max_object_size) / MiB;
        size_t bytes = static_cast<size_t>(std::min(bytes_mib, max_mib) * MiB);
        scheduler.adapt(bytes);
    }
}

} // namespace

TEST(AdaptiveIntervalTest, BasicAdaptation) {
    auto scheduler = make_test_scheduler();

    // 1. Should start at min_interval
    EXPECT_NEAR(interval_ms(scheduler), 250.0, 1.0);

    // 2. Interval increases when objects are below target fill.
    // Target size is 80MiB * 0.8 = 64MiB. Produce 32MiB (half of target).
    size_t half_target = static_cast<size_t>(
      max_object_size * default_target_fill / 2);
    for (int i = 0; i < 5; i++) {
        auto prev = interval_ms(scheduler);
        scheduler.adapt(half_target);
        EXPECT_GT(interval_ms(scheduler), prev);
    }

    // 3. Interval decreases when objects are above target fill.
    // Produce full-size objects (80MiB > 64MiB target).
    for (int i = 0; i < 5; i++) {
        auto prev = interval_ms(scheduler);
        scheduler.adapt(max_object_size);
        EXPECT_LT(interval_ms(scheduler), prev);
    }

    // 4. Stabilizes at max_interval with zero data rate.
    stabilize(scheduler, 0.0);
    EXPECT_NEAR(interval_ms(scheduler), 60000.0, 10.0);

    // 5. Stabilizes at min_interval with very high data rate.
    // At 1000MiB/s, even at min_interval (250ms), we produce 250MiB > 80MiB.
    stabilize(scheduler, 1000.0);
    EXPECT_NEAR(interval_ms(scheduler), 250.0, 10.0);
}

TEST(AdaptiveIntervalTest, ConvergesToIdealInterval) {
    auto scheduler = make_test_scheduler();

    constexpr double target_object_size_mib = 80.0 * default_target_fill;

    // Data rate of 10MiB/s -> ideal interval = 64MiB / 10MiB/s = 6.4s
    constexpr double data_rate_1 = 10.0;
    constexpr double ideal_interval_1 = target_object_size_mib / data_rate_1
                                        * 1000.0;

    stabilize(scheduler, data_rate_1);
    EXPECT_NEAR(interval_ms(scheduler), ideal_interval_1, 100.0);

    // Change to data rate of 20MiB/s -> ideal interval = 64MiB / 20MiB/s = 3.2s
    constexpr double data_rate_2 = 20.0;
    constexpr double ideal_interval_2 = target_object_size_mib / data_rate_2
                                        * 1000.0;

    stabilize(scheduler, data_rate_2);
    EXPECT_NEAR(interval_ms(scheduler), ideal_interval_2, 100.0);

    // Change to data rate of 5MiB/s -> ideal interval = 64MiB / 5MiB/s = 12.8s
    constexpr double data_rate_3 = 5.0;
    constexpr double ideal_interval_3 = target_object_size_mib / data_rate_3
                                        * 1000.0;

    stabilize(scheduler, data_rate_3);
    EXPECT_NEAR(interval_ms(scheduler), ideal_interval_3, 100.0);
}

TEST(AdaptiveIntervalTest, NonAdaptiveMode) {
    // When min == max, interval should stay fixed regardless of data rate.
    auto fixed_interval = 5000ms;
    auto scheduler = make_test_scheduler(fixed_interval, fixed_interval);

    EXPECT_NEAR(interval_ms(scheduler), 5000.0, 1.0);

    // Zero data rate should not change interval.
    scheduler.adapt(0);
    EXPECT_NEAR(interval_ms(scheduler), 5000.0, 1.0);

    // Full objects should not change interval.
    scheduler.adapt(max_object_size);
    EXPECT_NEAR(interval_ms(scheduler), 5000.0, 1.0);

    // Half-full objects should not change interval.
    scheduler.adapt(max_object_size / 2);
    EXPECT_NEAR(interval_ms(scheduler), 5000.0, 1.0);

    // Many rounds at various data rates should not change interval.
    stabilize(scheduler, 0.0);
    EXPECT_NEAR(interval_ms(scheduler), 5000.0, 1.0);

    stabilize(scheduler, 100.0);
    EXPECT_NEAR(interval_ms(scheduler), 5000.0, 1.0);

    stabilize(scheduler, 1000.0);
    EXPECT_NEAR(interval_ms(scheduler), 5000.0, 1.0);
}
