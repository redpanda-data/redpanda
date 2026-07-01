/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/prefetch/prefetch_pacer.h"

#include <gtest/gtest.h>

namespace cloud_topics::prefetch {

TEST(prefetch_pacer, window_tracks_rate_times_latency) {
    prefetch_pacer p(
      {.min_window = 1 << 20,
       .max_window = 64 << 20,
       .min_chunk = 256 << 10,
       .max_chunk = 4 << 20,
       .safety = 1.5});
    auto t0 = ss::lowres_clock::time_point(std::chrono::seconds(0));
    auto t1 = ss::lowres_clock::time_point(std::chrono::seconds(1));
    p.observe_consumed(0, t0);
    p.observe_consumed(10 << 20, t1); // ~10 MiB/s
    p.observe_refill_latency(std::chrono::milliseconds(200));
    auto w = p.window_target(/*cap=*/1 << 30);
    EXPECT_GE(w, 1u << 20);
    EXPECT_LE(w, 64u << 20);
    // ~10MiB/s * 0.2s * 1.5 ≈ 3 MiB, within clamp
    EXPECT_NEAR(double(w), double(3u << 20), double(1u << 20));
    EXPECT_EQ(p.next_chunk_size(/*run_remaining=*/100, /*res=*/1 << 30), 100u);
}

TEST(prefetch_pacer, window_clamped_to_min) {
    // Very slow consume rate => window should be clamped to min_window.
    prefetch_pacer p(
      {.min_window = 1 << 20,
       .max_window = 64 << 20,
       .min_chunk = 256 << 10,
       .max_chunk = 4 << 20,
       .safety = 1.5});
    auto t0 = ss::lowres_clock::time_point(std::chrono::seconds(0));
    auto t1 = ss::lowres_clock::time_point(std::chrono::seconds(100));
    p.observe_consumed(0, t0);
    p.observe_consumed(1, t1); // ~10 bytes/s
    p.observe_refill_latency(std::chrono::milliseconds(10));
    // rate*latency*safety = 10 * 0.01 * 1.5 = 0.15 bytes — way below min
    EXPECT_EQ(p.window_target(1u << 30), 1u << 20);
}

TEST(prefetch_pacer, window_clamped_to_max) {
    // Very fast consume rate => window should be clamped to max_window.
    prefetch_pacer p(
      {.min_window = 1 << 20,
       .max_window = 64 << 20,
       .min_chunk = 256 << 10,
       .max_chunk = 4 << 20,
       .safety = 1.5});
    auto t0 = ss::lowres_clock::time_point(std::chrono::seconds(0));
    auto t1 = ss::lowres_clock::time_point(std::chrono::seconds(1));
    // 1 GiB/s consume, 10s latency => target is huge
    p.observe_consumed(0, t0);
    p.observe_consumed(1u << 30, t1);
    p.observe_refill_latency(std::chrono::milliseconds(10000));
    EXPECT_EQ(p.window_target(1ull << 40), 64u << 20);
}

TEST(prefetch_pacer, window_capped_by_reservation) {
    // Even if the computed window is large, it must not exceed reservation_cap.
    prefetch_pacer p(
      {.min_window = 1 << 20,
       .max_window = 64 << 20,
       .min_chunk = 256 << 10,
       .max_chunk = 4 << 20,
       .safety = 1.5});
    auto t0 = ss::lowres_clock::time_point(std::chrono::seconds(0));
    auto t1 = ss::lowres_clock::time_point(std::chrono::seconds(1));
    p.observe_consumed(0, t0);
    p.observe_consumed(10 << 20, t1);
    p.observe_refill_latency(std::chrono::milliseconds(200));
    // Cap at 1 MiB — smaller than the ~3 MiB target.
    EXPECT_EQ(p.window_target(1u << 20), 1u << 20);
}

TEST(prefetch_pacer, next_chunk_capped_by_run_remaining) {
    prefetch_pacer p(
      {.min_window = 1 << 20,
       .max_window = 64 << 20,
       .min_chunk = 256 << 10,
       .max_chunk = 4 << 20,
       .safety = 1.5});
    // No observations — chunk size starts at min_chunk (256 KiB).
    // run_remaining is smaller than min_chunk.
    EXPECT_EQ(p.next_chunk_size(1000, 1u << 30), 1000u);
}

TEST(prefetch_pacer, next_chunk_capped_by_reservation_remaining) {
    prefetch_pacer p(
      {.min_window = 1 << 20,
       .max_window = 64 << 20,
       .min_chunk = 256 << 10,
       .max_chunk = 4 << 20,
       .safety = 1.5});
    // reservation_remaining is the binding constraint.
    EXPECT_EQ(p.next_chunk_size(1u << 30, 500), 500u);
}

TEST(prefetch_pacer, chunk_grows_after_repeated_observations) {
    prefetch_pacer p(
      {.min_window = 1 << 20,
       .max_window = 64 << 20,
       .min_chunk = 256 << 10,
       .max_chunk = 4 << 20,
       .safety = 1.5});
    // After each observe_consumed call the ramp doubles up to max_chunk.
    size_t prev = p.next_chunk_size(1u << 30, 1u << 30);
    for (int i = 0; i < 10; ++i) {
        auto t = ss::lowres_clock::time_point(std::chrono::seconds(i));
        auto t_next = ss::lowres_clock::time_point(std::chrono::seconds(i + 1));
        p.observe_consumed(static_cast<size_t>(i) * (1u << 20), t);
        p.observe_consumed(static_cast<size_t>(i + 1) * (1u << 20), t_next);
        size_t cur = p.next_chunk_size(1u << 30, 1u << 30);
        EXPECT_GE(cur, prev);
        prev = cur;
    }
    // Must never exceed max_chunk.
    EXPECT_LE(prev, 4u << 20);
}

TEST(prefetch_pacer, same_timestamp_ignored) {
    prefetch_pacer p(
      {.min_window = 1 << 20,
       .max_window = 64 << 20,
       .min_chunk = 256 << 10,
       .max_chunk = 4 << 20,
       .safety = 1.5});
    auto t0 = ss::lowres_clock::time_point(std::chrono::seconds(5));
    p.observe_consumed(0, t0);
    p.observe_consumed(1u << 20, t0); // same timestamp — must not crash
    // Rate still 0 (seeded sample was skipped).
    p.observe_refill_latency(std::chrono::milliseconds(200));
    // window_target clamped to min_window because rate is 0.
    EXPECT_EQ(p.window_target(1u << 30), 1u << 20);
}

TEST(prefetch_pacer, consume_rate_bps_reflects_ewma) {
    prefetch_pacer p(
      {.min_window = 1 << 20,
       .max_window = 64 << 20,
       .min_chunk = 256 << 10,
       .max_chunk = 4 << 20,
       .safety = 1.5});
    auto t0 = ss::lowres_clock::time_point(std::chrono::seconds(0));
    auto t1 = ss::lowres_clock::time_point(std::chrono::seconds(1));
    p.observe_consumed(0, t0);
    p.observe_consumed(10 << 20, t1);
    // First real sample seeds the EWMA directly.
    EXPECT_DOUBLE_EQ(p.consume_rate_bps(), double(10 << 20));
}

} // namespace cloud_topics::prefetch
