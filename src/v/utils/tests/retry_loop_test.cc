// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "context/context_frame.h"
#include "context/deadline_timer.h"
#include "utils/retry_loop.h"

#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

namespace {

using namespace std::chrono_literals;

} // namespace

TEST(RetryLoopTest, AttemptsExhausted) {
    constexpr uint16_t max_attempts = 5;
    utils::retry_loop_state rs{{.jitter = 0.0, .max_attempts = max_attempts}};

    context::context_frame<> ctx{context::background()};

    uint16_t count = 0;
    while (auto attempt = rs.next(ctx).get()) {
        EXPECT_EQ(*attempt, count);
        ++count;
    }

    EXPECT_EQ(count, max_attempts);

    // Further calls should return nullopt
    EXPECT_FALSE(rs.next(ctx).get().has_value());
}

TEST(RetryLoopTest, CancellationReturnsNullopt) {
    utils::retry_loop_state rs{{.jitter = 0.0, .max_attempts = 10}};

    context::context_frame<> ctx{context::background()};

    // First attempt succeeds
    auto first = rs.next(ctx).get();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(*first, 0);

    // Cancel the context
    ctx.cancel_handle().trigger(context::cancel_cause::manual);

    // Next attempt should return nullopt
    EXPECT_FALSE(rs.next(ctx).get().has_value());
}

TEST(RetryLoopTest, DeadlineExpiryReturnsNullopt) {
    // Configure backoff with initial delay longer than context deadline
    utils::retry_loop_state rs{
      {.initial_delay = 100ms, .jitter = 0.0, .max_attempts = 10}};

    using frame_t = context::context_frame<context::deadline_timer>;
    frame_t ctx{
      context::background(), context::with<context::deadline_timer>(5ms)};

    // First attempt returns immediately (no sleep)
    auto first = rs.next(ctx).get();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(*first, 0);

    // Second attempt: delay (100ms) > time_left (~5ms), returns nullopt
    EXPECT_FALSE(rs.next(ctx).get().has_value());

    // Context may or may not be cancelled yet (deadline timer is async)
    // The key assertion is that backoff returned nullopt due to insufficient
    // time
}

TEST(RetryLoopTest, CancellationDuringSleepReturnsNullopt) {
    // Use short initial delay so the sleep actually happens
    utils::retry_loop_state rs{
      {.initial_delay = 100ms, .jitter = 0.0, .max_attempts = 10}};

    context::context_frame<> ctx{context::background()};

    // First attempt returns immediately (no sleep)
    auto first = rs.next(ctx).get();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(*first, 0);

    // Start second attempt (will sleep for ~100ms)
    auto fut = rs.next(ctx);

    // Wait briefly then cancel
    seastar::sleep(10ms).get();
    ctx.cancel_handle().trigger(context::cancel_cause::manual);

    // Should return nullopt via handle_exception_type path
    EXPECT_FALSE(fut.get().has_value());
    EXPECT_TRUE(ctx.is_cancelled());
}
