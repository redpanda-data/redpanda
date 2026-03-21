// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "context/context.h"
#include "context/context_frame.h"
#include "context/deadline_timer.h"
#include "ssx/context_sleep.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

namespace {

using namespace std::chrono_literals;

} // namespace

TEST(ContextSleepTest, Basic) {
    // Basic sleep completes successfully and takes expected time
    ss::lowres_clock::update();
    auto start = ss::lowres_clock::now();
    ssx::sleep(context::background(), 10ms).get();
    ss::lowres_clock::update();
    auto elapsed = ss::lowres_clock::now() - start;
    EXPECT_GE(elapsed, 5ms);
}

TEST(ContextSleepTest, ZeroDuration) {
    // Zero/negative duration returns immediately
    auto fut = ssx::sleep(context::background(), 0ms);
    EXPECT_TRUE(fut.available());
    fut.get();
}

TEST(ContextSleepTest, PreCancelled) {
    // Pre-cancelled context throws immediately with correct cause
    context::context_frame<> ctx{context::background()};
    ctx.cancel_handle().trigger(context::cancel_cause::manual);

    try {
        ssx::sleep(ctx, 1000s).get();
        FAIL() << "Expected ssx::context_sleep_aborted exception";
    } catch (const ssx::context_sleep_aborted& e) {
        EXPECT_EQ(e.cause(), context::cancel_cause::manual);
    }
}

TEST(ContextSleepTest, CancelDuringSleep) {
    // Cancel context during sleep throws with correct cause
    context::context_frame<> ctx{context::background()};

    auto fut = ssx::sleep(ctx, 1000s);
    ss::sleep(10ms).get();
    ctx.cancel_handle().trigger(context::cancel_cause::manual);

    try {
        fut.get();
        FAIL() << "Expected ssx::context_sleep_aborted exception";
    } catch (const ssx::context_sleep_aborted& e) {
        EXPECT_EQ(e.cause(), context::cancel_cause::manual);
    }
}

TEST(ContextSleepTest, DeadlineCancels) {
    // Deadline timer cancels sleep with correct cause
    using frame_t = context::context_frame<context::deadline_timer>;

    ss::lowres_clock::update();

    frame_t ctx{
      context::background(), context::with<context::deadline_timer>(20ms)};

    try {
        ssx::sleep(ctx, 1000s).get();
        FAIL() << "Expected ssx::context_sleep_aborted exception";
    } catch (const ssx::context_sleep_aborted& e) {
        EXPECT_EQ(e.cause(), context::cancel_cause::deadline);
    }
}

TEST(ContextSleepTest, CatchableAsSeastarSleepAborted) {
    // Verify ssx::context_sleep_aborted is catchable as seastar::sleep_aborted
    context::context_frame<> frame{context::background()};
    frame.cancel_handle().trigger(context::cancel_cause::manual);

    EXPECT_THROW(ssx::sleep(frame, 1000s).get(), ss::sleep_aborted);
}
