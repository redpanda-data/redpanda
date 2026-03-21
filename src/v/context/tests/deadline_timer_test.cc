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

#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

namespace {

using namespace std::chrono_literals;

struct context_counter_mixin {
    void on_context_cancel(const context::cancel_cause) noexcept {
        ++on_cancel_called;
    }

    int on_cancel_called{0};
};

using test_frame = context::context_frame<context_counter_mixin>;

struct spy_mixin_a {
    template<typename...>
    friend class context::context_frame;

    bool called = false;
    context::cancel_cause last_cause{context::cancel_cause::not_cancelled};

private:
    void on_context_cancel(context::cancel_cause cause) noexcept {
        called = true;
        last_cause = cause;
    }
};

} // namespace

// Initialization and Expiration
// ----------------------------------------------------------------------------

TEST(DeadlineTimerTest, WithDuration) {
    using frame_t = context::context_frame<context::deadline_timer>;

    seastar::lowres_clock::update();

    frame_t frame{
      context::background(), context::with<context::deadline_timer>(50ms)};
    context_ref ref{frame};

    // Deadline is set and in the future
    EXPECT_TRUE(ref.has_deadline());
    EXPECT_NE(ref.deadline(), context::no_deadline);
    EXPECT_GT(ref.deadline(), context::clock::now());
    EXPECT_GT(context::wall_deadline(ref), std::chrono::system_clock::now());
    EXPECT_GT(ref.time_left(), 0ms);
    EXPECT_LE(ref.time_left(), 50ms);
    EXPECT_FALSE(ref.is_cancelled());

    // Partial wait - still not cancelled
    seastar::sleep(10ms).get();
    seastar::lowres_clock::update();
    EXPECT_FALSE(ref.is_cancelled());
    EXPECT_LT(ref.time_left(), 50ms);

    // Wait for expiration
    seastar::sleep(100ms).get();
    seastar::lowres_clock::update();

    EXPECT_TRUE(ref.is_cancelled());
    EXPECT_EQ(ref.cancel_cause(), context::cancel_cause::deadline);
    EXPECT_EQ(ref.time_left(), 0ms);
    EXPECT_LT(ref.deadline(), context::clock::now());
    EXPECT_LE(context::wall_deadline(ref), std::chrono::system_clock::now());
}

TEST(DeadlineTimerTest, WithTimePoint) {
    using frame_t = context::context_frame<context::deadline_timer>;

    seastar::lowres_clock::update();

    // Use absolute time_point (lowres_clock)
    auto absolute_deadline = context::clock::now() + 100ms;
    frame_t frame{
      context::background(),
      context::with<context::deadline_timer>(absolute_deadline)};
    context_ref ref{frame};

    EXPECT_TRUE(ref.has_deadline());
    EXPECT_EQ(ref.deadline(), absolute_deadline);
    EXPECT_FALSE(ref.is_cancelled());
}

TEST(DeadlineTimerTest, WithWallClock) {
    using frame_t = context::context_frame<context::deadline_timer>;

    seastar::lowres_clock::update();

    // Use wall clock (system_clock) deadline in the future
    auto wall_deadline = std::chrono::system_clock::now() + 100ms;
    frame_t frame{
      context::background(),
      context::with<context::deadline_timer>(wall_deadline)};
    context_ref ref{frame};

    EXPECT_TRUE(ref.has_deadline());
    EXPECT_FALSE(ref.is_cancelled());
    // wall_deadline() should be close to what we set
    EXPECT_LE(context::wall_deadline(ref), wall_deadline + 10ms);
}

// Cancellation
// ----------------------------------------------------------------------------

TEST(DeadlineTimerTest, CancelBeforeDeadline) {
    using frame_t = context::context_frame<context::deadline_timer>;

    seastar::lowres_clock::update();

    frame_t frame{
      context::background(), context::with<context::deadline_timer>(1h)};
    context_ref ref{frame};

    EXPECT_FALSE(ref.is_cancelled());
    auto deadline_before = ref.deadline();
    EXPECT_NE(deadline_before, context::no_deadline);

    // Manually cancel before deadline expires
    frame.cancel_handle().trigger(context::cancel_cause::manual);

    EXPECT_TRUE(ref.is_cancelled());
    EXPECT_EQ(ref.cancel_cause(), context::cancel_cause::manual);

    // Deadline is unchanged (timer was armed then cancelled)
    EXPECT_EQ(ref.deadline(), deadline_before);
}

// Parent/Child Hierarchy
// ----------------------------------------------------------------------------

TEST(DeadlineTimerTest, PropagationToChildren) {
    using frame_t
      = context::context_frame<context::deadline_timer, spy_mixin_a>;

    frame_t ctx{
      context::background(), context::with<context::deadline_timer>(10ms)};

    seastar::lowres_clock::update();

    test_frame child{ctx};

    EXPECT_FALSE(ctx.is_cancelled());
    seastar::sleep(15ms).get();
    seastar::lowres_clock::update();
    EXPECT_TRUE(ctx.is_cancelled());
    EXPECT_EQ(ctx.cancel_cause(), context::cancel_cause::deadline);

    EXPECT_TRUE(child.is_cancelled());
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::deadline);
    EXPECT_EQ(child.on_cancel_called, 1);

    // Adding a new child after deadline expiry should inherit cancelled state
    // and call on_cancel.
    frame_t late_child{context_ref{ctx}};
    EXPECT_TRUE(late_child.is_cancelled());
    EXPECT_EQ(late_child.cancel_cause(), context::cancel_cause::deadline);
    EXPECT_TRUE(late_child.called);
}

TEST(DeadlineTimerTest, ZeroDurationImmediatelyCancels) {
    using frame_t = context::context_frame<context::deadline_timer>;

    seastar::lowres_clock::update();

    frame_t frame{
      context::background(), context::with<context::deadline_timer>(0ms)};

    // Zero duration should immediately cancel
    EXPECT_TRUE(frame.is_cancelled());
    EXPECT_EQ(frame.cancel_cause(), context::cancel_cause::deadline);
}

TEST(DeadlineTimerTest, PastWallClockImmediatelyCancels) {
    using frame_t = context::context_frame<context::deadline_timer>;

    // Set deadline to 1 hour in the past
    auto past_deadline = std::chrono::system_clock::now() - 1h;

    frame_t frame{
      context::background(),
      context::with<context::deadline_timer>(past_deadline)};

    // Should be immediately cancelled
    EXPECT_TRUE(frame.is_cancelled());
    EXPECT_EQ(frame.cancel_cause(), context::cancel_cause::deadline);
}

TEST(DeadlineTimerTest, ChildCannotExtendParentDeadline) {
    using frame_t = context::context_frame<context::deadline_timer>;

    // Parent has 50ms deadline
    frame_t parent{
      context::background(), context::with<context::deadline_timer>(50ms)};
    auto parent_deadline = parent.deadline();

    // Child tries to set 1 hour deadline (longer than parent)
    frame_t child{
      context_ref{parent}, context::with<context::deadline_timer>(1h)};

    // Child's deadline should be clamped to parent's deadline
    EXPECT_EQ(child.deadline(), parent_deadline);
    EXPECT_FALSE(child.is_cancelled());
}

TEST(DeadlineTimerTest, ChildCanShortenDeadline) {
    using frame_t = context::context_frame<context::deadline_timer>;

    seastar::lowres_clock::update();

    // Parent has 1 hour deadline
    frame_t parent{
      context::background(), context::with<context::deadline_timer>(1h)};

    // Child sets 10ms deadline (shorter than parent)
    frame_t child{
      context_ref{parent}, context::with<context::deadline_timer>(10ms)};

    // Child's deadline should be the shorter one
    EXPECT_LT(child.deadline(), parent.deadline());
    EXPECT_FALSE(child.is_cancelled());

    // Wait for child's deadline to expire
    seastar::sleep(15ms).get();
    seastar::lowres_clock::update();

    // Child is cancelled but parent is not
    EXPECT_TRUE(child.is_cancelled());
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::deadline);
    EXPECT_FALSE(parent.is_cancelled());
}

TEST(DeadlineTimerTest, DeadlineOnCancelledParent) {
    using frame_t = context::context_frame<context::deadline_timer>;

    frame_t parent{context::background()};
    parent.cancel_handle().trigger(context::cancel_cause::manual);
    EXPECT_TRUE(parent.is_cancelled());

    // Child inherits cancelled state, but deadline is still set (timer not
    // armed)
    frame_t child{
      context_ref{parent}, context::with<context::deadline_timer>(1h)};

    // Child is already cancelled (inherited), cause is from parent
    EXPECT_TRUE(child.is_cancelled());
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::manual);

    // Deadline is set even though context is cancelled
    EXPECT_NE(child.deadline(), context::no_deadline);
}
