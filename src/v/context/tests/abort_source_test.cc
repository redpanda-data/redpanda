// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "context/abort_source.h"
#include "context/context_frame.h"
#include "context/deadline_timer.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

namespace {

using namespace std::chrono_literals;

using test_frame = context::context_frame<>;
using as_frame = context::context_frame<context::abort_source>;

} // namespace

// Basic functionality
// ----------------------------------------------------------------------------

TEST(AbortSourceMixinTest, CancelTriggersAbortSource) {
    as_frame frame{context::background()};
    EXPECT_FALSE(frame.as().abort_requested());

    frame.cancel_handle().trigger(context::cancel_cause::manual);
    EXPECT_TRUE(frame.as().abort_requested());
}

TEST(AbortSourceMixinTest, CancelBeforeAsCall) {
    // Verify on_context_cancel triggers abort_source even when as() was never
    // called (i.e., lazy subscription not yet created).
    as_frame frame{context::background()};

    // Cancel WITHOUT calling as() first
    frame.cancel_handle().trigger(context::cancel_cause::manual);

    // First as() call should find abort_source already triggered
    EXPECT_TRUE(frame.as().abort_requested());
}

TEST(AbortSourceMixinTest, CheckThrowsWhenCancelled) {
    as_frame frame{context::background()};
    EXPECT_NO_THROW(frame.as().check());

    frame.cancel_handle().trigger(context::cancel_cause::manual);
    EXPECT_THROW(frame.as().check(), context::cancelled_exception);

    try {
        frame.as().check();
    } catch (const context::cancelled_exception& e) {
        EXPECT_EQ(e.cause(), context::cancel_cause::manual);
        EXPECT_STREQ(e.what(), "context cancelled");
    }
}

TEST(AbortSourceMixinTest, CheckThrowsWithDeadlineCause) {
    as_frame frame{context::background()};
    frame.cancel_handle().trigger(context::cancel_cause::deadline);

    try {
        frame.as().check();
        FAIL() << "Expected cancelled_exception";
    } catch (const context::cancelled_exception& e) {
        EXPECT_EQ(e.cause(), context::cancel_cause::deadline);
        EXPECT_STREQ(e.what(), "context deadline exceeded");
    }
}

TEST(AbortSourceMixinTest, CancelledExceptionMessages) {
    EXPECT_STREQ(
      context::cancelled_exception{context::cancel_cause::manual}.what(),
      "context cancelled");
    EXPECT_STREQ(
      context::cancelled_exception{context::cancel_cause::deadline}.what(),
      "context deadline exceeded");
    EXPECT_STREQ(
      context::cancelled_exception{context::cancel_cause::not_cancelled}.what(),
      "context not cancelled");
}

// Integration with Seastar
// ----------------------------------------------------------------------------

TEST(AbortSourceMixinTest, SleepAbortableInterrupted) {
    using frame_t
      = context::context_frame<context::abort_source, context::deadline_timer>;

    seastar::lowres_clock::update();
    frame_t frame{
      context::background(), context::with<context::deadline_timer>(10ms)};

    auto start = std::chrono::steady_clock::now();
    bool was_aborted = false;
    try {
        seastar::sleep_abortable(10s, frame.as()).get();
    } catch (const context::cancelled_exception& e) {
        was_aborted = true;
        EXPECT_EQ(e.cause(), context::cancel_cause::deadline);
    }

    EXPECT_TRUE(was_aborted);
    EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);
}

// Parent-child propagation
// ----------------------------------------------------------------------------

TEST(AbortSourceMixinTest, CancelPropagesToChildAbortSource) {
    as_frame parent{context::background()};
    as_frame child{context_ref{parent}};

    EXPECT_FALSE(parent.as().abort_requested());
    EXPECT_FALSE(child.as().abort_requested());

    parent.cancel_handle().trigger(context::cancel_cause::deadline);

    EXPECT_TRUE(parent.as().abort_requested());
    EXPECT_TRUE(child.as().abort_requested());
}

TEST(AbortSourceMixinTest, ChildFromCancelledParent) {
    test_frame parent{context::background()};
    parent.cancel_handle().trigger(context::cancel_cause::deadline);

    as_frame child{context_ref{parent}};

    EXPECT_TRUE(child.is_cancelled());
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::deadline);
    EXPECT_TRUE(child.as().abort_requested());
}

// External abort (abort_source → context cancel)
// ----------------------------------------------------------------------------

TEST(AbortSourceMixinTest, ExternalAbortCancelsContextAndChildren) {
    as_frame parent{context::background()};
    as_frame child_with_as{context_ref{parent}};
    test_frame child_without_as{context_ref{parent}};
    test_frame grandchild{context_ref{child_with_as}};

    // Set up subscription and abort externally
    parent.as().request_abort();

    // All frames should be cancelled
    EXPECT_TRUE(parent.is_cancelled());
    EXPECT_TRUE(child_with_as.is_cancelled());
    EXPECT_TRUE(child_without_as.is_cancelled());
    EXPECT_TRUE(grandchild.is_cancelled());

    // Cause should be manual (from external abort subscription)
    EXPECT_EQ(parent.cancel_cause(), context::cancel_cause::manual);
    EXPECT_TRUE(child_with_as.as().abort_requested());
}

TEST(AbortSourceMixinTest, ExternalAbortWithCustomException) {
    as_frame frame{context::background()};
    test_frame child{context_ref{frame}};

    frame.as().request_abort_ex(std::runtime_error("custom abort"));

    EXPECT_TRUE(frame.is_cancelled());
    EXPECT_TRUE(child.is_cancelled());
    EXPECT_TRUE(frame.as().abort_requested());
}
