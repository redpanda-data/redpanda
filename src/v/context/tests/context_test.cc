// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "context/context.h"

#include <gtest/gtest.h>

// context_ref is zero-cost wrapper around basic_context_frame* unless debug ref
// counting is enabled
// ----------------------------------------------------------------------------
#ifndef CONTEXT_DEBUG_REF_COUNTING
static_assert(sizeof(context_ref) == sizeof(void*));
static_assert(std::is_trivially_destructible_v<context_ref>);
#endif

TEST(ContextTest, BackgroundContext) {
    context_ref ref{context::background()};

    EXPECT_FALSE(ref.is_cancelled());
    EXPECT_EQ(ref.cancel_cause(), context::cancel_cause::not_cancelled);
    EXPECT_EQ(ref.deadline(), context::no_deadline);
    EXPECT_FALSE(ref.has_deadline());
    EXPECT_EQ(ref.time_left(), context::duration::max());

    EXPECT_FALSE(context::todo().is_cancelled());
}

TEST(ContextTest, BackgroundContextImplicitConversion) {
    auto f = [](context_ref ref) { EXPECT_FALSE(ref.is_cancelled()); };

    // background() is implicitly convertible to context_ref
    f(context::background());

    context_ref ref = context::background();
    f(ref);
}

TEST(ContextTest, WallDeadlineNoDeadline) {
    context_ref ref{context::background()};

    EXPECT_EQ(
      context::wall_deadline(ref),
      std::chrono::system_clock::time_point::max());
}

TEST(ContextTest, ContextRefCopy) {
    context_ref ref1{context::background()};
    context_ref ref2{ref1};

    EXPECT_FALSE(ref1.is_cancelled());
    EXPECT_FALSE(ref2.is_cancelled());

    context_ref ref3 = ref1;
    EXPECT_FALSE(ref3.is_cancelled());
}

TEST(ContextTest, ContextRefMove) {
    context_ref ref1{context::background()};
    context_ref ref2{std::move(ref1)};

    // Move = copy, both remain valid per [lib.types.movedfrom]
    EXPECT_FALSE(ref1.is_cancelled());
    EXPECT_FALSE(ref2.is_cancelled());
}

TEST(ContextTest, ContextRefSize) {
#ifdef NDEBUG
    static_assert(sizeof(context_ref) == sizeof(void*));
#else
    static_assert(
      sizeof(context_ref) == sizeof(void*) + sizeof(oncore) + /*padding*/ 4);
#endif
}
