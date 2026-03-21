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
#include "context/linker.h"

#include <gtest/gtest.h>

namespace {

using namespace std::chrono_literals;

struct spy_mixin {
    template<typename...>
    friend class context::context_frame;

    bool spy_cancel_called = false;
    context::cancel_cause spy_cancel_cause{
      context::cancel_cause::not_cancelled};

private:
    void on_context_cancel(context::cancel_cause cause) noexcept {
        spy_cancel_called = true;
        spy_cancel_cause = cause;
    }
};

} // namespace

TEST(LinkerTest, CancelPropagates) {
    context::context_frame<> parent1{context::background()};
    context::context_frame<> parent2{context::background()};
    context::context_frame<> parent3{context::background()};

    context::context_frame<context::linker<2>, spy_mixin> child{
      parent1,
      context::with<context::linker<2>>(
        context_ref{parent2}, context_ref{parent3})};

    EXPECT_FALSE(child.is_cancelled());

    parent3.cancel_handle().trigger(context::cancel_cause::manual);

    EXPECT_TRUE(child.is_cancelled());
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::manual);
}

TEST(LinkerTest, DeadlineMinimum) {
    using frame_t = context::context_frame<context::deadline_timer>;

    seastar::lowres_clock::update();

    frame_t parent1{
      context::background(), context::with<context::deadline_timer>(1h)};
    frame_t parent2{
      context::background(), context::with<context::deadline_timer>(50ms)};

    context::context_frame<context::linker<1>> child{
      parent1, context::with<context::linker<1>>(context_ref{parent2})};

    EXPECT_EQ(child.deadline(), parent2.deadline());
    EXPECT_LT(child.deadline(), parent1.deadline());
}

TEST(LinkerTest, AlreadyCancelled) {
    context::context_frame<> parent1{context::background()};
    context::context_frame<> parent2{context::background()};

    parent2.cancel_handle().trigger(context::cancel_cause::manual);
    EXPECT_TRUE(parent2.is_cancelled());

    context::context_frame<context::linker<1>, spy_mixin> child{
      parent1, context::with<context::linker<1>>(context_ref{parent2})};

    EXPECT_TRUE(child.is_cancelled());
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::manual);
    EXPECT_TRUE(child.spy_cancel_called);
}

TEST(LinkerTest, LinkHelper) {
    context::context_frame<> parent1{context::background()};
    context::context_frame<> parent2{context::background()};

    auto child = context::link(parent1, context_ref{parent2});

    EXPECT_FALSE(child.is_cancelled());

    parent2.cancel_handle().trigger(context::cancel_cause::manual);

    EXPECT_TRUE(child.is_cancelled());
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::manual);
}

TEST(LinkerTest, SameParent) {
    context::context_frame<> parent{context::background()};

    context::context_frame<context::linker<1>> child{
      parent, context::with<context::linker<1>>(context_ref{parent})};

    EXPECT_FALSE(child.is_cancelled());

    parent.cancel_handle().trigger(context::cancel_cause::manual);

    EXPECT_TRUE(child.is_cancelled());
}

TEST(LinkerTest, ChainedLinks) {
    context::context_frame<> a{context::background()};

    context::context_frame<context::linker<1>> b{
      context::background(), context::with<context::linker<1>>(context_ref{a})};

    context::context_frame<context::linker<1>> c{
      context::background(), context::with<context::linker<1>>(context_ref{b})};

    EXPECT_FALSE(c.is_cancelled());

    a.cancel_handle().trigger(context::cancel_cause::manual);

    EXPECT_TRUE(a.is_cancelled());
    EXPECT_TRUE(b.is_cancelled());
    EXPECT_TRUE(c.is_cancelled());
}
