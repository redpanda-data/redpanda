// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "context/context.h"
#include "context/context_frame.h"

#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <list>
#include <memory>

namespace {

using namespace std::chrono_literals;

struct context_counter_mixin {
    void on_context_cancel(const context::cancel_cause) noexcept {
        ++on_cancel_called;
    }

    int on_cancel_called{0};
};

using test_frame = context::context_frame<context_counter_mixin>;

// Spy mixins for testing private hook invocation
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

struct spy_mixin_b {
    template<typename...>
    friend class context::context_frame;

    bool called = false;

private:
    void on_context_cancel(context::cancel_cause) noexcept { called = true; }
};

// Mixin using constructor (direct init, simpler but no cross-access)
struct ctor_based_mixin {
    int value1{0};
    std::string_view value2{};

    ctor_based_mixin() = default;
    ctor_based_mixin(int v1, std::string_view v2) noexcept
      : value1(v1)
      , value2(v2) {}
};

// Test fixture for CancelPropagationNoOverride - uses ops-based cancellation
struct cancel_counter_frame final
  : public context::detail::basic_context_frame {
    int on_cancel_called{0};

    static void on_cancel_impl(
      context::detail::basic_context_frame& base,
      context::cancel_cause) noexcept {
        ++static_cast<cancel_counter_frame&>(base).on_cancel_called;
    }

    static constexpr context::detail::frame_ops ops{
      .on_cancel = &on_cancel_impl};

    explicit cancel_counter_frame(context_ref parent)
      : context::detail::basic_context_frame(parent, &ops) {}
};

} // namespace

TEST(ContextFrameTest, ContextSizes) {
    struct empty_mixin {
        void on_context_cancel(context::cancel_cause) noexcept {};
    };

#ifdef NDEBUG
    // We still have 2 pointers we can optimize away:
    // - frame_ops can be included only when needed with a tagged pointer for
    // presence
    // - cancel_cause_ can be tagged into a pointer (e.g., next_sibling_)
    constexpr size_t empty_frame_size = 56;
#else
    constexpr size_t empty_frame_size = 64;
#endif

    static_assert(sizeof(context::context_frame<>) == empty_frame_size);
    static_assert(
      sizeof(context::context_frame<empty_mixin>) == empty_frame_size);

    // Compile-time trait tests for constructible_from_tuple_v
    struct ctor_mixin {
        explicit ctor_mixin(int, std::string_view) noexcept {}
    };
    struct default_mixin {};

    static_assert(context::detail::constructible_from_tuple_v<
                  ctor_mixin,
                  std::tuple<int, std::string_view>>);
    static_assert(
      !context::detail::constructible_from_tuple_v<ctor_mixin, std::tuple<>>);
    static_assert(
      context::detail::constructible_from_tuple_v<default_mixin, std::tuple<>>);
}

TEST(ContextFrameTest, CustomContextFrame) {
    test_frame frame{context::background()};

    // Implicitly convertible to context_ref.
    auto f = [](context_ref) { test_frame tf{context::background()}; };
    f(frame);
}

TEST(ContextFrameTest, CancelCauseManual) {
    test_frame frame{context::background()};
    context_ref ref{frame};

    // Trigger manual cancellation.
    frame.cancel_handle().trigger(context::cancel_cause::manual);
    EXPECT_TRUE(ref.is_cancelled());
    EXPECT_EQ(ref.cancel_cause(), context::cancel_cause::manual);

    // Trigger not_cancelled cancellation (no-op).
    frame.cancel_handle().trigger(context::cancel_cause::not_cancelled);
    EXPECT_TRUE(ref.is_cancelled());
    EXPECT_EQ(ref.cancel_cause(), context::cancel_cause::manual);

    // Trigger deadline cancellation (no-op).
    frame.cancel_handle().trigger(context::cancel_cause::deadline);
    EXPECT_TRUE(ref.is_cancelled());
    EXPECT_EQ(ref.cancel_cause(), context::cancel_cause::manual);
}

TEST(ContextFrameTest, CancelPropagation) {
    test_frame parent{context::background()};
    test_frame child{context_ref{parent}};

    // Test by using context_ref too.
    context_ref parent_ref{parent};
    context_ref child_ref{child};

    // Cancel parent context.
    parent.cancel_handle().trigger(context::cancel_cause::deadline);

    // Child context should be cancelled as well.
    EXPECT_TRUE(parent.is_cancelled());
    EXPECT_EQ(parent.cancel_cause(), context::cancel_cause::deadline);
    EXPECT_TRUE(child.is_cancelled());
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::deadline);

    EXPECT_TRUE(parent_ref.is_cancelled());
    EXPECT_EQ(parent_ref.cancel_cause(), context::cancel_cause::deadline);
    EXPECT_TRUE(child_ref.is_cancelled());
    EXPECT_EQ(child_ref.cancel_cause(), context::cancel_cause::deadline);
}

TEST(ContextFrameTest, CancelPropagationToGrandchildren) {
    // Tests depth-first traversal: grandparent -> parent -> child
    // Covers the `curr->child_ && did_cancel` branch (true && true)
    test_frame grandparent{context::background()};
    test_frame parent{context_ref{grandparent}};
    test_frame child{context_ref{parent}};

    EXPECT_FALSE(grandparent.is_cancelled());
    EXPECT_FALSE(parent.is_cancelled());
    EXPECT_FALSE(child.is_cancelled());

    grandparent.cancel_handle().trigger(context::cancel_cause::manual);

    EXPECT_TRUE(grandparent.is_cancelled());
    EXPECT_TRUE(parent.is_cancelled());
    EXPECT_TRUE(child.is_cancelled());
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::manual);
}

TEST(ContextFrameTest, CancelPropagationSkipsAlreadyCancelledSubtree) {
    // Tests: curr->child_ is true but did_cancel is false (already cancelled)
    // Covers the `curr->child_ && did_cancel` branch (true && false)
    test_frame grandparent{context::background()};
    test_frame parent{context_ref{grandparent}};
    test_frame child{context_ref{parent}};

    // Cancel parent (and child) first
    parent.cancel_handle().trigger(context::cancel_cause::manual);
    EXPECT_TRUE(parent.is_cancelled());
    EXPECT_TRUE(child.is_cancelled());
    EXPECT_FALSE(grandparent.is_cancelled());

    // Now cancel grandparent - parent is already cancelled so did_cancel=false
    // but parent still has child_ pointing to child
    grandparent.cancel_handle().trigger(context::cancel_cause::deadline);

    EXPECT_TRUE(grandparent.is_cancelled());
    // Parent keeps original cause (not overwritten)
    EXPECT_EQ(parent.cancel_cause(), context::cancel_cause::manual);
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::manual);
}

TEST(ContextFrameTest, CancelPropagationNoOverride) {
    test_frame parent{context::background()};

    cancel_counter_frame child{context_ref{parent}};

    // Manually cancel child context first.
    child.cancel_handle().trigger(context::cancel_cause::manual);
    EXPECT_TRUE(child.is_cancelled());
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::manual);
    EXPECT_FALSE(parent.is_cancelled());
    EXPECT_EQ(parent.cancel_cause(), context::cancel_cause::not_cancelled);

    // Now cancel parent context.
    parent.cancel_handle().trigger(context::cancel_cause::deadline);
    EXPECT_TRUE(parent.is_cancelled());
    EXPECT_EQ(parent.cancel_cause(), context::cancel_cause::deadline);

    // Child context cancellation cause should not be overridden.
    EXPECT_TRUE(child.is_cancelled());
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::manual);

    EXPECT_EQ(child.on_cancel_called, 1);
}

TEST(ContextFrameTest, CancelComplexTree) {
    test_frame root{context::background()};

    std::list<test_frame> level1;
    level1.emplace_back(context_ref{root});
    level1.emplace_back(context_ref{root});
    level1.emplace_back(context_ref{root});

    std::list<test_frame> level2;
    auto l1_it = level1.begin();
    ++l1_it;

    level2.emplace_back(context_ref{*l1_it});
    level2.emplace_back(context_ref{*l1_it});
    level2.emplace_back(context_ref{*l1_it});

    std::list<test_frame> level3;
    auto l2_it = ++level2.begin();
    level3.emplace_back(context_ref{*l2_it});
    level3.emplace_back(context_ref{*l2_it});
    level3.emplace_back(context_ref{*l2_it});

    level1.begin()->trigger_cancel(context::cancel_cause::manual);
    EXPECT_TRUE(level1.begin()->is_cancelled());
    for (auto it = std::next(level1.begin()); it != level1.end(); ++it) {
        EXPECT_FALSE(it->is_cancelled());
    }
    for (auto& f : level2) {
        EXPECT_FALSE(f.is_cancelled());
    }
    for (auto& f : level3) {
        EXPECT_FALSE(f.is_cancelled());
    }

    l1_it->trigger_cancel(context::cancel_cause::deadline);
    EXPECT_TRUE(l1_it->is_cancelled());
    for (auto it = level2.begin(); it != level2.end(); ++it) {
        EXPECT_TRUE(it->is_cancelled());
    }
    for (auto& f : level3) {
        EXPECT_TRUE(f.is_cancelled());
    }
}

TEST(ContextFrameTest, ContextRefCopyAssignment) {
    test_frame frame1{context::background()};
    test_frame frame2{context::background()};

    context_ref ref1{frame1};
    context_ref ref2{frame2};

    ref1 = ref2;

    EXPECT_FALSE(ref1.is_cancelled());
    EXPECT_FALSE(ref2.is_cancelled());
}

TEST(ContextFrameTest, ContextRefMoveAssignment) {
    test_frame frame1{context::background()};
    test_frame frame2{context::background()};

    context_ref ref1{frame1};
    context_ref ref2{frame2};

    ref1 = std::move(ref2);

    EXPECT_FALSE(ref1.is_cancelled());
}

TEST(ContextFrameTest, CancelHandleCopyConstructor) {
    test_frame frame{context::background()};
    context::cancel_handle handle1 = frame.cancel_handle();

    context::cancel_handle handle2{handle1};

    handle2.trigger(context::cancel_cause::manual);
    EXPECT_TRUE(frame.is_cancelled());
}

TEST(ContextFrameTest, CancelHandleCopyAssignment) {
    test_frame frame1{context::background()};
    test_frame frame2{context::background()};

    context::cancel_handle handle1 = frame1.cancel_handle();
    context::cancel_handle handle2 = frame2.cancel_handle();

    handle1 = handle2;

    handle1.trigger(context::cancel_cause::manual);
    EXPECT_TRUE(frame2.is_cancelled());
}

TEST(ContextFrameTest, CancelHandleMoveAssignment) {
    test_frame frame1{context::background()};
    test_frame frame2{context::background()};

    context::cancel_handle handle1 = frame1.cancel_handle();
    context::cancel_handle handle2 = frame2.cancel_handle();

    handle1 = std::move(handle2);

    handle1.trigger(context::cancel_cause::manual);
    EXPECT_TRUE(frame2.is_cancelled());
}

TEST(ContextFrameTest, OnCancelCalledExactlyOnce) {
    test_frame parent{context::background()};
    test_frame child{context_ref{parent}};

    // Cancel multiple times.
    child.trigger_cancel(context::cancel_cause::manual);
    child.trigger_cancel(context::cancel_cause::deadline);
    child.trigger_cancel(context::cancel_cause::manual);

    // on_cancel should only be called once.
    EXPECT_EQ(child.on_cancel_called, 1);
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::manual);
}

TEST(ContextFrameTest, ChildDestroyedBeforeParent) {
    test_frame parent{context::background()};

    {
        test_frame child{context_ref{parent}};
        EXPECT_FALSE(child.is_cancelled());
    }
    // Child destroyed, parent should still be valid.

    EXPECT_FALSE(parent.is_cancelled());
    parent.trigger_cancel(context::cancel_cause::manual);
    EXPECT_TRUE(parent.is_cancelled());
}

TEST(ContextFrameTest, MiddleChildDestroyed) {
    test_frame parent{context::background()};

    std::list<test_frame> children;
    children.emplace_back(context_ref{parent});
    children.emplace_back(context_ref{parent});
    children.emplace_back(context_ref{parent});

    // Destroy middle child.
    auto it = children.begin();
    ++it;
    children.erase(it);

    // Cancel parent - should propagate to remaining children.
    parent.trigger_cancel(context::cancel_cause::manual);

    EXPECT_TRUE(parent.is_cancelled());
    for (auto& child : children) {
        EXPECT_TRUE(child.is_cancelled());
    }
}

TEST(ContextFrameTest, FirstChildDestroyed) {
    test_frame parent{context::background()};

    std::list<test_frame> children;
    children.emplace_back(context_ref{parent});
    children.emplace_back(context_ref{parent});
    children.emplace_back(context_ref{parent});

    // Destroy first child.
    children.erase(children.begin());

    // Cancel parent - should propagate to remaining children.
    parent.trigger_cancel(context::cancel_cause::manual);

    EXPECT_TRUE(parent.is_cancelled());
    for (auto& child : children) {
        EXPECT_TRUE(child.is_cancelled());
    }
}

TEST(ContextFrameTest, LastChildDestroyed) {
    test_frame parent{context::background()};

    std::list<test_frame> children;
    children.emplace_back(context_ref{parent});
    children.emplace_back(context_ref{parent});
    children.emplace_back(context_ref{parent});

    // Destroy last child.
    children.pop_back();

    // Cancel parent - should propagate to remaining children.
    parent.trigger_cancel(context::cancel_cause::manual);

    EXPECT_TRUE(parent.is_cancelled());
    for (auto& child : children) {
        EXPECT_TRUE(child.is_cancelled());
    }
}

TEST(ContextFrameTest, ChildInheritsCancelledState) {
    test_frame parent{context::background()};

    // Cancel parent first.
    parent.trigger_cancel(context::cancel_cause::deadline);
    EXPECT_TRUE(parent.is_cancelled());

    // Create child from cancelled parent.
    test_frame child{context_ref{parent}};

    // Child should inherit cancellation status.
    EXPECT_TRUE(child.is_cancelled());
    EXPECT_EQ(child.cancel_cause(), context::cancel_cause::deadline);
}

TEST(ContextFrameTest, MixinPrivateHookCalled) {
    using frame_t = context::context_frame<spy_mixin_a>;

    frame_t frame{context::background()};

    EXPECT_FALSE(frame.called);

    frame.cancel_handle().trigger(context::cancel_cause::manual);

    EXPECT_TRUE(frame.called);
    EXPECT_EQ(frame.last_cause, context::cancel_cause::manual);
}

TEST(ContextFrameTest, MultipleMixinsAllCalled) {
    using frame_t = context::context_frame<spy_mixin_a, spy_mixin_b>;

    frame_t frame{context::background()};

    EXPECT_FALSE(frame.spy_mixin_a::called);
    EXPECT_FALSE(frame.spy_mixin_b::called);

    frame.cancel_handle().trigger(context::cancel_cause::deadline);

    EXPECT_TRUE(frame.spy_mixin_a::called);
    EXPECT_TRUE(frame.spy_mixin_b::called);
}

TEST(ContextFrameTest, HasDeadline) {
    test_frame frame{context::background()};
    context_ref ref{frame};

    EXPECT_FALSE(ref.has_deadline());
    EXPECT_EQ(ref.deadline(), context::no_deadline);
}

// Mixin init wrapper constructor tests
// ----------------------------------------------------------------------------

TEST(MixinWithTest, MultipleMixinsWith) {
    struct mixin_a {
        int val{0};
        mixin_a() = default;
        explicit mixin_a(int v)
          : val(v) {}
    };

    using frame_t = context::context_frame<mixin_a, ctor_based_mixin>;

    frame_t frame{
      context::background(),
      context::with<mixin_a>(123),
      context::with<ctor_based_mixin>(42, "test")};

    EXPECT_EQ(frame.mixin_a::val, 123);
    EXPECT_EQ(frame.ctor_based_mixin::value1, 42);
}

TEST(MixinWithTest, WithOrderIndependent) {
    struct mixin_a {
        int val{0};
        mixin_a() = default;
        explicit mixin_a(int v)
          : val(v) {}
    };

    // with<> order doesn't have to match mixin order in composed_frame
    using frame_t = context::context_frame<ctor_based_mixin, mixin_a>;

    // with<> in reverse order of mixin declaration
    frame_t frame{
      context::background(),
      context::with<mixin_a>(77),
      context::with<ctor_based_mixin>(99, "order_test")};

    EXPECT_EQ(frame.ctor_based_mixin::value1, 99);
    EXPECT_EQ(frame.mixin_a::val, 77);
}

TEST(MixinWithTest, PartialWith) {
    // Only init some mixins via with<>, others use defaults
    using frame_t = context::context_frame<ctor_based_mixin, spy_mixin_a>;

    frame_t frame{
      context::background(), context::with<ctor_based_mixin>(100, "partial")};

    // spy_mixin_a default-constructed
    EXPECT_FALSE(frame.called);
    EXPECT_EQ(frame.value1, 100);
}

namespace {
// Mixin using on_context_init (two-step, can access full object)
struct hook_based_mixin {
    template<typename...>
    friend class context::context_frame;

    int value1{0};
    std::string_view value2{};

private:
    void on_context_init(int v1, std::string_view v2) noexcept {
        value1 = v1;
        value2 = v2;
    }
};

// Mixin with public default ctor and zero-arg on_context_init for self access
struct zero_arg_hook_mixin {
    template<typename...>
    friend class context::context_frame;

    bool init_called{false};

    zero_arg_hook_mixin() noexcept = default;

private:
    template<typename Self>
    void on_context_init(this Self& self) noexcept {
        self.init_called = true;
    }
};

} // namespace

TEST(MixinWithTest, HookBasedMixin) {
    using frame_t = context::context_frame<hook_based_mixin>;

    frame_t frame{
      context::background(), context::with<hook_based_mixin>(42, "hello")};

    EXPECT_EQ(frame.value1, 42);
    EXPECT_EQ(frame.value2, "hello");
}

TEST(MixinWithTest, ConstructorBasedMixin) {
    using frame_t = context::context_frame<ctor_based_mixin>;

    frame_t frame{
      context::background(), context::with<ctor_based_mixin>(99, "world")};

    EXPECT_EQ(frame.value1, 99);
    EXPECT_EQ(frame.value2, "world");
}

TEST(MixinWithTest, MixedInitStrategies) {
    // Mix both approaches in one frame
    using frame_t = context::context_frame<hook_based_mixin, ctor_based_mixin>;

    frame_t frame{
      context::background(),
      context::with<hook_based_mixin>(1, "hook"),
      context::with<ctor_based_mixin>(2, "ctor")};

    EXPECT_EQ(frame.hook_based_mixin::value1, 1);
    EXPECT_EQ(frame.hook_based_mixin::value2, "hook");
    EXPECT_EQ(frame.ctor_based_mixin::value1, 2);
    EXPECT_EQ(frame.ctor_based_mixin::value2, "ctor");
}

TEST(MixinWithTest, ZeroArgHookInit) {
    // Mixin with public default ctor still gets on_context_init() called
    using frame_t = context::context_frame<zero_arg_hook_mixin>;

    frame_t frame{context::background(), context::with<zero_arg_hook_mixin>()};

    EXPECT_TRUE(frame.init_called);
}

TEST(MixinWithTest, UninitializedMixinGetsHookCalled) {
    // Bug test: when a mixin is NOT explicitly initialized via with<>,
    // its on_context_init() should still be called after default construction.
    using frame_t
      = context::context_frame<ctor_based_mixin, zero_arg_hook_mixin>;

    // Only initialize ctor_based_mixin, NOT zero_arg_hook_mixin
    frame_t frame{
      context::background(), context::with<ctor_based_mixin>(42, "test")};

    // zero_arg_hook_mixin was default-constructed, but on_context_init()
    // should still have been called
    EXPECT_TRUE(frame.init_called);
}

TEST(MixinWithTest, ConstraintsRejectInvalidUsage) {
    using frame_t = context::context_frame<ctor_based_mixin, spy_mixin_a>;

    // Valid: single with<> per mixin
    static_assert(std::is_constructible_v<
                  frame_t,
                  context_ref,
                  decltype(context::with<ctor_based_mixin>(1, "x"))>);

    // Invalid: duplicate with<> for same mixin
    static_assert(!std::is_constructible_v<
                  frame_t,
                  context_ref,
                  decltype(context::with<ctor_based_mixin>(1, "x")),
                  decltype(context::with<ctor_based_mixin>(2, "y"))>);

    // Invalid: with<> targets mixin not in composed_frame
    struct other_mixin {};
    static_assert(!std::is_constructible_v<
                  frame_t,
                  context_ref,
                  decltype(context::with<other_mixin>())>);
}

// Test: when mixin throws during construction, frame is properly unlinked
// from parent (base class destructor runs during stack unwinding).
TEST(ContextFrameTest, ThrowingMixinUnlinksFromParent) {
    static int dtor_count = 0;

    struct counting_mixin {
        ~counting_mixin() { ++dtor_count; }
    };

    struct throwing_mixin {
        throwing_mixin() { throw std::runtime_error("mixin throws"); }
    };

    test_frame parent{context::background()};
    test_frame normal_child{context_ref{parent}};

    dtor_count = 0;
    using throwing_frame
      = context::context_frame<counting_mixin, throwing_mixin>;

    EXPECT_THROW(
      { [[maybe_unused]] throwing_frame bad_child{context_ref{parent}}; },
      std::runtime_error);

    // counting_mixin destructor should have run during stack unwinding
    EXPECT_EQ(dtor_count, 1);

    // Parent should still be able to cancel - the failed child was unlinked
    parent.cancel_handle().trigger(context::cancel_cause::manual);
    EXPECT_TRUE(parent.is_cancelled());
    EXPECT_TRUE(normal_child.is_cancelled());
}

// Test: moved-from context_ref remains valid per [lib.types.movedfrom].
// Move behaves like copy - both refs observe the same frame.
TEST(ContextFrameTest, MovedFromContextRefBehavior) {
    test_frame frame{context::background()};
    context_ref ref1{frame};

    // Move ref1 to ref2
    context_ref ref2{std::move(ref1)};

    // Both refs should work and observe the same frame
    EXPECT_FALSE(ref1.is_cancelled());
    EXPECT_FALSE(ref2.is_cancelled());

    // Cancelling via frame affects both refs
    frame.cancel_handle().trigger(context::cancel_cause::manual);
    EXPECT_TRUE(ref1.is_cancelled());
    EXPECT_TRUE(ref2.is_cancelled());
}

// Test: moved-from cancel_handle remains valid per [lib.types.movedfrom].
// Move behaves like copy - both handles can trigger the same frame.
TEST(ContextFrameTest, MovedFromCancelHandleBehavior) {
    test_frame frame{context::background()};
    context::cancel_handle handle1 = frame.cancel_handle();

    // Move handle1 to handle2
    context::cancel_handle handle2{std::move(handle1)};

    // Both handles should work - first trigger cancels
    handle1.trigger(context::cancel_cause::manual);
    EXPECT_TRUE(frame.is_cancelled());
    EXPECT_EQ(frame.cancel_cause(), context::cancel_cause::manual);

    // Second trigger is no-op (already cancelled with different cause)
    handle2.trigger(context::cancel_cause::deadline);
    EXPECT_EQ(frame.cancel_cause(), context::cancel_cause::manual);
}

#ifdef CONTEXT_DEBUG_REF_COUNTING
TEST(ContextDeathTest, ParentDestroyedWithLiveChildren) {
    ASSERT_DEATH(
      {
          auto parent = std::make_unique<test_frame>(context::background());
          [[maybe_unused]] test_frame child{context_ref{*parent}};
          parent.reset();
      },
      "live children");
}

TEST(ContextDeathTest, FrameDestroyedWithLiveRefs) {
    ASSERT_DEATH(
      {
          auto frame = std::make_unique<test_frame>(context::background());
          [[maybe_unused]] context_ref ref{*frame};
          frame.reset();
      },
      "live context_ref");
}
#endif

// Exception Propagation Tests
// ----------------------------------------------------------------------------

namespace {

// Mixins for exception propagation testing.

struct throwing_init_mixin {
    template<typename...>
    friend class context::context_frame;

private:
    void on_context_init(int) { throw std::runtime_error("init throws"); }
};

struct throwing_zero_arg_init_mixin {
    template<typename...>
    friend class context::context_frame;

private:
    void on_context_init() { throw std::runtime_error("init throws"); }
};

} // namespace

TEST(ContextFrameTest, FrameExceptionPropagation) {
    // Throwing constructor propagates
    struct throwing_ctor {
        throwing_ctor() { throw std::runtime_error("ctor throws"); }
    };
    EXPECT_THROW(
      {
          [[maybe_unused]] context::context_frame<throwing_ctor> frame{
            context::background()};
      },
      std::runtime_error);

    // Throwing on_context_init(args) propagates
    EXPECT_THROW(
      {
          [[maybe_unused]] context::context_frame<throwing_init_mixin> frame(
            context::background(), context::with<throwing_init_mixin>(42));
      },
      std::runtime_error);

    // Throwing zero-arg on_context_init() propagates
    EXPECT_THROW(
      {
          [[maybe_unused]] context::context_frame<throwing_zero_arg_init_mixin>
            frame{context::background()};
      },
      std::runtime_error);
}
