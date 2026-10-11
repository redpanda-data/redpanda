/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "base/vassert.h" // IWYU pragma: keep; macro expansion
#include "model/timeout_clock.h"
#include "test_utils/test_macros.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/idle_cpu_handler.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/timed_out_error.hh>

#include <fmt/format.h>

#include <chrono>
#include <string>
#include <type_traits>

using namespace std::chrono_literals;

/// Waits until the given condition becomes true, failing the test on timeout.
///
/// The condition is either a boolean expression, re-evaluated every poll
/// interval:
///     RPTEST_REQUIRE_EVENTUALLY(5s, consumed_offset() == 100);
/// or a nullary predicate returning bool or ss::future<bool>:
///     RPTEST_REQUIRE_EVENTUALLY(5s, [&] { ... });
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define RPTEST_REQUIRE_EVENTUALLY(timeout, ...)                                \
    /* NOLINTNEXTLINE(*do-while*) */                                           \
    do {                                                                       \
        try {                                                                  \
            ::tests::cooperative_spin_wait_with_timeout(                       \
              timeout, ::tests::detail::eventually_predicate([&] {             \
                  return (__VA_ARGS__);                                        \
              }))                                                              \
              .get();                                                          \
        } catch (const ss::timed_out_error&) {                                 \
            RPTEST_FAIL(                                                       \
              ::tests::detail::eventually_timeout_message(                     \
                #__VA_ARGS__, __FILE__, __LINE__));                            \
        }                                                                      \
    } while (0);

/// Coroutine variant of RPTEST_REQUIRE_EVENTUALLY.
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define RPTEST_REQUIRE_EVENTUALLY_CORO(timeout, ...)                           \
    /* NOLINTNEXTLINE(*do-while*) */                                           \
    do {                                                                       \
        try {                                                                  \
            co_await ::tests::cooperative_spin_wait_with_timeout(              \
              timeout, ::tests::detail::eventually_predicate([&] {             \
                  return (__VA_ARGS__);                                        \
              }));                                                             \
        } catch (const ss::timed_out_error&) {                                 \
            RPTEST_FAIL_CORO(                                                  \
              ::tests::detail::eventually_timeout_message(                     \
                #__VA_ARGS__, __FILE__, __LINE__));                            \
        }                                                                      \
    } while (0);

namespace tests {

namespace detail {

/// A nullary predicate returning bool or ss::future<bool>.
template<typename R>
inline constexpr bool is_eventually_predicate
  = std::is_invocable_r_v<bool, R>
    || std::is_invocable_r_v<ss::future<bool>, R>;

/// A value testable for truthiness, or an ss::future<bool>.
template<typename R>
inline constexpr bool is_eventually_value
  = std::is_constructible_v<bool, R> || std::is_same_v<R, ss::future<bool>>;

/// The coroutine frame keeps \p p at a stable address: a suspended
/// coroutine predicate holds a pointer to it.
template<typename Predicate>
ss::future<>
spin_wait_loop(model::timeout_clock::time_point deadline, Predicate p) {
    while (model::timeout_clock::now() <= deadline) {
        bool stop = co_await ss::futurize_invoke(p);
        if (stop) {
            co_return;
        }
        co_await ss::sleep(std::chrono::milliseconds(10));
    }
    throw ss::timed_out_error();
}

} // namespace detail

template<typename Rep, typename Period, typename Predicate>
requires detail::is_eventually_predicate<Predicate>
/// Used to wait for Predicate to become true
ss::future<> cooperative_spin_wait_with_timeout(
  std::chrono::duration<Rep, Period> timeout, Predicate p) {
    auto deadline = model::timeout_clock::now() + timeout;
    // with_timeout bounds a predicate call that never resolves; the loop's
    // deadline check ends polling, including in the abandoned fiber.
    return ss::with_timeout(
      deadline, detail::spin_wait_loop(deadline, std::move(p)));
}

namespace detail {

inline std::string
eventually_timeout_message(const char* condition, const char* file, int line) {
    return fmt::format(
      "Timed out waiting for {} at {}:{}", condition, file, line);
}

/// \p f is the macro-wrapped condition. A predicate condition is unwrapped
/// and polled directly; a value condition is re-evaluated on every poll by
/// polling the wrapper itself.
template<typename F>
auto eventually_predicate(F f) {
    using R = std::invoke_result_t<F&>;
    static_assert(
      !(is_eventually_predicate<R> && is_eventually_value<R>),
      "RPTEST_REQUIRE_EVENTUALLY condition is ambiguous: its value is both "
      "invocable and convertible to bool. Wrap it in a lambda ([&] { return "
      "p(); }) to poll it, or write an explicit condition (p != nullptr) to "
      "test its value. For captureless lambdas, use [&] instead of []");
    if constexpr (is_eventually_predicate<R>) {
        return f();
    } else if constexpr (std::is_same_v<R, ss::future<bool>>) {
        return f;
    } else if constexpr (is_eventually_value<R>) {
        return [f = std::move(f)]() mutable { return static_cast<bool>(f()); };
    } else {
        static_assert(
          false,
          "RPTEST_REQUIRE_EVENTUALLY condition must be a boolean expression "
          "or a nullary predicate returning bool or ss::future<bool>");
        // keeps the static_assert the only error
        return [] { return false; };
    }
}

} // namespace detail

// When a test expects that any background fibers should complete promptly,
// and wants to send a barrier through all the inter-CPU queues to ensure
// that earlier-submitted tasks have reached their destination cores already.
//
// This is useful in tests that know they have put the system into a state where
// it will get to a known state once all the non-i/o-blocking tasks in flight
// have completed, such as background release of quota units.
//
// **Be aware** that there are assumptions to using this:
// A) That your test code is using the same default scheduling group that
//    this routine will run within.
// B) Seastar's debug-mode shuffling of tasks doesn't re-order stuff so
//    far that resulting delay outruns the invoke_on_all round trip, *and*
//    the timer sleep that we do in debug mode.
// C) You are calling from a seastar thread (.get() is used)
// D) Tasks that have exhausted their scheduling quota and been suspended
//    can still be running after this returns.
inline void flush_tasks() {
    // Ensure anything in inter-CPU queues before we entered the function
    // has drained: this is an all-to-all to cover the full mesh of queues
    // between cores.
    ss::smp::invoke_on_all([]() {
        return ss::smp::invoke_on_all([]() { return ss::yield(); });
    }).get();

    // Yield to anything that ended up runnable on the current core as a result
    // of the above flush.
    ss::thread::yield();

    // Mitigate shuffling task queues in debug mode with a crude sleep
#ifndef NDEBUG
    ss::sleep(10ms).get();
#endif
}

/**
 * This allows us to wait for the seastar queue to be drained.
 *
 * This can used when we need to wait for manual_clock tasks to
 * execute to ensure that task execution is deterministic. Because in
 * debug mode seastar randomizes task order, so there is no way to wait
 * for those tasks to be executed outside of draining the seastar queue.
 *
 * The above function's caveats are valid for this function, but it does not
 * require the sleep hack.
 */
inline ss::future<> drain_task_queue() {
    return ss::smp::invoke_on_all([] {
        // Using an optional ensures that this is a noop on subsequent idle CPU
        // callbacks.
        std::optional<ss::promise<>> p;
        p.emplace();
        auto fut = p->get_future();
        ss::set_idle_cpu_handler(
          [p = std::move(p)](ss::work_waiting_on_reactor) mutable {
              if (!p) {
                  return ss::idle_cpu_handler_result::no_more_work;
              }
              p->set_value();
              p.reset();
              // this tells the reactor loop to go back and check
              // for more work, which we should have just enqueued because of
              // completing the promise above.
              return ss::idle_cpu_handler_result::
                interrupted_by_higher_priority_task;
          });
        return fut;
    });
}

}; // namespace tests
