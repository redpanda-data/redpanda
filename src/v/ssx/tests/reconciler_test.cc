// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "ssx/reconciler.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <chrono>
#include <exception>
#include <stdexcept>

namespace ssx {

using namespace std::chrono_literals;

namespace {

ss::logger test_log("reconciler_test");

/// We want to be able to be more selective than ssx::is_shutdown_exception so
/// we can distinguish broken_promise from abort_requested.
template<typename Ex>
bool failed_with(const ss::future<>& f) {
    try {
        std::rethrow_exception(const_cast<ss::future<>&>(f).get_exception());
    } catch (const Ex&) {
        return true;
    } catch (...) {
    }
    return false;
}

/// Runs a step under a gate closed on scope exit, so a test that leaves the
/// loop mid-retry does not have to unwind it by hand.
class harness {
public:
    explicit harness(reconcile_fn step, backoff_policy backoff = {})
      : _r(_gate, test_log, "test", std::move(step), _as, backoff) {}

    reconciler& r() { return _r; }
    ss::abort_source& as() { return _as; }

    ss::future<> stop() {
        _as.request_abort();
        return _gate.close();
    }

    ss::future<> close_gate() { return _gate.close(); }

private:
    ss::gate _gate;
    ss::abort_source _as;
    reconciler _r;
};

} // namespace

TEST_CORO(Reconciler, does_not_run_until_notified) {
    int runs = 0;
    harness h([&runs] {
        ++runs;
        return ss::now();
    });
    co_await ss::yield();
    ASSERT_EQ_CORO(runs, 0);
    ASSERT_TRUE_CORO(h.r().idle());
    co_await h.stop();
}

TEST_CORO(Reconciler, notify_and_wait_resolves_after_a_run) {
    int runs = 0;
    harness h([&runs] {
        ++runs;
        return ss::now();
    });
    co_await h.r().notify_and_wait();
    ASSERT_EQ_CORO(runs, 1);
    co_await h.stop();
}

// Notifications arriving while a run is in progress collapse into exactly one
// further run: not zero (the change would be lost) and not one per notify.
TEST_CORO(Reconciler, notifies_during_a_run_collapse_into_one) {
    int runs = 0;
    ss::promise<> hold;
    harness h([&runs, &hold] {
        ++runs;
        return runs == 1 ? hold.get_future() : ss::now();
    });

    h.r().notify();
    co_await ss::yield();
    ASSERT_EQ_CORO(runs, 1);

    h.r().notify();
    h.r().notify();
    h.r().notify();
    ASSERT_EQ_CORO(runs, 1);

    hold.set_value();
    co_await tests::cooperative_spin_wait_with_timeout(
      5s, [&h] { return h.r().idle(); });
    ASSERT_EQ_CORO(runs, 2);
    co_await h.stop();
}

// A run already in flight may have read the state the notification is about, so
// it cannot be what satisfies the waiter.
TEST_CORO(Reconciler, a_run_in_flight_does_not_satisfy_a_later_waiter) {
    int runs = 0;
    ss::promise<> hold;
    harness h([&runs, &hold] {
        ++runs;
        return runs == 1 ? hold.get_future() : ss::now();
    });

    h.r().notify();
    co_await ss::yield();
    ASSERT_EQ_CORO(runs, 1);

    auto waiter = h.r().notify_and_wait();
    ASSERT_FALSE_CORO(waiter.available());

    hold.set_value();
    co_await std::move(waiter);
    ASSERT_EQ_CORO(runs, 2);
    co_await h.stop();
}

// A waiter queued while a run is in flight is folded into the retry of that
// run, so it is answered by the next attempt rather than waiting for one of its
// own -- and, above all, is not dropped.
TEST_CORO(Reconciler, a_waiter_queued_during_a_retry_is_still_answered) {
    int runs = 0;
    ss::promise<> hold;
    harness h(
      [&runs, &hold] {
          ++runs;
          return runs == 1 ? hold.get_future() : ss::now();
      },
      backoff_policy{.base = 1ms, .max = 1ms});

    auto first = h.r().notify_and_wait();
    co_await ss::yield();
    ASSERT_EQ_CORO(runs, 1);

    // Queued while the first run is still in flight, so the attempt that
    // answers it is the retry, which begins after the notification.
    auto second = h.r().notify_and_wait();

    hold.set_exception(std::runtime_error("first attempt fails"));
    co_await std::move(first);
    co_await std::move(second);
    ASSERT_EQ_CORO(runs, 2);
    co_await h.stop();
}

// Two waiters answered by consecutive attempts of one loop. Each attempt
// answers its own batch and no more: the first waiter is answered as its
// attempt ends, while the second attempt is already in flight and its waiter
// still pending.
TEST_CORO(Reconciler, consecutive_attempts_answer_their_own_waiters) {
    // The step reports each attempt as it begins and then blocks, so the test
    // can look at the reconciler with an attempt in flight.
    ss::semaphore begun{0};
    ss::semaphore finish{0};
    int attempts = 0;
    harness h([&begun, &finish, &attempts] {
        ++attempts;
        begun.signal(1);
        return finish.wait(1);
    });

    auto first = h.r().notify_and_wait();
    co_await begun.wait(1);
    ASSERT_FALSE_CORO(first.available());

    // Queued while the first attempt is in flight, so it belongs to the next
    // one rather than to the attempt about to finish.
    auto second = h.r().notify_and_wait();

    finish.signal(1);
    co_await begun.wait(1);

    // The second attempt is now blocked, and the state of the two waiters says
    // which attempt answered which.
    ASSERT_TRUE_CORO(first.available());
    ASSERT_FALSE_CORO(second.available());
    co_await std::move(first);

    finish.signal(1);
    co_await std::move(second);
    ASSERT_EQ_CORO(attempts, 2);
    co_await h.stop();
}

TEST_CORO(Reconciler, failures_are_retried_until_success) {
    int runs = 0;
    harness h(
      [&runs] {
          ++runs;
          if (runs <= 3) {
              return ss::make_exception_future<>(std::runtime_error("not yet"));
          }
          return ss::now();
      },
      backoff_policy{.base = 1ms, .max = 4ms});

    co_await h.r().notify_and_wait();
    ASSERT_EQ_CORO(runs, 4);
    co_await h.stop();
}

// A step that throws before its first suspension point fails the attempt, it
// does not escape the loop.
TEST_CORO(Reconciler, a_synchronous_throw_is_a_failed_attempt) {
    int runs = 0;
    harness h(
      [&runs] {
          ++runs;
          if (runs == 1) {
              throw std::runtime_error("thrown before suspending");
          }
          return ss::now();
      },
      backoff_policy{.base = 1ms, .max = 1ms});

    co_await h.r().notify_and_wait();
    ASSERT_EQ_CORO(runs, 2);
    co_await h.stop();
}

TEST_CORO(Reconciler, a_shutdown_exception_from_the_step_stops_the_loop) {
    int runs = 0;
    harness h([&runs] {
        ++runs;
        return ss::make_exception_future<>(ss::abort_requested_exception{});
    });

    auto res = co_await ss::coroutine::as_future(h.r().notify_and_wait());
    ASSERT_TRUE_CORO(res.failed());
    ASSERT_TRUE_CORO(failed_with<ss::abort_requested_exception>(res));
    // The loop gives up before answering the waiter, so it is already gone by
    // the time this resumes: stopped rather than retried.
    ASSERT_TRUE_CORO(h.r().idle());
    ASSERT_EQ_CORO(runs, 1);
    co_await h.stop();
}

// An abort has to interrupt the backoff, not merely be noticed once it expires,
// so the backoff here is far longer than the test is willing to wait for.
TEST_CORO(Reconciler, an_abort_fails_the_waiter_rather_than_hanging_it) {
    ss::semaphore attempted{0};
    harness h(
      [&attempted] {
          attempted.signal(1);
          return ss::make_exception_future<>(
            std::runtime_error("never succeeds"));
      },
      backoff_policy{.base = 10s, .max = 10s});

    auto waiter = h.r().notify_and_wait();
    // The attempt has run and failed, so the loop is in its backoff.
    co_await attempted.wait(1);

    const auto start = std::chrono::steady_clock::now();
    h.as().request_abort();
    auto res = co_await ss::coroutine::as_future(std::move(waiter));
    const auto waited = std::chrono::steady_clock::now() - start;

    ASSERT_TRUE_CORO(res.failed());
    ASSERT_TRUE_CORO(failed_with<ss::abort_requested_exception>(res));
    ASSERT_LT_CORO(waited, 2s);
    co_await h.stop();
}

// Closing the gate has to end a retrying loop, or the close waits on a holder
// that never leaves -- and the waiter it was carrying has to be told. Unlike an
// abort, a close cannot interrupt the backoff, so the loop only notices when
// the sleep expires and the close pays for it: hence the short backoff here.
TEST_CORO(Reconciler, closing_the_gate_ends_a_retrying_loop) {
    ss::semaphore attempted{0};
    harness h(
      [&attempted] {
          attempted.signal(1);
          return ss::make_exception_future<>(
            std::runtime_error("never succeeds"));
      },
      backoff_policy{.base = 20ms, .max = 20ms});

    auto waiter = h.r().notify_and_wait();
    // The attempt has run and failed, so the loop is retrying.
    co_await attempted.wait(1);

    co_await h.close_gate();
    auto res = co_await ss::coroutine::as_future(std::move(waiter));
    ASSERT_TRUE_CORO(res.failed());
    ASSERT_TRUE_CORO(failed_with<ss::gate_closed_exception>(res));
}

TEST_CORO(Reconciler, an_already_aborted_source_fails_the_waiter) {
    ss::gate gate;
    ss::abort_source as;
    as.request_abort();

    int runs = 0;
    reconciler r(
      gate,
      test_log,
      "test",
      [&runs] {
          ++runs;
          return ss::now();
      },
      as);

    auto res = co_await ss::coroutine::as_future(r.notify_and_wait());
    ASSERT_TRUE_CORO(res.failed());
    ASSERT_TRUE_CORO(failed_with<ss::abort_requested_exception>(res));
    ASSERT_EQ_CORO(runs, 0);
    co_await gate.close();
}

TEST_CORO(Reconciler, notifying_a_closed_reconciler_fails_the_waiter) {
    int runs = 0;
    harness h([&runs] {
        ++runs;
        return ss::now();
    });
    co_await h.close_gate();

    h.r().notify();
    auto res = co_await ss::coroutine::as_future(h.r().notify_and_wait());
    ASSERT_TRUE_CORO(res.failed());
    ASSERT_TRUE_CORO(failed_with<ss::gate_closed_exception>(res));
    ASSERT_EQ_CORO(runs, 0);
}

} // namespace ssx
