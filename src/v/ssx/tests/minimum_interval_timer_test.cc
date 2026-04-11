/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "ssx/minimum_interval_timer.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/util/later.hh>

using namespace std::chrono_literals;
using namespace ::testing;

namespace ssx {

namespace {

constexpr auto arbitrary_interval = 1ms;
constexpr auto minimum_interval = 100ms;

constexpr const char* should_have_fired_message = "Expected timer to fire";
constexpr const char* should_be_scheduled_message = "Timer should be scheduled";

// move the clock forward and yield so the timer can fire
ss::future<> advance_and_yield(std::chrono::milliseconds ms) {
    ss::manual_clock::advance(ms);
    co_await tests::drain_task_queue();
}

} // namespace

class head_minimum_interval_timer_fixture : public seastar_test {
public:
    using timer_type = minimum_interval_timer<
      minimum_interval_timer_type::head,
      ss::manual_clock>;

    ss::future<> SetUpAsync() override { co_return; }

    ss::future<> TearDownAsync() override {
        co_await tests::drain_task_queue();
    }

    void setup_callback() {
        _timer.set_callback([this] {
            _tick_count++;
            _last_tick_time = ss::manual_clock::now();
        });
    }

    int tick_count() const { return _tick_count; }

    ss::manual_clock::time_point last_tick_time() const {
        return _last_tick_time;
    }

    timer_type& timer() { return _timer; }

private:
    timer_type _timer{minimum_interval};
    int _tick_count{0};
    ss::manual_clock::time_point _last_tick_time{
      ss::manual_clock::time_point::min()};
};

// ============================================================================
// 1. Basic Scheduling Tests
// ============================================================================

TEST_F_CORO(
  head_minimum_interval_timer_fixture, basic_scheduling_with_time_point) {
    auto now = ss::manual_clock::now();
    auto target_time = now + 200ms;

    timer().request_tick(target_time);

    auto scheduled_time = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled_time.has_value()) << should_be_scheduled_message;
    EXPECT_EQ(*scheduled_time, target_time)
      << "Scheduling with a particular time point should yield that time point";
    co_return;
}

TEST_F_CORO(
  head_minimum_interval_timer_fixture, basic_scheduling_with_duration) {
    auto now = ss::manual_clock::now();

    timer().request_tick(300ms);

    auto scheduled_time = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled_time.has_value()) << should_be_scheduled_message;
    EXPECT_EQ(*scheduled_time, now + 300ms)
      << "Scheduling with a duration should yield a scheduled time of now() "
         "plus the provided duration";
    co_return;
}

// ============================================================================
// 2. Minimum Interval Enforcement Tests
// ============================================================================

TEST_F_CORO(
  head_minimum_interval_timer_fixture,
  minimum_interval_not_enforced_from_start) {
    auto now = ss::manual_clock::now();
    timer().request_tick(now);

    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;
    EXPECT_EQ(*scheduled, now)
      << "The first scheduling of bounded timer should have no restrictions";
    co_return;
}

TEST_F_CORO(
  head_minimum_interval_timer_fixture, minimum_interval_enforced_after_firing) {
    setup_callback();

    // burn the first tick so we have a 'last ticked' which is not -inf
    timer().request_tick(0ms);
    co_await advance_and_yield(arbitrary_interval);
    EXPECT_EQ(tick_count(), 1) << should_have_fired_message;

    auto now_after_fire = ss::manual_clock::now();

    // Try to schedule too soon after firing (30ms < 100ms minimum)
    timer().request_tick(now_after_fire + 30ms);

    // Should be scheduled at minimum interval from last fire
    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;
    EXPECT_EQ(*scheduled, now_after_fire + minimum_interval)
      << "A timer for which the user attempts to reschedule below the minimum "
         "interval should instead be rescheduled to a minimum interval after "
         "the last tick time";
}

TEST_F_CORO(head_minimum_interval_timer_fixture, minimum_interval_exactly_met) {
    setup_callback();
    auto now = ss::manual_clock::now();

    timer().request_tick(now + minimum_interval);
    co_await advance_and_yield(minimum_interval);
    EXPECT_EQ(tick_count(), 1) << should_have_fired_message;

    timer().request_tick(minimum_interval);

    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value())
      << "Timer should have accepted a reschedule to exactly the minimum "
         "interval";
    EXPECT_EQ(*scheduled, ss::manual_clock::now() + minimum_interval)
      << "Timer should be rescheduled to exactly the minimum interval";

    co_return;
}

// ============================================================================
// 3. Rescheduling Logic Tests
// ============================================================================

TEST_F_CORO(head_minimum_interval_timer_fixture, reschedule_to_earlier_time) {
    auto now = ss::manual_clock::now();
    timer().request_tick(now + 1s);

    // Request earlier time
    timer().request_tick(now + 200ms);

    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;

    EXPECT_EQ(*scheduled, now + 200ms)
      << "Timer's rescheduled value should equal the earlier provided time";
    co_return;
}

TEST_F_CORO(
  head_minimum_interval_timer_fixture, reschedule_to_later_time_keeps_current) {
    auto now = ss::manual_clock::now();

    timer().request_tick(now + 200ms);

    auto original_time = timer().get_scheduled_time();
    EXPECT_TRUE(original_time) << should_be_scheduled_message;

    timer().request_tick(now + 500ms);

    auto scheduled = timer().get_scheduled_time();
    EXPECT_EQ(scheduled, original_time)
      << "Timer should have honored the originally scheduled time";
    co_return;
}

// ============================================================================
// 4. Past Time Handling Tests
// ============================================================================

TEST_F_CORO(
  head_minimum_interval_timer_fixture, scheduling_in_past_adjusts_to_now) {
    ss::manual_clock::advance(1s);
    auto now = ss::manual_clock::now();

    // Try to schedule in the past
    auto past_time = now - 500ms;
    timer().request_tick(past_time);

    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;
    EXPECT_EQ(*scheduled, now)
      << "A timer scheduled in the past should floor to \"now\"";
    co_return;
}

TEST_F_CORO(
  head_minimum_interval_timer_fixture,
  past_time_still_respects_minimum_interval) {
    setup_callback();
    timer().set_minimum_interval(200ms);

    timer().request_tick(200ms);
    co_await advance_and_yield(200ms);

    EXPECT_EQ(tick_count(), 1) << should_have_fired_message;
    auto last_tick_time = this->last_tick_time();
    timer().request_tick(last_tick_time - 100ms);

    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;

    EXPECT_EQ(*scheduled, last_tick_time + 200ms)
      << "Next tick time should respect minimum interval, even if scheduled in "
         "the past";
    co_return;
}

// ============================================================================
// 5. Minimum Interval Updates Tests
// ============================================================================

TEST_F_CORO(head_minimum_interval_timer_fixture, decrease_minimum_interval) {
    setup_callback();
    timer().set_minimum_interval(200ms);

    auto now = ss::manual_clock::now();
    timer().request_tick(now + 300ms);
    auto original_time = timer().get_scheduled_time();
    EXPECT_TRUE(original_time) << should_be_scheduled_message;

    // Decrease minimum interval
    timer().set_minimum_interval(50ms);

    // Existing schedule should remain unchanged
    EXPECT_EQ(timer().get_scheduled_time(), original_time)
      << "Lowering the minimum interval should not lower the currently "
         "scheduled time";

    // New scheduling should use shorter interval
    timer().request_tick(now + 60ms);
    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;
    EXPECT_EQ(*scheduled, now + 60ms)
      << "Scheduling requests after lowering the minimum interval should use "
         "the new minimum interval";
    co_return;
}

TEST_F_CORO(
  head_minimum_interval_timer_fixture,
  increase_minimum_interval_after_recent_fire) {
    setup_callback();

    timer().request_tick(ss::manual_clock::now() + 50ms);

    co_await advance_and_yield(50ms);
    EXPECT_EQ(tick_count(), 1) << should_have_fired_message;

    auto last_tick_time = this->last_tick_time();
    timer().request_tick(last_tick_time + 100ms);

    auto scheduled_before = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled_before.has_value()) << should_be_scheduled_message;

    timer().set_minimum_interval(500ms);

    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;

    auto expected_min = last_tick_time + 500ms;
    EXPECT_EQ(*scheduled, expected_min)
      << "Timer should be rescheduled to respect new minimum from last fire";
}

// ============================================================================
// 6. Edge Cases Tests
// ============================================================================

TEST_F_CORO(head_minimum_interval_timer_fixture, initial_state) {
    EXPECT_FALSE(timer().get_scheduled_time().has_value())
      << "Timer should not be scheduled initially";
    co_return;
}

TEST_F_CORO(head_minimum_interval_timer_fixture, is_tick_requested_lifecycle) {
    setup_callback();

    EXPECT_FALSE(timer().is_tick_requested())
      << "No tick should be requested initially";

    timer().request_tick(ss::manual_clock::now() + 200ms);
    EXPECT_TRUE(timer().is_tick_requested())
      << "Tick should be requested after request_tick";

    timer().cancel();
    EXPECT_FALSE(timer().is_tick_requested())
      << "No tick should be requested after cancel";

    timer().request_tick(0ms);
    co_await advance_and_yield(arbitrary_interval);
    EXPECT_EQ(tick_count(), 1) << should_have_fired_message;
    EXPECT_FALSE(timer().is_tick_requested())
      << "No tick should be requested after firing with no new request";
    co_return;
}

TEST_F_CORO(head_minimum_interval_timer_fixture, zero_minimum_interval) {
    setup_callback();
    timer().set_minimum_interval(0ms);

    auto now = ss::manual_clock::now();

    timer().request_tick(now);
    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;
    EXPECT_EQ(*scheduled, now)
      << "With zero minimum interval, we should be immediately scheduled";

    // manual clock advancement fires expired timers, the tick should fire
    ss::manual_clock::advance(0ms);
    EXPECT_EQ(tick_count(), 1) << should_have_fired_message;

    co_return;
}

// ============================================================================
// 7. Additional Tests
// ============================================================================

TEST_F_CORO(head_minimum_interval_timer_fixture, constructor_with_callback) {
    // Create timer with callback in constructor
    int local_tick_count = 0;
    timer_type timer_with_cb(
      minimum_interval, [&local_tick_count] { local_tick_count++; });

    timer_with_cb.request_tick(ss::manual_clock::now());
    co_await advance_and_yield(minimum_interval);

    EXPECT_EQ(local_tick_count, 1)
      << "The timer should have bound to the callback provided at construction";
    timer_with_cb.cancel();
}

TEST_F_CORO(head_minimum_interval_timer_fixture, set_callback_replaces_prior) {
    int first_callback_count = 0;
    int second_callback_count = 0;

    timer().set_callback([&first_callback_count] { first_callback_count++; });
    timer().set_callback([&second_callback_count] { second_callback_count++; });

    timer().request_tick(ss::manual_clock::now());
    co_await advance_and_yield(minimum_interval);

    EXPECT_EQ(first_callback_count, 0)
      << "Replaced callback should not have fired";
    EXPECT_EQ(second_callback_count, 1)
      << "Replacement callback should have fired";
    co_return;
}

TEST_F_CORO(
  head_minimum_interval_timer_fixture, rapid_tick_requests_respect_interval) {
    setup_callback();

    // Make multiple rapid requests
    for (int i = 0; i < 5; i++) {
        timer().request_tick(ss::manual_clock::now());
        co_await advance_and_yield(10ms);
    }

    // Should only have fired once due to minimum interval
    EXPECT_EQ(tick_count(), 1)
      << "Rapid requests should respect minimum interval";
}

TEST_F_CORO(
  head_minimum_interval_timer_fixture, interval_between_successive_fires) {
    setup_callback();

    timer().request_tick(ss::manual_clock::now());
    co_await advance_and_yield(arbitrary_interval);
    EXPECT_EQ(tick_count(), 1) << should_have_fired_message;
    auto first_tick_time = last_tick_time();

    timer().request_tick(ss::manual_clock::now());

    co_await advance_and_yield(minimum_interval);
    EXPECT_EQ(tick_count(), 2) << "Expected timer to fire again";
    auto second_tick_time = last_tick_time();

    auto interval = second_tick_time - first_tick_time;
    EXPECT_GE(interval, minimum_interval)
      << "Actual time between ticks should be at least the minimum interval";
}

// ============================================================================
// 8. Head Bounded Timer - Async Callback Tests
// ============================================================================

TEST_F_CORO(
  head_minimum_interval_timer_fixture, async_request_tick_inside_callback) {
    ss::condition_variable callback_done{};
    int local_tick_count = 0;
    timer().set_callback(
      [this, &local_tick_count, &callback_done](this auto) -> ss::future<> {
          local_tick_count++;
          // The head timer's close_guard is already destroyed, so request_tick
          // should schedule normally (no in-flight blocking).
          timer().request_tick(ss::manual_clock::now() + minimum_interval);

          auto scheduled = timer().get_scheduled_time();
          EXPECT_TRUE(scheduled.has_value())
            << "request_tick inside an async head callback should schedule the "
               "next tick";
          callback_done.signal();
          co_return;
      });

    timer().request_tick(ss::manual_clock::now());
    co_await advance_and_yield(minimum_interval);
    co_await callback_done.wait();

    EXPECT_EQ(local_tick_count, 1) << should_have_fired_message;
}

TEST_F_CORO(
  head_minimum_interval_timer_fixture, async_fires_again_while_prev_running) {
    // An async head callback that blocks on a condition variable.
    // The timer should be able to fire a second time while the first callback
    // is still "running" because head timers complete at invocation time.
    ss::condition_variable hold_first_open{};
    int local_tick_count = 0;
    timer().set_callback(
      [&local_tick_count, &hold_first_open](this auto) -> ss::future<> {
          local_tick_count++;
          if (local_tick_count == 1) {
              co_await hold_first_open.wait();
          }
      });

    // First tick — callback will suspend
    timer().request_tick(ss::manual_clock::now());
    co_await advance_and_yield(minimum_interval);

    EXPECT_EQ(local_tick_count, 1) << should_have_fired_message;

    // Second tick — should fire even though the first callback is suspended
    timer().request_tick(ss::manual_clock::now());
    co_await advance_and_yield(minimum_interval);

    EXPECT_EQ(local_tick_count, 2)
      << "Head timer should fire again while previous async callback is still "
         "running";

    // Release the first callback so there are no abandoned futures
    hold_first_open.signal();
}

// ============================================================================
// 9. Tail Bounded Timer - Sync Callback Tests
// ============================================================================

class tail_minimum_interval_timer_fixture : public seastar_test {
public:
    using timer_type = minimum_interval_timer<
      minimum_interval_timer_type::tail,
      ss::manual_clock>;

    // Callbacks advance the clock by this amount to simulate execution time.
    // Tests can then deduce when the tick was considered complete from the
    // next scheduled time.
    static constexpr auto simulated_work_duration = 50ms;

    ss::future<> SetUpAsync() override { co_return; }
    ss::future<> TearDownAsync() override {
        // ensure that there are no trailing .finally callbacks on test
        // completion
        co_await tests::drain_task_queue();
    }

    void setup_sync_callback() {
        _timer.set_callback([this] {
            _tick_count++;
            ss::manual_clock::advance(simulated_work_duration);
        });
    }

    void setup_async_callback() {
        _timer.set_callback([this](this auto) -> ss::future<> {
            _tick_count++;

            co_await tests::drain_task_queue();
            callback_entered.signal();
            co_await callback_release.wait();
            ss::manual_clock::advance(simulated_work_duration);
        });
    }

    // waits until the user defined async is inflight
    ss::future<> wait_for_callback_inflight(int count_before = 0) {
        co_await callback_entered.wait();
        EXPECT_EQ(tick_count(), count_before + 1) << should_have_fired_message;
    }

    // forces the callback to finish so there's no abandoned futures
    ss::future<> finish_callback() {
        callback_release.signal();
        co_await tests::drain_task_queue();
    }

    // start the callback and finish it so there's no abandoned futures
    ss::future<> start_and_finish_callback(int count_before = 0) {
        co_await wait_for_callback_inflight(count_before);
        co_await finish_callback();
    }

    int tick_count() const { return _tick_count; }

    timer_type& timer() { return _timer; }

protected:
    // callback signals this to tell the test fiber it is in flight
    ss::condition_variable callback_entered{};
    // test signals this to tell the callback fiber it can finish
    ss::condition_variable callback_release{};

private:
    timer_type _timer{minimum_interval};
    int _tick_count{0};
};

TEST_F_CORO(tail_minimum_interval_timer_fixture, sync_callback_fires) {
    setup_sync_callback();
    timer().request_tick(ss::manual_clock::now());
    co_await advance_and_yield(minimum_interval);
    EXPECT_EQ(tick_count(), 1) << should_have_fired_message;
}

TEST_F_CORO(
  tail_minimum_interval_timer_fixture, sync_interval_measured_from_completion) {
    setup_sync_callback();
    auto start = ss::manual_clock::now();
    timer().request_tick(0ms);
    co_await advance_and_yield(minimum_interval);
    EXPECT_EQ(tick_count(), 1) << should_have_fired_message;

    // Request next tick immediately
    timer().request_tick(ss::manual_clock::now());

    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;

    // callback advances time by simulated_work_duration
    // so start + minimum interval is tick start
    // tick end is tick_start + simulated_work_duration
    auto tick_completion = start + minimum_interval + simulated_work_duration;
    EXPECT_EQ(*scheduled, tick_completion + minimum_interval)
      << "Minimum interval should be measured from callback completion, not "
         "invocation";
}

TEST_F_CORO(
  tail_minimum_interval_timer_fixture,
  sync_request_during_callback_is_deferred) {
    bool did_fire{false};
    std::optional<ss::manual_clock::time_point> scheduled_during_callback;
    timer().set_callback([this, &scheduled_during_callback, &did_fire] {
        did_fire = true;
        ss::manual_clock::advance(simulated_work_duration);
        timer().request_tick(ss::manual_clock::now() + 200ms);
        scheduled_during_callback = timer().get_scheduled_time();
    });

    timer().request_tick(ss::manual_clock::now());
    co_await advance_and_yield(minimum_interval);
    EXPECT_TRUE(did_fire) << should_have_fired_message;

    EXPECT_FALSE(scheduled_during_callback.has_value())
      << "Request during tail sync callback should be deferred while in-flight";

    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value())
      << "Deferred request should be scheduled after callback completes";
    EXPECT_EQ(scheduled.value(), ss::manual_clock::now() + 200ms);
}

// ============================================================================
// 10. Tail Bounded Timer - Async In-Flight Behavior
// ============================================================================

TEST_F_CORO(tail_minimum_interval_timer_fixture, async_callback_fires) {
    setup_async_callback();
    timer().request_tick(ss::manual_clock::now());
    ss::manual_clock::advance(arbitrary_interval);
    co_await start_and_finish_callback();
}

TEST_F_CORO(
  tail_minimum_interval_timer_fixture, not_scheduled_during_in_flight) {
    setup_async_callback();
    timer().request_tick(ss::manual_clock::now());
    ss::manual_clock::advance(arbitrary_interval);
    co_await wait_for_callback_inflight();

    EXPECT_FALSE(timer().get_scheduled_time().has_value())
      << "Timer should not report a scheduled time while async op is "
         "in-flight";

    co_await finish_callback();
}

TEST_F_CORO(
  tail_minimum_interval_timer_fixture,
  request_during_in_flight_deferred_until_completion) {
    setup_async_callback();
    timer().request_tick(ss::manual_clock::now());
    ss::manual_clock::advance(arbitrary_interval);

    co_await wait_for_callback_inflight();

    timer().request_tick(ss::manual_clock::now() + 200ms);

    EXPECT_FALSE(timer().get_scheduled_time().has_value())
      << "Timer should not schedule while in-flight";

    co_await finish_callback();

    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value())
      << "Deferred request should be scheduled after async op completes";
}

TEST_F_CORO(
  tail_minimum_interval_timer_fixture, no_reschedule_without_pending_request) {
    setup_async_callback();
    timer().request_tick(ss::manual_clock::now());
    ss::manual_clock::advance(arbitrary_interval);
    co_await this->start_and_finish_callback();
    EXPECT_FALSE(timer().get_scheduled_time().has_value())
      << "Timer should not be scheduled after completion with no pending "
         "request";
}

TEST_F_CORO(
  tail_minimum_interval_timer_fixture,
  is_tick_requested_with_pending_during_in_flight) {
    setup_async_callback();
    timer().request_tick(ss::manual_clock::now());
    ss::manual_clock::advance(arbitrary_interval);
    co_await wait_for_callback_inflight();

    // In-flight with no pending request: timer is not armed (it already fired)
    // and _next_requested was consumed when the tick was scheduled
    EXPECT_FALSE(timer().is_tick_requested())
      << "No tick should be requested during in-flight with no pending request";
    EXPECT_FALSE(timer().get_scheduled_time().has_value())
      << "Timer should not be armed during in-flight";

    // Request during in-flight: _next_requested gets set but timer stays
    // unarmed
    timer().request_tick(ss::manual_clock::now() + 200ms);
    EXPECT_TRUE(timer().is_tick_requested())
      << "Tick should be requested when pending request exists during "
         "in-flight";
    EXPECT_FALSE(timer().get_scheduled_time().has_value())
      << "Timer should not be armed during in-flight even with pending request";

    co_await finish_callback();
    co_return;
}

// ============================================================================
// 11. Tail Bounded Timer - Minimum Interval From Completion
// ============================================================================

TEST_F_CORO(
  tail_minimum_interval_timer_fixture,
  async_interval_measured_from_completion) {
    setup_async_callback();
    auto start = ss::manual_clock::now();
    timer().request_tick(start);
    ss::manual_clock::advance(arbitrary_interval);
    co_await wait_for_callback_inflight();

    timer().request_tick(ss::manual_clock::now());

    co_await finish_callback();

    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;

    // tick_start = start + arbitrary_interval
    // tick end = tick_start + simulated_work_duration
    auto tick_completion = start + arbitrary_interval + simulated_work_duration;
    EXPECT_EQ(*scheduled, tick_completion + minimum_interval)
      << "Minimum interval should be measured from async completion, not "
         "invocation";
}

TEST_F_CORO(
  tail_minimum_interval_timer_fixture,
  increase_minimum_interval_during_in_flight) {
    setup_async_callback();
    timer().request_tick(ss::manual_clock::now());
    ss::manual_clock::advance(arbitrary_interval);
    co_await wait_for_callback_inflight();

    // Request a tick and then increase the minimum interval while in-flight.
    // The pending request gets replayed in on_guard_close after _last_fired is
    // set, so the new (larger) minimum interval should apply.
    timer().request_tick(ss::manual_clock::now() + 10ms);
    timer().set_minimum_interval(500ms);

    co_await finish_callback();

    auto completion_time = ss::manual_clock::now();
    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;
    EXPECT_EQ(*scheduled, completion_time + 500ms)
      << "Deferred request replayed after completion should respect the new "
         "minimum interval";
    co_return;
}

// ============================================================================
// 12. Tail Bounded Timer - Multiple Requests and Cancellation
// ============================================================================

TEST_F_CORO(
  tail_minimum_interval_timer_fixture, earliest_request_wins_during_in_flight) {
    setup_async_callback();
    timer().request_tick(ss::manual_clock::now());
    ss::manual_clock::advance(arbitrary_interval);
    co_await wait_for_callback_inflight();

    auto now = ss::manual_clock::now();
    timer().request_tick(now + 500ms);
    timer().request_tick(now + 200ms);
    timer().request_tick(now + 800ms);

    co_await finish_callback();

    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;
    EXPECT_EQ(*scheduled, now + 200ms)
      << "Earliest of multiple in-flight requests should be used";
}

TEST_F_CORO(tail_minimum_interval_timer_fixture, cancel_during_in_flight) {
    setup_async_callback();
    timer().request_tick(ss::manual_clock::now());
    ss::manual_clock::advance(arbitrary_interval);
    co_await wait_for_callback_inflight();

    timer().request_tick(ss::manual_clock::now() + 200ms);
    timer().cancel();

    co_await finish_callback();

    EXPECT_FALSE(timer().get_scheduled_time().has_value())
      << "Cancel during in-flight should prevent deferred request from "
         "scheduling";
    EXPECT_EQ(tick_count(), 1) << "No additional tick should fire after cancel";
}

// ============================================================================
// 13. Tail Bounded Timer - Sequential Ticks
// ============================================================================

TEST_F_CORO(
  tail_minimum_interval_timer_fixture,
  sequential_async_ticks_respect_interval) {
    setup_async_callback();

    // First tick
    timer().request_tick(0ms);
    ss::manual_clock::advance(arbitrary_interval);

    // Complete first async op
    co_await start_and_finish_callback();
    auto first_completion = ss::manual_clock::now();

    // Second tick — request immediately after first completes
    timer().request_tick(first_completion);

    auto scheduled = timer().get_scheduled_time();
    EXPECT_TRUE(scheduled.has_value()) << should_be_scheduled_message;
    EXPECT_GE(*scheduled, first_completion + minimum_interval)
      << "Second tick should respect minimum interval from first completion";

    // advance is a scheduling point because incrementing time processes expired
    // timers, be sure to grab the tick count BEFORE the callback fires
    auto count_before_second = tick_count();
    ss::manual_clock::advance(minimum_interval);

    // Complete second async op
    co_await start_and_finish_callback(count_before_second);
    auto second_completion = ss::manual_clock::now();

    auto interval = second_completion - first_completion;
    EXPECT_GE(interval, minimum_interval)
      << "Interval between completions should be at least the minimum "
         "interval";
}

// ============================================================================
// 14. Tail Bounded Timer - Timer dies before callback
// ============================================================================

// The goal of this test is to show that a timer can be destructed while one of
// its spawned callbacks is in flight. This is in answer to a timer internal,
// where timer attaches a callback to a .finally on the user provided callback.
// Because this .finally may call back into the timer, destruction of the timer
// may lead to a use after free. This test checks that the current
// implementation of the timer guards against this.
TEST_F_CORO(tail_minimum_interval_timer_fixture, timer_dies_before_callback) {
    ss::condition_variable wait_for_in_flight{};
    ss::condition_variable hold_callback_open{};
    bool callback_is_in_flight{false};
    {
        timer_type local_timer{
          minimum_interval,
          [&wait_for_in_flight, &hold_callback_open, &callback_is_in_flight](
            this auto) -> ss::future<> {
              callback_is_in_flight = true;
              wait_for_in_flight.signal();
              co_await hold_callback_open.wait();
              callback_is_in_flight = false;
          }};

        local_timer.request_tick(ss::manual_clock::now());
        ss::manual_clock::advance(minimum_interval);
        co_await wait_for_in_flight.wait();

        EXPECT_TRUE(callback_is_in_flight) << "Callback should be in progress";
    }
    // the timer has been destructed

    hold_callback_open.signal();
    co_await tests::drain_task_queue();

    // there are actually three points in time which are relevant
    // A: the destruction time of the timer
    // B: the completion of the user provided callback
    //        this is indistinguishable from the point at which
    //        callback_is_in_flight becomes false
    // C: the destruction of the timer's close_guard
    //        this occurs in a .finally after completion of the user callback
    // This test is checking that when A occurs before C, nothing bad happens
    // The below check is that B has completed, which is distinct from C because
    // there may be a scheduling point on a .finally
    // Because, though, we have called tests::drain_task_queue, we can know that
    // the C has occurred here as well
    EXPECT_FALSE(callback_is_in_flight)
      << "Callback should have completed after signalling and draining";

    // if nothing awful has happened by now, the test has passed
}

} // namespace ssx
