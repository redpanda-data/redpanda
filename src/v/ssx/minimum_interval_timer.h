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

#pragma once
#include "base/seastarx.h"
#include "base/vassert.h"
#include "ssx/future-util.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <optional>

namespace ssx {

// A timer with utilities for limiting the maximum frequency of timer firings
// details:
// 1. This timer guarantees that after a tick terminates, no additional tick
//    will fire sooner than a provided minimum duration
//
// 2. A ticks termination is determined by the destruction of a guard which is
//    passed to the timer's callback
//
// 3. Ticks will be rescheduled to the earliest possible time according to the
//    following rules
//    a. no scheduled tick may be closer than the provided minimum duration to
//       the prior tick
//    b. no tick may be scheduled in the past
//    c. no tick may be scheduled while another tick is ongoing
//       i. the request to schedule will be noted, and a rescheduling attempt
//          will occur on termination of the inflight tick (unless cancelled)
template<typename Clock>
class minimum_interval_timer_base
  : public ss::weakly_referencable<minimum_interval_timer_base<Clock>> {
private: // internal type definitions
    // convenience wrapper on time_point with a magic value (max) to stand in as
    // 'invalid'
    struct next_requested_time_point {
    private:
        using time_point = Clock::time_point;
        static constexpr auto unscheduled_time = time_point::max();

    public:
        time_point time;

        next_requested_time_point()
          : time{unscheduled_time} {}

        // NOLINTNEXTLINE(hicpp-explicit-conversions)
        next_requested_time_point(time_point time)
          : time{time} {}

        bool is_requested() const { return time != time_point::max(); }
        void reset() { time = unscheduled_time; }
    };

protected: // types, type aliases and friends
    // using this instead of ss::defer because the type of defer bound to
    // a member of an incomplete template is a beast
    //
    // holds a weak reference to its parent timer, on destruction invokes
    // on_guard_close
    struct close_guard {
        // weak because the lifecycle of user spawned fibers should NOT be tied
        // to the timer that happens to have spawned them (to keep the pointer
        // valid)
        ss::weak_ptr<minimum_interval_timer_base> parent;

        explicit close_guard(ss::weak_ptr<minimum_interval_timer_base> parent)
          : parent(std::move(parent)) {}

        close_guard(const close_guard&) = delete;
        close_guard& operator=(const close_guard&) = delete;
        close_guard(close_guard&&) = default;
        close_guard& operator=(close_guard&&) = default;

        ~close_guard() noexcept {
            if (!parent) {
                return;
            }
            parent->on_guard_close();
        }
    };

    using self_t = minimum_interval_timer_base<Clock>;
    using clock_t = Clock;
    using time_point_t = typename Clock::time_point;
    using duration_t = typename Clock::duration;

public: // publicly usable types
    using callback_t = ss::noncopyable_function<void(close_guard)>;

private:
    // underlying timer, holds the timepoint of the next tick if requested, and
    // if theres no other tick in flight
    ss::timer<Clock> _timer;

    // last completion time as governed by on_guard_close
    time_point_t _last_fired{time_point_t::min()};

    // minimum duration between _last_fired and subsequent ticks
    duration_t _minimum_interval;

    // buffers the next tick request while a tick is currently inflight
    next_requested_time_point _next_requested{};

    // guard to track if theres a tick in flight, gets reset by close_guard on
    // destruction
    bool _is_in_flight{false};

private: // detail methods for close_guard
    void on_guard_close() {
        // closing of the guard signifies that the tick has completed
        // in flight is obviously now false, last fired is now, and if there is
        // a backlogged request resend it now
        _is_in_flight = false;
        _last_fired = clock_t::now();
        if (_next_requested.is_requested()) {
            // request tick will consume _next_requested
            request_tick(_next_requested.time);
        }
    }

    // close_guard factory
    close_guard open_in_flight_guard() {
        vassert(!_is_in_flight, "inflight guard should never be opened twice");
        _is_in_flight = true;
        return close_guard{this->weak_from_this()};
    }

public:
    explicit minimum_interval_timer_base(duration_t minimum_interval) noexcept
      : _minimum_interval{minimum_interval} {}

    /**
     * Requests a tick to occur at the specified point in time.
     *
     * Schedules the timer if appropriate to do so.
     *
     * The timer will never be rescheduled while a tick is ongoing, but the
     *     request will be noted and retried on timer completion
     *
     * The timer may never be scheduled to fire closer than _minimum_interval to
     *     _last_fired
     *
     * The timer may never be scheduled to fire in the past
     *
     * The timer will be rescheduled to the soonest point in time that does
     *     not violate these constraints
     */
    void request_tick(time_point_t requested_time) {
        // store requested, bail out if theres a tick in flight
        _next_requested = std::min(requested_time, _next_requested.time);
        if (_is_in_flight) {
            return;
        }

        // no tick is in flight, consume the requested time and attempt to
        // reschedule the underlying _timer
        requested_time = _next_requested.time;
        _next_requested.reset();

        requested_time = std::max(requested_time, clock_t::now());

        // grab the next tick time point or infinity
        const auto next_scheduled_time = _timer.armed() ? _timer.get_timeout()
                                                        : time_point_t::max();

        const auto soonest_allowable_time = _last_fired + _minimum_interval;

        // take lesser of the current schedule and requested schedule, bounded
        // by the minimum allowable interval
        auto to_reschedule_at = std::min(requested_time, next_scheduled_time);
        to_reschedule_at = std::max(to_reschedule_at, soonest_allowable_time);

        if (to_reschedule_at != next_scheduled_time) {
            _timer.rearm(to_reschedule_at);
        }
    }

    // convenience method for requesting a tick some duration from now
    void request_tick(std::chrono::milliseconds from_now) {
        return request_tick(
          clock_t::now() + std::chrono::duration_cast<duration_t>(from_now));
    }

    // change the minimum interval.
    // increasing the minimum_interval will move the next tick back if necessary
    // decreasing the minimum_interval will not reschedule the next tick any
    // sooner
    void set_minimum_interval(std::chrono::milliseconds minimum_interval) {
        _minimum_interval = std::chrono::duration_cast<duration_t>(
          minimum_interval);
        // kick the timeout further into the future if the timeout is now
        // inappropriately close to the last tick
        if (_timer.armed()) {
            request_tick(_timer.get_timeout());
        }
    }

    // sets the callback, replaces & discards any prior callback
    void set_callback(callback_t&& callback) {
        _timer.set_callback([this, callback = std::move(callback)] {
            std::invoke(callback, open_in_flight_guard());
        });
    }

    // cancels any requested or scheduled ticks
    void cancel() {
        _next_requested.reset();
        _timer.cancel();
    }

    // gets the timepoint of the next scheduled tick
    // note that ticks in _next_requested are not scheduled, because their exact
    // time is not yet known
    std::optional<time_point_t> get_scheduled_time() const {
        if (_timer.armed()) {
            return _timer.get_timeout();
        }
        return std::nullopt;
    }

    // if nothing changes, will there eventually be another tick?
    bool is_tick_requested() const {
        return _next_requested.is_requested() || _timer.armed();
    }
};

enum class minimum_interval_timer_type : int8_t { head, tail };

// wrappers for the two most common use cases
// head: a tick is complete as soon as the timer fires
// tail: a tick is complete when the user provided operation is complete
template<minimum_interval_timer_type timer_type, typename Clock>
class minimum_interval_timer : private minimum_interval_timer_base<Clock> {
    using base = minimum_interval_timer_base<Clock>;

private: // type aliases
    using callback_t = ss::noncopyable_function<void(void)>;
    using async_callback_t = ss::noncopyable_function<ss::future<>()>;

private: // internal only helper functions
    void set_sync_callback(callback_t callback) {
        auto wrapped_cb = [callback = std::move(callback)](
                            base::close_guard deferred_function) {
            // head timer, invoke the guard now
            if constexpr (timer_type == minimum_interval_timer_type::head) {
                // force destruction to invoke the
                // callback immediately
                auto(std::move(deferred_function));
            }
            callback();
            // tail timer, the guard will invoke itself
        };

        base::set_callback(std::move(wrapped_cb));
    }

    void set_async_callback(async_callback_t callback) {
        auto wrapped_cb = [callback = std::move(callback)](
                            base::close_guard deferred_function) {
            // head timer, we just destruct the close_guard right away
            if constexpr (timer_type == minimum_interval_timer_type::head) {
                {
                    // force destruction to invoke the
                    // callback immediately
                    auto(std::move(deferred_function));
                }
                ssx::background = callback();
                return;
            }
            // tail timer, attach the close guard to the user provided function
            // s.t. it can be completed after
            ssx::background = callback().finally(
              [deferred_function = std::move(deferred_function)] {});
        };

        base::set_callback(std::move(wrapped_cb));
    }

public:
    using minimum_interval_timer_base<Clock>::cancel;
    using minimum_interval_timer_base<Clock>::get_scheduled_time;
    using minimum_interval_timer_base<Clock>::is_tick_requested;
    using minimum_interval_timer_base<Clock>::request_tick;
    using minimum_interval_timer_base<Clock>::set_minimum_interval;

    explicit minimum_interval_timer(
      std::chrono::milliseconds minimum_interval) noexcept
      : base(
          std::chrono::duration_cast<typename base::duration_t>(
            minimum_interval)) {}

    template<typename Func>
    requires std::is_invocable_v<Func>
    minimum_interval_timer(
      std::chrono::milliseconds minimum_interval, Func callback) noexcept
      : base(
          std::chrono::duration_cast<typename base::duration_t>(
            minimum_interval)) {
        this->set_callback(std::move(callback));
    }

    // set the callback of the timer
    // accepts void(void) or ss::future<void>(void) callbacks
    // the caller is responsible for maintaining the lifetime guarantees of
    //     their own callbacks with regard to non-timer memory
    // namely, it is permissible to destroy the timer while a user callback is
    //     in flight, but the timer will not guarantee consistency of any other
    //     memory. If the user snaps [this] on an async callback, it is
    //     advisable that the user also cancels the timer on close, and use a
    //     gate + cancellation token to ensure graceful completion of their
    //     callback fibers
    template<typename Func>
    requires std::is_invocable_v<Func>
    void set_callback(Func func) {
        using result_t = std::invoke_result_t<Func>;
        if constexpr (std::is_void_v<result_t>) {
            set_sync_callback(std::move(func));
        } else {
            static_assert(
              std::is_same_v<result_t, ss::future<>>,
              "if not void returning must return ss::future<>");
            set_async_callback(std::move(func));
        }
    }
};

// ticks complete right when the timer fires
using head_minimum_interval_timer
  = minimum_interval_timer<minimum_interval_timer_type::head, ss::lowres_clock>;

// ticks complete when the user operation completes
using tail_minimum_interval_timer
  = minimum_interval_timer<minimum_interval_timer_type::tail, ss::lowres_clock>;

} // namespace ssx
