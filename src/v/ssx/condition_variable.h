/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include "container/intrusive_list_helpers.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/timer.hh>

#include <exception>
#include <type_traits>

namespace ssx {

// A condition variable that supports abort sources in addition to
// timeouts.
class condition_variable {
public:
    /// Constructs a condition_variable object.
    condition_variable() noexcept = default;
    condition_variable&
    operator=(const condition_variable& rhs) noexcept = delete;
    condition_variable& operator=(condition_variable&& rhs) noexcept = delete;
    condition_variable(const condition_variable& rhs) noexcept = delete;
    condition_variable(condition_variable&& rhs) noexcept = delete;
    ~condition_variable();

    // Wait for the condition variable to be signalled.
    // If the abort_source is notified then the future will resolve with the
    // exception from the abort_source.
    ss::future<> wait(ss::abort_source&) noexcept;

    // Wait for the condition variable to be signalled within the deadline.
    //
    // If the timepoint is reached then the future fails with
    // ss::condition_variable_timed_out
    ss::future<> wait(ss::lowres_clock::time_point) noexcept;
    ss::future<> wait(ss::manual_clock::time_point) noexcept;

    // Wait for the condition variable to be signalled within the deadline.
    //
    // If the timepoint is reached then the future fails with
    // ss::condition_variable_timed_out
    //
    // If the abort_source is notified then the future will resolve with the
    // exception from the abort_source.
    ss::future<> wait(ss::manual_clock::time_point, ss::abort_source&) noexcept;
    ss::future<> wait(ss::lowres_clock::time_point, ss::abort_source&) noexcept;

    // Wait for the condition variable to be signalled until the predicate
    // returns true.
    //
    // If the abort_source is notified then the future will
    // resolve with the exception from the abort_source.
    template<typename Pred>
    requires std::is_invocable_r_v<bool, Pred>
    ss::future<> wait(ss::abort_source& as, Pred&& pred) noexcept {
        return ss::do_until(
          std::forward<Pred>(pred), [this, &as] { return wait(as); });
    }

    // Wait for the condition variable to be signalled within the deadline until
    // the predicate returns true.
    //
    // If the timepoint is reached then the future fails with
    // ss::condition_variable_timed_out
    template<
      typename Clock = typename ss::lowres_clock,
      typename Duration = typename Clock::duration,
      typename Pred>
    requires std::is_invocable_r_v<bool, Pred>
    ss::future<>
    wait(std::chrono::time_point<Clock, Duration> tp, Pred&& pred) noexcept {
        return ss::do_until(
          std::forward<Pred>(pred), [this, tp] { return wait(tp); });
    }

    // Wait for the condition variable to be signalled within the deadline until
    // the predicate returns true.
    //
    // If the timepoint is reached then the future fails with
    // ss::condition_variable_timed_out
    //
    // If the abort_source is notified then the future will resolve with the
    // exception from the abort_source.
    template<
      typename Clock = typename ss::lowres_clock,
      typename Duration = typename Clock::duration,
      typename Pred>
    requires std::is_invocable_r_v<bool, Pred>
    ss::future<> wait(
      std::chrono::time_point<Clock, Duration> tp,
      ss::abort_source& as,
      Pred&& pred) noexcept {
        return ss::do_until(
          std::forward<Pred>(pred), [this, tp, &as] { return wait(tp, as); });
    }

    // If there are any pending waiters for the condition variable.
    bool has_waiters() const noexcept { return !_waiters.empty(); }
    // Notify and wake up a single waiter.
    void signal() noexcept;
    // Notify and wake up all waiters.
    void broadcast() noexcept;
    // Signal to waiters that an error occurred. `wait` will see an exceptional
    // future<> containing the provided exception parameter. The future is made
    // available immediately.
    void broken() noexcept;
    // The same as broken, but specify the exception that waiters will see.
    void broken(const std::exception_ptr&) noexcept;

private:
    struct waiter : public ss::promise<> {
        waiter() = default;
        waiter(const waiter&) = delete;
        waiter(waiter&&) = delete;
        waiter& operator=(const waiter&) = delete;
        waiter& operator=(waiter&&) = delete;
        virtual ~waiter() = default;
        void signal() noexcept;
        void set_exception(const std::exception_ptr& ex) noexcept;

        intrusive_list_hook hook;
    };
    struct abort_subscription_holder {
        ss::abort_source::subscription sub;
    };
    friend bool
    setup_waiter(waiter&, ss::abort_source&, abort_subscription_holder&);
    friend bool setup_waiter(
      waiter&, ss::manual_clock::time_point, ss::timer<ss::manual_clock>&);
    friend void setup_waiter(
      waiter&, ss::lowres_clock::time_point, ss::timer<ss::lowres_clock>&);
    void add_waiter(waiter&) noexcept;
    bool wakeup_first() noexcept;
    bool check_and_consume_signal() noexcept;

    intrusive_list<waiter, &waiter::hook> _waiters;
    std::exception_ptr _ex;
    bool _signalled = false;
};

} // namespace ssx
