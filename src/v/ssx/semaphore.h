/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "ssx/deadlock_detector.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>

#include <utility>

#define ENABLE_DEADLOCK_DETECTOR

namespace ssx {

// We use named semaphores because the provided name will be included in
// exception messages, making diagnosing broken or timed-out semaphores much
// easier.

template<typename Clock = seastar::timer<>::clock>
class named_semaphore
  : public seastar::
      basic_semaphore<seastar::named_semaphore_exception_factory, Clock> {
public:
    named_semaphore(size_t count, seastar::sstring name)
#ifndef ENABLE_DEADLOCK_DETECTOR
      : seastar::
        basic_semaphore<seastar::named_semaphore_exception_factory, Clock>(
          count, seastar::named_semaphore_exception_factory{std::move(name)})
#else
      // We need to keep additional state to enable deadlock detector
      : seastar::
        basic_semaphore<seastar::named_semaphore_exception_factory, Clock>(
          count, seastar::named_semaphore_exception_factory{name})
      , _name(name)
#endif
    {
    }

#ifdef ENABLE_DEADLOCK_DETECTOR
    const seastar::sstring& get_name() const noexcept { return _name; }

private:
    seastar::sstring _name;
#endif
};

using semaphore = named_semaphore<>;

template<
  typename Clock = typename seastar::timer<>::clock,
  typename Monitor = ssx::details::failstop_monitor>
class dd_semaphore_units
  : public seastar::semaphore_units<
      seastar::named_semaphore_exception_factory> {
public:
    dd_semaphore_units() noexcept = default;

    dd_semaphore_units(semaphore& sem, size_t n) noexcept
      : seastar::semaphore_units<seastar::named_semaphore_exception_factory>(
        sem, n) {}

    dd_semaphore_units(
      semaphore& sem,
      size_t n,
      details::deadlock_detector_waiter_state<Monitor> state) noexcept
      : seastar::semaphore_units<seastar::named_semaphore_exception_factory>(
        sem, n)
      , _waiter(std::move(state)) {}

    dd_semaphore_units(dd_semaphore_units&& o) noexcept = default;

    dd_semaphore_units& operator=(dd_semaphore_units&& o) noexcept = default;

    dd_semaphore_units(const semaphore_units&) = delete;

    ~dd_semaphore_units() noexcept = default;

private:
    using waiter_state = details::deadlock_detector_waiter_state<Monitor>;
    std::optional<waiter_state> _waiter;
};

#ifndef ENABLE_DEADLOCK_DETECTOR

using semaphore_units = seastar::semaphore_units<semaphore::exception_factory>;

#else

using semaphore_units = dd_semaphore_units<>;
using dd_waiter_state
  = details::deadlock_detector_waiter_state<details::failstop_monitor>;

#endif

} // namespace ssx

namespace seastar {
#ifdef ENABLE_DEADLOCK_DETECTOR

template<
  typename Clock = typename timer<>::clock,
  typename Monitor = ssx::details::failstop_monitor>
future<ssx::dd_semaphore_units<Clock>>
get_units(ssx::named_semaphore<Clock>& sem, size_t units) noexcept {
    ssx::dd_waiter_state state(sem.get_name());
    return sem.wait(units).then([&sem, units, s = std::move(state)]() mutable {
        return ssx::dd_semaphore_units<Clock>{sem, units, std::move(s)};
    });
}

template<
  typename Clock = typename timer<>::clock,
  typename Monitor = ssx::details::failstop_monitor>
future<ssx::dd_semaphore_units<Clock, Monitor>> get_units(
  ssx::named_semaphore<Clock>& sem,
  size_t units,
  typename ssx::named_semaphore<Clock>::time_point timeout) noexcept {
    ssx::dd_waiter_state state(sem.get_name());
    return sem.wait(timeout, units)
      .then([&sem, units, s = std::move(state)]() mutable {
          return ssx::dd_semaphore_units<Clock, Monitor>{
            sem, units, std::move(s)};
      });
}

template<
  typename Clock = typename timer<>::clock,
  typename Monitor = ssx::details::failstop_monitor>
future<ssx::dd_semaphore_units<Clock, Monitor>> get_units(
  ssx::named_semaphore<Clock>& sem,
  size_t units,
  typename ssx::named_semaphore<Clock>::duration timeout) noexcept {
    ssx::dd_waiter_state state(sem.get_name());
    return sem.wait(timeout, units)
      .then([&sem, units, s = std::move(state)]() mutable {
          return ssx::dd_semaphore_units<Clock, Monitor>{
            sem, units, std::move(s)};
      });
}

template<
  typename Clock = typename timer<>::clock,
  typename Monitor = ssx::details::failstop_monitor>
future<ssx::dd_semaphore_units<Clock, Monitor>> get_units(
  ssx::named_semaphore<Clock>& sem, size_t units, abort_source& as) noexcept {
    ssx::dd_waiter_state state(sem.get_name());
    return sem.wait(as, units).then(
      [&sem, units, s = std::move(state)]() mutable {
          return ssx::dd_semaphore_units<Clock, Monitor>{
            sem, units, std::move(s)};
      });
}

template<
  typename Clock = typename timer<>::clock,
  typename Monitor = ssx::details::failstop_monitor>
std::optional<ssx::dd_semaphore_units<Clock, Monitor>>
try_get_units(ssx::named_semaphore<Clock>& sem, size_t units) noexcept {
    // deadlock-detector state is not created because this op can't deadlock
    if (!sem.try_wait(units)) {
        return std::nullopt;
    }
    return std::make_optional<ssx::dd_semaphore_units<Clock, Monitor>>(
      sem, units);
}

template<
  typename Clock = typename timer<>::clock,
  typename Monitor = ssx::details::failstop_monitor>
ssx::dd_semaphore_units<Clock, Monitor>
consume_units(ssx::named_semaphore<Clock>& sem, size_t units) noexcept {
    // deadlock-detector state is not created because this op can't deadlock
    sem.consume(units);
    return ssx::dd_semaphore_units<Clock>{sem, units};
}

#endif
} // namespace seastar
