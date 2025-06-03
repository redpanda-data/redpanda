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

#include "base/vassert.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>

#include <fmt/format.h>

#include <cstdint>
#include <optional>

namespace ssx {

/// The callsite represents a single point in the code
/// which could invoke the execution monitor.
template<typename Clock = seastar::timer<>::clock>
struct execution_monitor_callsite {
    using time_point = typename Clock::time_point;
    using duration = typename Clock::duration;

    template<typename T>
    explicit execution_monitor_callsite(T&& name)
      : name(std::forward<T>(name)) {}

    /// Add checkpoint with the current time.
    /// The stall timeout will be reset.
    void checkpoint() noexcept {
        last_check_time = Clock::now();
        ++check_count;
        _mem_buf = fmt::memory_buffer();
        _truncated = false;
    }

    /// Suspend monitoring for a specified duration.
    /// This is useful when the controlled process has to sleep
    /// for know duration and we don't want to trigger the
    /// stall alarm.
    void suspend(duration d) noexcept {
        suspended_to = Clock::now() + d;
        ++suspend_count;
    }
    void suspend(time_point t) noexcept {
        suspended_to = t;
        ++suspend_count;
    }

    /// The name of the callsite.
    seastar::sstring name;

    /// The number of times the callsite has been checked.
    uint64_t check_count{0};

    /// The number of times the callsite has been suspended.
    uint64_t suspend_count{0};

    Clock::time_point last_check_time;
    Clock::time_point suspended_to;

    template<typename... Args>
    void log(fmt::format_string<Args...> format, Args&&... args) {
        if (_mem_buf.size() >= 4096) {
            // Don't log if the buffer is too large to avoid
            // memory issues.
            if (!_truncated) {
                fmt::format_to(
                  std::back_inserter(_mem_buf),
                  "[{}|{}|{}] truncated...\n",
                  name,
                  check_count,
                  suspend_count);
                _truncated = true;
            }
            return;
        }
        fmt::format_to(
          std::back_inserter(_mem_buf),
          "[{}|{}|{}] {}\n",
          name,
          check_count,
          suspend_count,
          fmt::vformat(
            format, fmt::make_format_args(std::forward<Args>(args)...)));
    }

    seastar::sstring str() const { return fmt::to_string(_mem_buf); }

    bool has_context_str() const noexcept { return _mem_buf.size() > 0; }

private:
    /// Buffer to format the messages.
    bool _truncated{false};
    fmt::memory_buffer _mem_buf;
};

/// The execution monitor uses this callsite to detect the unexpected
/// shutdown of the process.
template<typename Clock = seastar::timer<>::clock>
struct execution_monitor_shutdown_callsite {
    using time_point = typename Clock::time_point;
    using duration = typename Clock::duration;

    /// Add checkpoint with the current time.
    /// The stall timeout will be reset.
    void checkpoint() noexcept {
        last_check_time = Clock::now();
        ++check_count;
    }

    /// The name of the callsite.
    seastar::sstring name;

    /// The number of times the callsite has been checked.
    uint64_t check_count{0};

    Clock::time_point last_check_time;

    /// Set to true when the shutdown is expected.
    bool is_expected_shutdown{false};
};

namespace detail {

/// Helper to copy elements from a tuple to a vector of references
template<typename T, typename Tuple, std::size_t... I>
void tuple_to_vector_impl(
  std::vector<std::reference_wrapper<T>>& vec,
  Tuple&& tuple,
  std::index_sequence<I...>) {
    (vec.emplace_back(std::get<I>(std::forward<Tuple>(tuple))), ...);
}

/// Convert a tuple of callsites to a vector of references
template<typename T, typename... Callsites>
void tuple_to_vector(
  std::vector<std::reference_wrapper<T>>& vec,
  std::tuple<Callsites...>&& tuple) {
    tuple_to_vector_impl(
      vec, std::move(tuple), std::index_sequence_for<Callsites...>{});
}

} // namespace detail

template<typename Clock = seastar::timer<>::clock>
using execution_monitor_stall_callback
  = std::function<void(execution_monitor_callsite<Clock>&)>;

template<typename Clock = seastar::timer<>::clock>
using execution_monitor_unexpected_shutdown_callback
  = std::function<void(execution_monitor_shutdown_callsite<Clock>&)>;

/// The execution monitor is used to detect stalls in the execution
/// of a process. It allows to register multiple callsites and checkpoint
/// them periodically (when the callsite is reached).
/// The set of callsites is fixed at the construction time. The callsites
/// are constructed as a set of objects and passed to the c-tor.
template<typename Clock = seastar::timer<>::clock>
class execution_monitor {
public:
    using time_point = typename Clock::time_point;
    using duration = typename Clock::duration;

    /// Create a new execution monitor with a specified stall timeout.
    execution_monitor(seastar::sstring name, duration stall_timeout) noexcept
      : _name(std::move(name))
      , _stall_timeout(stall_timeout) {}

    /// Start the execution monitor with the given callsites.
    /// 'callsites' is a tuple of callsites that will be monitored.
    /// The algorithm of the execution monitor is as follows:
    /// 1. The monitor is started with a timer that checks the callsites
    ///    periodically (every stall_timeout).
    /// 2. Each callsite can be checkpointed, which updates the last check time.
    /// 3. The stall is detected if the last check time of any callsite
    ///    is older than the stall timeout.
    template<typename... U, typename... S>
    void start(
      execution_monitor_stall_callback<Clock> stall_cb,
      execution_monitor_unexpected_shutdown_callback<Clock> shutdown_cb,
      std::tuple<U...>&& callsites,
      std::tuple<S...>&& shutdown_callsites) {
        vassert(!_timer.armed(), "Execution monitor already started");
        detail::tuple_to_vector(_callsites, std::move(callsites));
        detail::tuple_to_vector(
          _shutdown_callsites, std::move(shutdown_callsites));
        _timer.set_callback([this,
                             stall_cb = std::move(stall_cb),
                             shutdown_cb = std::move(shutdown_cb)]() mutable {
            auto h = _gate.hold();
            // Check all callsites for stalls. At least one callsite
            // should have the access time within the stall timeout.
            bool has_stall = true;
            execution_monitor_callsite<>* last_cs = nullptr;
            for (auto callsite : _callsites) {
                auto& cs = callsite.get();
                auto deadline = std::max(
                  cs.last_check_time + _stall_timeout, cs.suspended_to);
                if (deadline > Clock::now()) {
                    // If at least one callsite has been checked
                    // within the stall timeout, we don't have a stall.
                    // No need to check the rest of them.
                    has_stall = false;
                    break;
                }
                if (last_cs != nullptr) {
                    // Save the last callsite
                    if (cs.last_check_time > last_cs->last_check_time) {
                        last_cs = &cs;
                    }
                } else {
                    last_cs = &cs;
                }
            }
            if (has_stall && last_cs != nullptr) {
                stall_cb(*last_cs);
            }
            // The inverse logic is used for shutdown callsites.
            for (auto& callsite : _shutdown_callsites) {
                auto& cs = callsite.get();
                if (!cs.is_expected_shutdown && cs.check_count > 0) {
                    shutdown_cb(cs);
                    break;
                }
            }
        });
        _timer.arm_periodic(_stall_timeout);
    }

    seastar::future<> stop() {
        vassert(!_gate.is_closed(), "Execution monitor not started");
        _timer.cancel();
        return _gate.close();
    }

    void expect_shutdown() {
        for (auto& callsite : _shutdown_callsites) {
            callsite.get().is_expected_shutdown = true;
        }
    }

    /// List all callsites ordered by the last check time. The most recently
    /// checkpointed callsite will be the first in the list.
    std::vector<std::reference_wrapper<execution_monitor_callsite<Clock>>>
    list_callsites() {
        std::vector<std::reference_wrapper<execution_monitor_callsite<Clock>>>
          result;
        result.reserve(_callsites.size());
        for (auto& cs : _callsites) {
            result.emplace_back(cs);
        }
        std::sort(
          result.begin(), result.end(), [](const auto& a, const auto& b) {
              return a.get().last_check_time > b.get().last_check_time;
          });
        return result;
    }

private:
    seastar::sstring _name;
    seastar::gate _gate;
    duration _stall_timeout;
    /// The callsites that are monitored.
    std::vector<std::reference_wrapper<execution_monitor_callsite<Clock>>>
      _callsites;
    /// The shutdown callsites that are monitored.
    std::vector<
      std::reference_wrapper<execution_monitor_shutdown_callsite<Clock>>>
      _shutdown_callsites;
    seastar::timer<Clock> _timer;
};

} // namespace ssx
