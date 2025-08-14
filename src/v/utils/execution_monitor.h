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
#include "utils/retry_chain_node.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>

#include <cstdint>
#include <optional>

namespace ssx {

template<typename Clock = seastar::lowres_clock>
using execution_monitor_stall_callback
  = std::function<void(basic_retry_chain_context<Clock>&)>;

using execution_monitor_unexpected_shutdown_callback = std::function<void()>;

/// The execution monitor detects stalls in a process's execution.
/// It monitors a collection of `retry_chain_context` instances,
/// each with a deadline that should not be reached under normal conditions.
/// A stall is detected if all monitored contexts reach their deadlines.
template<typename Clock = seastar::lowres_clock>
class basic_execution_monitor {
public:
    using time_point = typename Clock::time_point;
    using duration = typename Clock::duration;

    /// Create a new execution monitor with a specified stall timeout.
    basic_execution_monitor(
      seastar::sstring name, duration stall_timeout) noexcept
      : _name(std::move(name))
      , _stall_timeout(stall_timeout) {}

    basic_execution_monitor(const basic_execution_monitor&) = delete;
    basic_execution_monitor(basic_execution_monitor&&) = delete;
    basic_execution_monitor& operator=(const basic_execution_monitor&) = delete;
    basic_execution_monitor& operator=(basic_execution_monitor&&) = delete;

    /// Configuration structure for the stall detector.
    /// It is used to pass a set of monitored contexts and a callback.
    struct stall_detector_config {
        /// The callback which is invoked when the stall is detected
        execution_monitor_stall_callback<Clock> cb;
        /// List of contexts to monitor for stalls
        std::vector<retry_chain_context*> contexts;
    };

    /// Configuration structure for the shutdown detector.
    struct unexpected_shutdown_detector_config {
        /// The callback which is invoked when the unexpected shutdown is
        /// detected
        execution_monitor_unexpected_shutdown_callback cb;
        /// The callback is invoked when the gate doesn't have any holders
        seastar::gate* gate{nullptr};
    };

    /// Start the execution monitor with the given configs.
    /// The algorithm of the execution monitor is as follows:
    /// 1. The monitor is started with a timer that checks the contexts
    ///    periodically (every stall_timeout).
    /// 2. Each context can be checkpointed, which updates the last check time.
    /// 3. The stall is detected if the last check time of all contexts
    ///    is older than the stall timeout.
    /// 4. The unexpected shutdown is detected if the gate count is zero
    ///    and the shutdown is not announced.
    void start(
      std::optional<stall_detector_config> stall_cfg,
      std::optional<unexpected_shutdown_detector_config> exit_cfg) {
        if (!exit_cfg.has_value() && !stall_cfg.has_value()) {
            return;
        }
        vassert(!_timer.armed(), "Execution monitor already started");
        _timer.set_callback([this,
                             stall_cfg = std::move(stall_cfg),
                             exit_cfg = std::move(exit_cfg)]() mutable {
            // Check all contexts for stalls. At least one context
            // should have the access time within the stall timeout.
            if (stall_cfg.has_value()) {
                bool has_stall = true;
                basic_retry_chain_context<Clock>* last_ctx = nullptr;
                for (auto* ctx : stall_cfg.value().contexts) {
                    auto deadline = ctx->get_deadline(_stall_timeout);
                    if (deadline > Clock::now()) {
                        // If at least one context has been checked
                        // within the stall timeout, we don't have a stall.
                        // No need to check the rest of them.
                        has_stall = false;
                        break;
                    }
                    if (last_ctx != nullptr) {
                        // Save the last callsite
                        if (
                          ctx->get_last_trace_time()
                          > last_ctx->get_last_trace_time()) {
                            last_ctx = ctx;
                        }
                    } else {
                        last_ctx = ctx;
                    }
                }
                if (has_stall && last_ctx != nullptr) {
                    stall_cfg.value().cb(*last_ctx);
                }
            }
            // The inverse logic is used for shutdown callsites.
            if (
              exit_cfg.has_value() && !_expect_shutdown
              && exit_cfg.value().gate->get_count() == 0) {
                exit_cfg.value().cb();
            }
        });
        _timer.arm_periodic(_stall_timeout);
    }

    void stop() {
        expect_shutdown();
        _timer.cancel();
    }

    void expect_shutdown() { _expect_shutdown = true; }

private:
    seastar::sstring _name;
    duration _stall_timeout;
    bool _expect_shutdown{false};
    seastar::timer<Clock> _timer;
};

using execution_monitor = basic_execution_monitor<>;

} // namespace ssx
