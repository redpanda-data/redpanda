// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "context/context.h"

#include <seastar/core/timer.hh>

namespace context {

class deadline_timer {
    template<typename...>
    friend class context_frame;

public:
    using cancellable = void;

    deadline_timer() = default;

private:
    template<typename Self, typename Rep, typename Period>
    void on_context_init(
      this Self& self, std::chrono::duration<Rep, Period> d) noexcept {
        self.arm_timer(context::clock::now() + d);
    }

    template<typename Self>
    void on_context_init(this Self& self, context::time_point tp) noexcept {
        self.arm_timer(tp);
    }

    template<typename Self>
    void on_context_init(
      this Self& self, system_clock::time_point wall_deadline) noexcept {
        const auto delta = std::chrono::duration_cast<context::duration>(
          wall_deadline.time_since_epoch()
          - context::lowres_system_clock::now().time_since_epoch());
        self.arm_timer(context::clock::now() + delta);
    }

    template<typename Self>
    void arm_timer(this Self& self, const context::time_point tp) noexcept {
        // Child cannot extend parent's deadline (inherited in constructor).
        if (self.deadline_ != context::no_deadline && tp > self.deadline_) {
            return;
        }

        // Set deadline regardless of whether we arm the timer or not. This
        // behavior is consistent with manual cancellation: once cancelled, the
        // deadline remains set.
        self.deadline_ = tp;

        // Already cancelled: deadline is set but don't arm timer.
        if (self.is_cancelled()) {
            return;
        }

        // Already expired: cancel immediately, don't arm timer.
        if (tp <= context::clock::now()) {
            self.trigger_cancel(context::cancel_cause::deadline);
            return;
        }

        self.timer_.set_callback(
          [cancel_handle = self.cancel_handle()]() mutable noexcept {
              cancel_handle.trigger(context::cancel_cause::deadline);
          });
        self.timer_.arm(tp);
    }

    void on_context_cancel(const context::cancel_cause) noexcept {
        timer_.cancel();
    }

    seastar::timer<context::clock> timer_;
};

} // namespace context
