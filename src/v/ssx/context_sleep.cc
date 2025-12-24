// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/context_sleep.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/timer.hh>

#include <memory>

namespace ssx {

namespace {

template<typename Clock>
struct sleeper_frame final : context::detail::basic_context_frame {
    seastar::timer<Clock> timer;
    seastar::promise<> done;

    static void on_cancel_impl(
      context::detail::basic_context_frame& base,
      context::cancel_cause cause) noexcept {
        auto& self = static_cast<sleeper_frame&>(base);
        if (self.timer.cancel()) {
            self.done.set_exception(ssx::context_sleep_aborted{cause});
        }
    }

    static constexpr context::detail::frame_ops ops{
      .on_cancel = &on_cancel_impl};

    sleeper_frame(context_ref ctx, typename Clock::duration dur)
      : basic_context_frame(ctx, &ops)
      , timer([this] { done.set_value(); }) {
        timer.arm(dur);
    }
};

} // namespace

template<typename Clock>
seastar::future<> sleep(context_ref ctx, typename Clock::duration dur) {
    // Fast path: already cancelled
    if (ctx.is_cancelled()) {
        return seastar::make_exception_future<>(
          context_sleep_aborted{ctx.cancel_cause()});
    }

    // Fast path: zero or negative duration
    if (dur <= Clock::duration::zero()) {
        return seastar::make_ready_future<>();
    }

    auto s = std::make_unique<sleeper_frame<Clock>>(ctx, dur);
    auto fut = s->done.get_future();
    return fut.finally([s = std::move(s)] {});
}

// Explicit instantiations
template seastar::future<>
  sleep<seastar::lowres_clock>(context_ref, seastar::lowres_clock::duration);
template seastar::future<>
  sleep<seastar::manual_clock>(context_ref, seastar::manual_clock::duration);
template seastar::future<> sleep<seastar::steady_clock_type>(
  context_ref, seastar::steady_clock_type::duration);

} // namespace ssx
