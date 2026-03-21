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

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>

namespace ssx {

/// Exception thrown when ssx::sleep is cancelled.
/// Extends seastar::sleep_aborted for compatibility with existing catch
/// handlers.
class context_sleep_aborted final : public seastar::sleep_aborted {
public:
    explicit context_sleep_aborted(context::cancel_cause c) noexcept
      : cause_(c) {}

    [[nodiscard]] context::cancel_cause cause() const noexcept {
        return cause_;
    }

    [[nodiscard]] const char* what() const noexcept override {
        switch (cause_) {
        case context::cancel_cause::manual:
            return "context sleep aborted: cancelled";
        case context::cancel_cause::deadline:
            return "context sleep aborted: deadline exceeded";
        case context::cancel_cause::not_cancelled:
            return "context sleep aborted";
        }
    }

private:
    context::cancel_cause cause_;
};

/// Cancellation-aware sleep that integrates with the context hierarchy.
///
/// Unlike seastar::sleep_abortable which requires an abort_source subscription
/// for each sleep call, this function directly uses the context's cancellation
/// mechanism for lower overhead.
///
/// \warning The sleep must be fully awaited or cancelled before the context
/// frame is destroyed; otherwise, it will lead to use-after-free errors.
///
/// \param ctx The context to respect for cancellation.
/// \param dur The duration to sleep.
/// \return A future that resolves when the sleep completes.
/// \throws ssx::context_sleep_aborted if the context is cancelled during sleep.
template<typename Clock = context::clock>
seastar::future<> sleep(context_ref ctx, typename Clock::duration dur);

extern template seastar::future<>
  sleep<seastar::lowres_clock>(context_ref, seastar::lowres_clock::duration);

extern template seastar::future<>
  sleep<seastar::manual_clock>(context_ref, seastar::manual_clock::duration);

extern template seastar::future<> sleep<seastar::steady_clock_type>(
  context_ref, seastar::steady_clock_type::duration);

} // namespace ssx
