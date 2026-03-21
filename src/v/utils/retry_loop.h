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

#include <optional>

namespace utils {

struct retry_loop_config {
    context::duration initial_delay{std::chrono::milliseconds{50}};
    context::duration max_delay{std::chrono::seconds{3}};
    double multiplier{2.0};
    double jitter{0.2}; // 0.0 - 1.0
    uint16_t max_attempts{10};
};

/// Context-aware exponential backoff with jitter for retry loops.
/// Respects cancellation and deadlines via context_ref.
class retry_loop_state {
public:
    explicit retry_loop_state(retry_loop_config cfg = {});

    /// Returns attempt number (0-indexed), or nullopt when exhausted/cancelled.
    /// When nullopt is returned, inspect the context to determine the cause:
    /// ctx.is_cancelled() indicates cancellation, otherwise attempts exhausted.
    ///
    /// Example:
    /// \code
    /// retry_loop_state rs{};
    /// while (auto attempt = co_await rs.next(ctx)) {
    ///     if (try_operation()) {
    ///         co_return success;
    ///     }
    /// }
    /// co_return ctx.is_cancelled() ? errc::cancelled :
    ///     errc::retries_exhausted;
    /// \endcode
    ///
    /// \warning The future must be fully awaited before the context frame is
    /// destroyed; otherwise, it will lead to use-after-free errors.
    seastar::future<std::optional<uint16_t>> next(context_ref ctx);

private:
    context::duration apply_jitter(context::duration d) const;

    retry_loop_config cfg_;
    context::duration current_;
    uint16_t attempt_{0};
};

} // namespace utils
