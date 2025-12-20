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

#include <seastar/core/abort_source.hh>
#include <seastar/util/optimized_optional.hh>

namespace context {

/// \brief Exception thrown when a context is cancelled.
///
/// This exception is used by abort_source to signal cancellation through
/// Seastar's abort_source mechanism. It carries the cancellation cause for
/// diagnostic purposes.
class cancelled_exception final : public seastar::abort_requested_exception {
public:
    explicit cancelled_exception(cancel_cause cause) noexcept
      : cause_(cause) {}

    [[nodiscard]] cancel_cause cause() const noexcept { return cause_; }

    [[nodiscard]] const char* what() const noexcept override {
        switch (cause_) {
        case cancel_cause::manual:
            return "context cancelled";
        case cancel_cause::deadline:
            return "context deadline exceeded";
        case cancel_cause::not_cancelled:
            return "context not cancelled";
        }
    }

private:
    cancel_cause cause_;
};

/// Mixin providing Seastar abort_source bridge with bidirectional sync:
/// context cancel triggers abort_source, and external abort cancels context.
class abort_source {
    template<typename...>
    friend class context_frame;

public:
    using cancellable = void;

    abort_source() noexcept = default;

    /// Returns abort_source for Seastar APIs (sleep_abortable, etc.).
    template<typename Self>
    [[nodiscard]] seastar::abort_source& as(this Self& self) noexcept {
        // Lazy subscription: external abort -> context cancel.
        if (!self.sub_) {
            self.sub_ = self.as_.subscribe(
              [h = self.cancel_handle()]() mutable noexcept {
                  h.trigger(cancel_cause::manual);
              });
        }
        return self.as_;
    }

private:
    void on_context_cancel(const cancel_cause cause) noexcept {
        as_.request_abort_ex(cancelled_exception{cause});
    }

    seastar::abort_source as_;
    seastar::optimized_optional<seastar::abort_source::subscription> sub_;
};

} // namespace context
