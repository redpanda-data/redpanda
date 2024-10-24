/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

#include <optional>
#include <utility>

/// \brief Predicate required to continue operation
///
/// Describes a predicate to be evaluated before starting an expensive
/// operation, or to be evaluated when an operation has failed, to find
/// the reason for failure
struct lazy_abort_source {
    /// Predicate to be evaluated before an operation. Evaluates to true when
    /// the operation should be aborted, false otherwise.
    using predicate_t = ss::noncopyable_function<std::optional<ss::sstring>()>;

    lazy_abort_source(predicate_t predicate)
      : _predicate{std::move(predicate)} {}

    bool abort_requested() {
        auto maybe_abort = _predicate();
        if (maybe_abort.has_value()) {
            _abort_reason = *maybe_abort;
            return true;
        } else {
            return false;
        }
    }
    ss::sstring abort_reason() const { return _abort_reason; }

private:
    ss::sstring _abort_reason;
    predicate_t _predicate;
};
