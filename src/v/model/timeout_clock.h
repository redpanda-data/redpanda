/*
 * Copyright 2020 Redpanda Data, Inc.
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

#include <seastar/core/lowres_clock.hh>

#include <chrono>

namespace model {

using timeout_clock = ss::lowres_clock;

static constexpr timeout_clock::time_point no_timeout
  = timeout_clock::time_point::max();

inline constexpr timeout_clock::duration max_duration
  = timeout_clock::duration::max();

inline model::timeout_clock::time_point
time_from_now(model::timeout_clock::duration d) {
    /// Saturate now() + duration at no_timeout, avoid overflow
    const auto now = model::timeout_clock::now();
    const auto remaining = model::no_timeout - now;
    return d < remaining ? now + d : model::no_timeout;
}

inline model::timeout_clock::duration
time_until(model::timeout_clock::time_point deadline) {
    const auto now = model::timeout_clock::now();
    if (deadline < now) {
        return model::timeout_clock::duration(0);
    }
    return deadline - now;
}

} // namespace model
