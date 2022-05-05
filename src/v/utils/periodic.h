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
#include "seastarx.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>

template <typename CLOCK>
struct periodic {
    using clock = CLOCK;
    using duration_type = std::chrono::milliseconds;
    using time_point = std::chrono::time_point<clock, duration_type>;

    explicit periodic(duration_type period) : _period(period), _next(time_point::min()) {}

    bool check() {
        auto n = clock::now();
        if (n > _next) {
            _next = n + _period;
            return true;
        }
        return false;
    }

    duration_type _period;
    time_point _next;
};

using periodic_ms = periodic<ss::lowres_clock>;
