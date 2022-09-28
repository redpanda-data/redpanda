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
#include "likely.h"
#include "seastarx.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/util/bool_class.hh>

#include <chrono>
#include <numeric>
#include <vector>

namespace kafka {

/// Token bucket algorithm, useful for rate-limiting solutions.
///
/// NOTE: There are a few token bucket algorithms in the tree, this one was
/// implemented to mimick the small varitaions introduced in the algorithm that
/// backs KIP-599, rate limiting for partition mutations. For a more generic
/// implementation check out utils/ until these are merged.
///
/// Keeps track of a limited number of resources known as 'tokens'.
/// The current amount of tokens are kept in \ref bucket and when a user
/// consumes resources they take a corresponding number of tokens from the
/// bucket. Every \ref window duration the pre-configured amount of units are
/// added to the bucket. If there are enough tokens in the bucket when queried
/// the algorithm recommends rate-limiting, otherwise not.
class token_bucket_rate_tracker {
public:
    using clock = ss::lowres_clock;
    using units_t = int64_t;

    /// Class constructor
    /// @param quota Expected rate to operate within
    /// @param samples Number of windows to accumulate in memory for burst
    /// @param window_sec width of window in which tokens are added - in seconds
    token_bucket_rate_tracker(
      uint32_t quota, uint32_t samples, clock::duration window_sec) noexcept
      : _quota(quota)
      , _samples(samples)
      , _window(std::chrono::duration_cast<std::chrono::seconds>(window_sec))
      , _last_check() {}

    /// Slight variation of token bucket algorithm to allow bursty traffic
    /// \ref burst tokens are removed from bucket after returning throttle
    /// behavior suggestion
    units_t record_and_measure(uint32_t n, clock::time_point now) noexcept {
        refill(now);
        const auto tokens = _bucket;
        if (_bucket >= 0) {
            _bucket = std::min<units_t>(burst(), _bucket - n);
        }
        return tokens;
    }

    void record(uint32_t n, const clock::time_point& now) noexcept {
        refill(now);
        if (_bucket >= 0) {
            /// Number of tokens in bucket is allowed to become negative to
            /// allow burst of traffic to proceed
            _bucket = std::min<units_t>(burst(), _bucket - n);
        }
    }

    units_t measure(const clock::time_point& now) noexcept {
        refill(now);
        return _bucket;
    }

    /// Returns the number of tokens that current remain in the bucket
    units_t units() const noexcept { return _bucket; }

private:
    void refill(const clock::time_point& now_ms) noexcept {
        const auto delta = std::chrono::duration_cast<std::chrono::seconds>(
          now_ms - _last_check);
        _last_check = now_ms;
        _bucket = std::min<units_t>(burst(), _bucket + delta.count() * _quota);
    }

    uint32_t burst() const noexcept {
        constexpr uint32_t max_val = std::numeric_limits<uint32_t>::max();
        const auto b64 = static_cast<uint64_t>(_quota)
                         * static_cast<uint64_t>(_samples)
                         * static_cast<uint64_t>(_window.count());
        return unlikely(b64 >= max_val) ? max_val : b64;
    }

private:
    uint32_t _quota;
    uint32_t _samples;
    units_t _bucket{0};
    std::chrono::seconds _window;
    clock::time_point _last_check;
};
} // namespace kafka
