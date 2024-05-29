/*
 * Copyright 2024 Redpanda Data, Inc.
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

#include <seastar/core/cacheline.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/util/shared_token_bucket.hh>

#include <chrono>
#include <numeric>
#include <vector>

// atomic_token_bucket tracks the rate of a metric over time using atomic
// counters. It limits taking tokens from the bucket if it goes over the
// configured rate and provides methods to calculate how much time needs to
// elapse for the token bucket deficit to be gone.
class alignas(seastar::cache_line_size) atomic_token_bucket final {
public:
    using clock = ss::lowres_clock;

    /// rate -- the steady state rate we want to allow by the token bucket
    /// limit -- the maximal burst capacity we allow the bucket to increase to
    /// threshold -- only replenish the bucket when at least this many tokens
    /// would be added
    /// start_full -- sets the last threshold timestamp to well into the past to
    /// replenish the bucket as much as possible on the next call to replenish()
    atomic_token_bucket(
      uint64_t rate, uint64_t limit, uint64_t threshold, bool start_full)
      : _bucket(rate, limit, threshold, start_full) {
        if (start_full) {
            replenish(ss::lowres_clock::now());
        }
    }

    /// Take `v` tokens from the bucket
    /// If there are less than `v` tokens in the bucket, this call will still
    /// succeed, but subsequent calls to calculate_delay() will return a
    /// positive delay.
    void record(uint64_t v) { _bucket.grab(v); }

    /// Calculates the delay necessary to replenish the built up deficit
    /// The second parameter `v` can optionally be set to a positive value to
    /// implement a more efficient replenish-record-calculate_delay sequence.
    template<typename delay_t>
    delay_t
    update_and_calculate_delay(const clock::time_point& now, uint64_t v = 0) {
        replenish(now);
        return delay_for<delay_t>(_bucket.grab(v));
    }

    /// Calculates the delay necessary to replenish the built up deficit
    /// Prefer update_and_calculate_delay over this method as that ensures that
    /// replenish is called before calculating the delay (to provide an up to
    /// date view of the state of the token bucket). Furthermore,
    /// update_and_calculate_delay also allows to efficiently record values at
    /// the same time.
    template<typename delay_t>
    delay_t calculate_delay() {
        return delay_for<delay_t>(_bucket.grab(0));
    }

    /// Replenishes the bucket by adding tokens corresponding to the time
    /// interval since the last replenishment time to the given `now` time
    /// point.
    void replenish(const clock::time_point& now) { _bucket.replenish(now); }

    /// Returns the configured fixed rate at which the token bucket is
    /// replenished
    uint64_t rate() const { return _bucket.rate(); }

private:
    template<typename delay_t>
    delay_t delay_for(uint64_t grab_result) {
        return std::chrono::duration_cast<delay_t>(
          _bucket.duration_for(_bucket.deficiency(grab_result)));
    }

    using bucket_t = ss::internal::shared_token_bucket<
      uint64_t,
      std::ratio<1>,
      ss::internal::capped_release::no,
      clock>;

    bucket_t _bucket;
};
