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
#include "seastarx.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <numeric>
#include <vector>

// rate_tracker tracks the rate of a metric over time using a sliding window
// average. configure with number of windows and the width of each window.
//
// this class effectively merges Kafka's SampledStat and Rate classes together
// so that the same computation is done with a single pass over the samples for
// the common case of recording a new value and sampling the rate.
class rate_tracker final {
public:
    using clock = ss::lowres_clock;

private:
    struct window {
        clock::time_point time;
        double count{0.};
        window() = default;
    };

public:
    rate_tracker(size_t num_windows, clock::duration window_size)
      : _windows(num_windows)
      , _window_size(window_size)
      , _current(0) {}

    // record an observation.
    void record(double v, const clock::time_point& now) {
        // update the current window
        maybe_advance_current(now);
        _windows[_current].count += v;
    }

    // return the updated rate in units/second.
    double measure(const clock::time_point& now) {
        maybe_advance_current(now);
        // process historical samples
        double total = 0.;
        struct window* oldest = nullptr;
        auto max_age = _window_size * _windows.size();
        for (auto& w : _windows) {
            // purge windows that are obsolete
            if ((now - w.time) > max_age) {
                w.time = now;
                w.count = 0;
            }

            // find oldest non-obsolete window
            if (!oldest || w.time < oldest->time) {
                oldest = &w;
            }

            // accumulate
            total += w.count;
        }

        // compute window size and current rate. the reason that the elapsed
        // time is adjusted up is to error conservatively to allow utilization
        // to ramp up until a steady state is hit in which all the windows are
        // fully used. this is an identical approach taken by the kafka broker.
        auto elapsed = now - oldest->time;
        auto num_windows = elapsed / _window_size;
        auto min_windows = _windows.size() - 1;
        // chrono uses signed integers
        if ((size_t)num_windows < min_windows) {
            elapsed += (min_windows - num_windows) * _window_size;
        }

        return total / std::chrono::duration<double>(elapsed).count();
    }

    clock::duration window_size() const { return _window_size; }

    // maybe advance to and reset the next window
    void maybe_advance_current(clock::time_point now) {
        if ((_windows[_current].time + _window_size) < now) {
            _current = (_current + 1) % _windows.size();
            auto& curr = _windows[_current];
            curr.time = now;
            curr.count = 0.;
        }
    }

private:
    std::vector<window> _windows;
    clock::duration _window_size;
    std::size_t _current;
};
