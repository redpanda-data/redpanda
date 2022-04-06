/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include <chrono>
#include <vector>

/*
 * exponential weighted moving average
 *
 * 1. Create smoothing factor, initial average, window count
 * 2. Update the current window by calling `update`
 * 3. Call `tick` to advance to the next time window
 *
 * The implementation does not account for missed ticks, so it is best to drive
 * ticks with a timer which is often convenient when used in some control algo.
 * This has the added advantage that high frequency updates are cheaper because
 * they do not need to test for and initialize new time windows.
 *
 * TODO:
 *  - Generalize this on the type being counted. The structure of this ewma
 *  utility is generic, but the implementation assumes that it is parameterized
 *  on std::chrono durations.
 */
template<typename Duration>
class exponential_moving_average {
public:
    using duration = Duration;

    // clamp to zero below this threshold
    static constexpr double zero_clamp = 0.001;

    struct window {
        duration total{0};
        size_t count{0};
    };

    exponential_moving_average(
      double smoothing_factor, duration init, size_t num_windows)
      : _alpha(smoothing_factor)
      , _windows(num_windows, window{.total = init, .count = 1})
      , _current(0) {}

    void update(duration sample) {
        _windows[_current].count++;
        _windows[_current].total += sample;
    }

    void tick() {
        _current = (_current + 1) % _windows.size();
        _windows[_current] = window{};
    }

    double sample() {
        double lat_t = 0.0;

        // visit windows from oldest to newest
        auto idx = (_current + 1) % _windows.size();
        for (auto cnt = 0U; cnt < _windows.size(); cnt++) {
            auto& w = _windows[idx];

            // current latency observation
            double lat_cur = 0.0;
            if (w.count > 0) {
                auto total
                  = std::chrono::duration_cast<std::chrono::microseconds>(
                    w.total);
                lat_cur = static_cast<double>(total.count())
                          / static_cast<double>(w.count * 1000);
            }

            // apply update
            lat_t = (1.0 - _alpha) * lat_cur + _alpha * lat_t;

            // next window with wrap around
            idx = (idx + 1) % _windows.size();
        }

        if (lat_t < zero_clamp) {
            return 0.;
        }

        return lat_t;
    }

private:
    const double _alpha;
    std::vector<window> _windows;
    size_t _current;
};
