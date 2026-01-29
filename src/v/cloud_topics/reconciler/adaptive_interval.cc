/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/reconciler/adaptive_interval.h"

#include <algorithm>

namespace cloud_topics::reconciler {

adaptive_interval::adaptive_interval(
  config::binding<std::chrono::milliseconds> min_interval,
  config::binding<std::chrono::milliseconds> max_interval,
  config::binding<double> target_fill_ratio,
  config::binding<double> speedup_blend,
  config::binding<double> slowdown_blend,
  config::binding<size_t> max_object_size)
  : _min_interval(std::move(min_interval))
  , _max_interval(std::move(max_interval))
  , _target_fill_ratio(std::move(target_fill_ratio))
  , _speedup_blend(std::move(speedup_blend))
  , _slowdown_blend(std::move(slowdown_blend))
  , _max_object_size(std::move(max_object_size))
  , _current_interval(_min_interval()) {}

void adaptive_interval::adapt(size_t max_bytes_produced) {
    auto target_object_size = static_cast<double>(_max_object_size())
                              * _target_fill_ratio();

    // Compute ideal interval based on how far off we were from target.
    ss::lowres_clock::duration ideal_interval;
    if (max_bytes_produced == 0) {
        ideal_interval = _max_interval();
    } else {
        // If actual < target, ratio > 1, so interval increases.
        // If actual > target, ratio < 1, so interval decreases.
        double ratio = target_object_size
                       / static_cast<double>(max_bytes_produced);
        auto current_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          _current_interval);
        ideal_interval = std::chrono::duration_cast<ss::lowres_clock::duration>(
          std::chrono::duration<double, std::milli>(
            current_ms.count() * ratio));
    }

    // Asymmetric blending: fast to speed up, slow to slow down.
    double blend = _slowdown_blend();
    if (ideal_interval < _current_interval) {
        blend = _speedup_blend();
    }

    auto current_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      _current_interval);
    auto ideal_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      ideal_interval);
    auto blended_ms = std::chrono::duration<double, std::milli>(
      (1.0 - blend) * current_ms.count() + blend * ideal_ms.count());
    _current_interval = std::chrono::duration_cast<ss::lowres_clock::duration>(
      blended_ms);

    auto min = ss::lowres_clock::duration(_min_interval());
    auto max = ss::lowres_clock::duration(_max_interval());
    _current_interval = std::clamp(_current_interval, min, max);
}

ss::lowres_clock::duration adaptive_interval::current_interval() const {
    return _current_interval;
}

} // namespace cloud_topics::reconciler
