/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "config/property.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>

namespace cloud_topics::reconciler {

/**
 * Adaptive interval calculator for reconciliation scheduling.
 *
 * This class implements an adaptive scheduling algorithm that adjusts the
 * reconciliation interval based on observed object fill ratios. The goal is
 * to produce well-sized objects (target ~80% of max size) while adapting to
 * varying data rates.
 *
 * Key properties:
 * - Asymmetric response: fast to shorten interval and slow to lengthen it.
 *   This helps because adjustment happens once per interval, so long
 *   intervals have an inherent slowness to how they adjust.
 * - Bounded: interval always stays within [min_interval, max_interval]
 * - Self-correcting: errors in rate estimation correct each round
 *
 * To disable adaptive scheduling, set min_interval == max_interval.
 */
class adaptive_interval {
public:
    adaptive_interval(
      config::binding<std::chrono::milliseconds> min_interval,
      config::binding<std::chrono::milliseconds> max_interval,
      config::binding<double> target_fill_ratio,
      config::binding<double> speedup_blend,
      config::binding<double> slowdown_blend,
      config::binding<size_t> max_object_size);

    /**
     * Update the interval based on the result of a reconciliation round.
     *
     * @param max_bytes_produced The size of the largest object produced in
     *        this round. Use 0 if no objects were produced.
     */
    void adapt(size_t max_bytes_produced);

    /**
     * Get the current interval to use for the next sleep.
     */
    ss::lowres_clock::duration current_interval() const;

private:
    config::binding<std::chrono::milliseconds> _min_interval;
    config::binding<std::chrono::milliseconds> _max_interval;
    config::binding<double> _target_fill_ratio;
    config::binding<double> _speedup_blend;
    config::binding<double> _slowdown_blend;
    config::binding<size_t> _max_object_size;

    ss::lowres_clock::duration _current_interval;
};

} // namespace cloud_topics::reconciler
