/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <cstddef>
#include <optional>

namespace cloud_topics::prefetch {

/// Configuration for the prefetch pacer adaptive window / chunk-size policy.
struct pacer_config {
    /// Minimum prefetch window in bytes.
    size_t min_window;
    /// Maximum prefetch window in bytes.
    size_t max_window;
    /// Minimum download chunk size in bytes.
    size_t min_chunk;
    /// Maximum download chunk size in bytes.
    size_t max_chunk;
    /// Window headroom multiplier applied to rate * latency.
    double safety;
};

/// Adaptive prefetch window and chunk-size calculator.
///
/// Observes the consume rate (bytes/s) via an EWMA over
/// `observe_consumed` samples and the backend refill latency via
/// `observe_refill_latency`, then exposes:
///   - `window_target(cap)` — how many bytes to keep prefetched ahead
///   - `next_chunk_size(run_remaining, reservation_remaining)` — how large
///     the next download should be
///   - `consume_rate_bps()` — current EWMA bytes/s estimate
///
/// Pure synchronous math — no IO, no Seastar futures. Time is injected via
/// `ss::lowres_clock::time_point` arguments so tests are deterministic.
class prefetch_pacer {
public:
    explicit prefetch_pacer(pacer_config cfg);

    /// Feed a consumption observation.
    ///
    /// The first call establishes a baseline; subsequent calls update the
    /// EWMA rate from the byte/time delta.  Two observations with identical
    /// timestamps are ignored (divide-by-zero guard).
    void observe_consumed(size_t bytes, ss::lowres_clock::time_point now);

    /// Feed a backend refill latency sample into the EWMA.
    void observe_refill_latency(std::chrono::milliseconds latency);

    /// Compute the prefetch window target in bytes.
    ///
    /// Returns clamp(consume_rate * latency * safety, min_window, max_window)
    /// further capped at `reservation_cap`.
    size_t window_target(size_t reservation_cap) const;

    /// Compute the size of the next download chunk in bytes.
    ///
    /// Returns a value in [min_chunk, max_chunk] that also does not exceed
    /// min(run_remaining, reservation_remaining).
    size_t
    next_chunk_size(size_t run_remaining, size_t reservation_remaining) const;

    /// Current EWMA consume rate in bytes per second.
    double consume_rate_bps() const;

private:
    pacer_config _cfg;

    // EWMA state for consume rate (bytes/s).
    double _rate_bps{0.0};

    // True once _rate_bps has been seeded by the first real delta.
    bool _rate_seeded{false};

    // EWMA state for refill latency (seconds).
    double _latency_s{0.0};

    // True once _latency_s has been seeded.
    bool _latency_seeded{false};

    // Previous observation for delta computation.
    std::optional<std::pair<size_t, ss::lowres_clock::time_point>> _prev;

    // Chunk-size ramp state: grows from min_chunk toward max_chunk.
    size_t _chunk_size;

    // EWMA smoothing factor applied after the first sample seeds the tracker.
    static constexpr double k_alpha = 0.25;
};

} // namespace cloud_topics::prefetch
