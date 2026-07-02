/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/prefetch/prefetch_pacer.h"

#include <algorithm>
#include <chrono>
#include <cstddef>

namespace cloud_topics::prefetch {

prefetch_pacer::prefetch_pacer(pacer_config cfg)
  : _cfg(cfg)
  , _chunk_size(cfg.min_chunk) {}

void prefetch_pacer::observe_consumed(
  size_t bytes, ss::lowres_clock::time_point now) {
    if (!_prev.has_value()) {
        _prev = {bytes, now};
        return;
    }

    auto [prev_bytes, prev_ts] = *_prev;
    _prev = {bytes, now};

    auto dt = std::chrono::duration_cast<std::chrono::duration<double>>(
      now - prev_ts);
    if (dt.count() <= 0.0) {
        // Same timestamp or backwards — ignore to avoid divide-by-zero.
        return;
    }

    double delta_bytes = static_cast<double>(bytes)
                         - static_cast<double>(prev_bytes);
    // Guard against negative deltas (e.g. wrapping or reset).
    if (delta_bytes < 0.0) {
        delta_bytes = 0.0;
    }
    double sample_rate = delta_bytes / dt.count();

    if (!_rate_seeded) {
        // Seed directly so the first real sample is fully reflected.
        _rate_bps = sample_rate;
        _rate_seeded = true;
    } else {
        _rate_bps = k_alpha * sample_rate + (1.0 - k_alpha) * _rate_bps;
    }

    // Grow chunk size toward max_chunk on each observation (fast-drain signal).
    _chunk_size = std::min(_chunk_size * 2, _cfg.max_chunk);
}

void prefetch_pacer::observe_refill_latency(std::chrono::milliseconds latency) {
    double sample_s = static_cast<double>(latency.count()) / 1000.0;
    if (!_latency_seeded) {
        _latency_s = sample_s;
        _latency_seeded = true;
    } else {
        _latency_s = k_alpha * sample_s + (1.0 - k_alpha) * _latency_s;
    }

    // Slow-start step, applied once per refill (~one round-trip). If a reader
    // starved since the last refill, the current window is too small to keep
    // the consumer fed, so double the demand window (capped at max_window) to
    // push more bytes in flight next round. If nothing starved, the prefetcher
    // is keeping up, so decay the demand window back toward the pure BDP and
    // release the reservation it no longer needs.
    if (_blocked_since_refill) {
        size_t next = _demand_window == 0 ? _cfg.min_window
                                          : _demand_window * 2;
        _demand_window = std::min(next, _cfg.max_window);
        _blocked_since_refill = false;
    } else if (_demand_window > _cfg.min_window) {
        _demand_window = (_demand_window * 3) / 4;
    } else {
        _demand_window = 0;
    }
}

void prefetch_pacer::note_demand_block() { _blocked_since_refill = true; }

size_t prefetch_pacer::window_target(size_t reservation_cap) const {
    // Keep the larger of the bandwidth-delay product and the slow-start demand
    // window: the BDP tracks steady-state need, the demand window lets the
    // window climb above it while the consumer is starving (bootstrap out of
    // the min_window floor).
    double bdp = _rate_bps * _latency_s * _cfg.safety;
    size_t want = std::max(static_cast<size_t>(bdp), _demand_window);
    auto clamped = std::clamp(want, _cfg.min_window, _cfg.max_window);
    return std::min(clamped, reservation_cap);
}

size_t prefetch_pacer::next_chunk_size(
  size_t run_remaining, size_t reservation_remaining) const {
    // Clamp the ramp value to [min_chunk, max_chunk].
    size_t sz = std::clamp(_chunk_size, _cfg.min_chunk, _cfg.max_chunk);
    // Further cap by the smaller of run_remaining and reservation_remaining.
    sz = std::min(sz, std::min(run_remaining, reservation_remaining));
    return sz;
}

double prefetch_pacer::consume_rate_bps() const { return _rate_bps; }

} // namespace cloud_topics::prefetch
