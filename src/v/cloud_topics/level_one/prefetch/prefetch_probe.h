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

#include "metrics/metrics.h"

#include <algorithm>
#include <cstdint>

namespace cloud_topics::prefetch {

/// Per-shard metrics for the L1 prefetch service. Mirrors the structure of
/// level_one_reader_probe: plain counters/gauges updated by the service and
/// exported through an internal metric group.
class prefetch_probe {
public:
    prefetch_probe();

    // Memory broker -------------------------------------------------------
    void set_reserved_bytes(uint64_t v) { _reserved_bytes = v; }
    void register_borrow(uint64_t bytes) {
        ++_borrows;
        _borrowed_bytes += bytes;
    }

    // Downloads -----------------------------------------------------------
    void inc_in_flight() {
        ++_in_flight;
        _peak_in_flight = std::max(_peak_in_flight, _in_flight);
    }
    void dec_in_flight() { --_in_flight; }
    void register_download() { ++_downloads; }

    // Cache ---------------------------------------------------------------
    void register_cache_hit() { ++_cache_hits; }
    void register_cache_miss() { ++_cache_misses; }

    // Demand --------------------------------------------------------------
    void register_demand_wait() { ++_demand_waits; }

    // Reclamation ---------------------------------------------------------
    void register_eviction() { ++_evictions; }

    // Streams -------------------------------------------------------------
    void set_active_streams(uint64_t v) { _active_streams = v; }
    void set_idle_streams(uint64_t v) { _idle_streams = v; }

    // Accessors used by tests --------------------------------------------
    uint64_t reserved_bytes() const { return _reserved_bytes; }
    uint64_t in_flight() const { return _in_flight; }
    uint64_t peak_in_flight() const { return _peak_in_flight; }
    uint64_t downloads() const { return _downloads; }
    uint64_t cache_hits() const { return _cache_hits; }
    uint64_t cache_misses() const { return _cache_misses; }
    uint64_t demand_waits() const { return _demand_waits; }
    uint64_t borrows() const { return _borrows; }
    uint64_t evictions() const { return _evictions; }
    uint64_t active_streams() const { return _active_streams; }
    uint64_t idle_streams() const { return _idle_streams; }

private:
    void setup_metrics();

    uint64_t _reserved_bytes{0};
    uint64_t _borrowed_bytes{0};
    uint64_t _borrows{0};
    uint64_t _in_flight{0};
    uint64_t _peak_in_flight{0};
    uint64_t _downloads{0};
    uint64_t _cache_hits{0};
    uint64_t _cache_misses{0};
    uint64_t _demand_waits{0};
    uint64_t _evictions{0};
    uint64_t _active_streams{0};
    uint64_t _idle_streams{0};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::prefetch
