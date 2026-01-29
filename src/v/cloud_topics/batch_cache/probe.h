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

#include "metrics/metrics.h"
#include "utils/hdr_hist.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/metrics_types.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace cloud_topics {

/// Probe for tracking batch cache statistics.
/// Tracks materialized cache (record batches with offsets) and hydrated cache
/// (raw L0 object data) separately for observability.
class batch_cache_probe {
public:
    explicit batch_cache_probe(bool disable_metrics);

    // Materialized cache operations (record batches with offsets)
    void register_materialized_put(uint64_t bytes) {
        _materialized_put_bytes += bytes;
    }
    void register_materialized_get(uint64_t bytes) {
        _materialized_get_bytes += bytes;
        _materialized_hits++;
    }
    void register_materialized_miss() { _materialized_misses++; }

    // Hydrated cache operations (raw L0 object data)
    void register_hydrated_put(uint64_t bytes) { _hydrated_put_bytes += bytes; }
    void register_hydrated_get(uint64_t bytes) {
        _hydrated_get_bytes += bytes;
        _hydrated_hits++;
    }
    void register_hydrated_miss() { _hydrated_misses++; }

    // Accessors for testing
    uint64_t materialized_hits() const { return _materialized_hits; }
    uint64_t materialized_misses() const { return _materialized_misses; }
    uint64_t materialized_put_bytes() const { return _materialized_put_bytes; }
    uint64_t materialized_get_bytes() const { return _materialized_get_bytes; }

    uint64_t hydrated_hits() const { return _hydrated_hits; }
    uint64_t hydrated_misses() const { return _hydrated_misses; }
    uint64_t hydrated_put_bytes() const { return _hydrated_put_bytes; }
    uint64_t hydrated_get_bytes() const { return _hydrated_get_bytes; }

private:
    void setup_internal_metrics(bool disable);

    // Materialized cache stats (record batches)
    uint64_t _materialized_put_bytes{0};
    uint64_t _materialized_get_bytes{0};
    uint64_t _materialized_misses{0};
    uint64_t _materialized_hits{0};

    // Hydrated cache stats (raw L0 object data)
    uint64_t _hydrated_put_bytes{0};
    uint64_t _hydrated_get_bytes{0};
    uint64_t _hydrated_misses{0};
    uint64_t _hydrated_hits{0};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics
