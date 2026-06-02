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

#include <functional>

namespace cloud_topics {

class level_one_reader_probe {
public:
    level_one_reader_probe();

    void register_footer_read(size_t bytes_requested) {
        _footer_bytes_read += bytes_requested;
    }

    void register_bytes_read(size_t bytes_read) { _bytes_read += bytes_read; }

    void register_bytes_skipped(size_t bytes_skipped) {
        _bytes_skipped += bytes_skipped;
    }

    void register_footer_cache_hit() noexcept { ++_footer_cache_hits; }
    void register_footer_cache_miss() noexcept { ++_footer_cache_misses; }

    /// Inject a function the probe polls on every footer_cache_size metric
    /// scrape. Callers register the lambda once after the footer cache is
    /// wired in; the metric defaults to 0 until set.
    void set_footer_cache_size_fn(std::function<size_t()> fn) {
        _footer_cache_size_fn = std::move(fn);
    }

private:
    void setup_metrics();

    uint64_t _footer_bytes_read{0};
    uint64_t _bytes_read{0};
    uint64_t _bytes_skipped{0};
    uint64_t _footer_cache_hits{0};
    uint64_t _footer_cache_misses{0};
    std::function<size_t()> _footer_cache_size_fn = []() -> size_t {
        return 0;
    };

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics
