#pragma once

#include "metrics/metrics.h"
#include "model/fundamental.h"

#include <seastar/core/metrics_registration.hh>

#include <bits/stdint-uintn.h>

namespace storage {

class readers_cache_probe {
public:
    void reader_added() { _readers_added++; }
    void reader_evicted() { _readers_evicted++; }
    void cache_hit() { _cache_hits++; }
    void cache_miss() { _cache_misses++; }
    void clear() { _metrics.clear(); }

    void setup_metrics(const model::ntp& ntp);

private:
    uint64_t _readers_added{0};
    uint64_t _readers_evicted{0};
    uint64_t _cache_misses{0};
    uint64_t _cache_hits{0};

    metrics::internal_metric_groups _metrics;
};
} // namespace storage
