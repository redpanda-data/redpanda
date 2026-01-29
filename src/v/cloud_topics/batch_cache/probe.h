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

class batch_cache_probe {
public:
    explicit batch_cache_probe(bool disable_metrics);

    void register_put(uint64_t bytes) { _put_bytes += bytes; }
    void register_get(uint64_t bytes) {
        _get_bytes += bytes;
        _hits++;
    }
    void register_miss() { _misses++; }

private:
    void setup_internal_metrics(bool disable);

    uint64_t _put_bytes{0};
    uint64_t _get_bytes{0};
    uint64_t _misses{0};
    uint64_t _hits{0};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics
