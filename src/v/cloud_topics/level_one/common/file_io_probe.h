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

#include <cstddef>
#include <cstdint>

namespace cloud_topics::l1 {

/// Per-shard L1 file_io metrics.
class file_io_probe {
public:
    file_io_probe();

    void register_read() { ++_reads; }
    void register_cache_miss() { ++_cache_misses; }
    void register_concurrent_read_merge() { ++_concurrent_read_merges; }

    void register_footer_read(size_t bytes) { _footer_bytes_read += bytes; }
    void register_ts_index_read(size_t bytes) { _ts_index_bytes_read += bytes; }
    void register_ts_tx_read(size_t bytes) { _ts_tx_bytes_read += bytes; }

private:
    void setup_metrics();

    uint64_t _reads{0};
    uint64_t _cache_misses{0};
    uint64_t _concurrent_read_merges{0};

    uint64_t _footer_bytes_read{0};
    uint64_t _ts_index_bytes_read{0};
    uint64_t _ts_tx_bytes_read{0};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::l1
