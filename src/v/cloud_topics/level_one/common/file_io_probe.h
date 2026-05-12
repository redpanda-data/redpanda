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

#include <cstdint>

namespace cloud_topics::l1 {

/// Per-shard metrics for file_io. `concurrent_read_merges` counts L1
/// reads that joined an already-in-flight download and got the
/// cached bytes on retry (an avoided S3 GET on this shard);
/// `merged_read_aborts` counts joined reads whose await was aborted
/// before the in-flight download resolved. Together they're a
/// per-shard signal of concurrent demand on hot extents and
/// cancellation pressure on consumers.
class file_io_probe {
public:
    file_io_probe();

    void register_concurrent_read_merge() { ++_concurrent_read_merges; }
    void register_merged_read_abort() { ++_merged_read_aborts; }

private:
    void setup_metrics();

    uint64_t _concurrent_read_merges{0};
    uint64_t _merged_read_aborts{0};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::l1
