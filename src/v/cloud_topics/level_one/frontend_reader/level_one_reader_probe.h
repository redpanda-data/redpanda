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

namespace cloud_topics {

class level_one_reader_probe {
public:
    level_one_reader_probe();

    void register_bytes_read(size_t bytes_read) { _bytes_read += bytes_read; }

    void register_bytes_skipped(size_t bytes_skipped) {
        _bytes_skipped += bytes_skipped;
    }

private:
    void setup_metrics();

    uint64_t _bytes_read{0};
    uint64_t _bytes_skipped{0};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics
