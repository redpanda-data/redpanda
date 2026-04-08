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

#include <seastar/core/metrics_registration.hh>

#include <cstdint>
#include <memory>

namespace cloud_topics::l1 {

class compaction_scheduler_probe {
public:
    compaction_scheduler_probe() = default;

    compaction_scheduler_probe(const compaction_scheduler_probe&) = delete;
    compaction_scheduler_probe&
    operator=(const compaction_scheduler_probe&) = delete;
    compaction_scheduler_probe(compaction_scheduler_probe&&) = delete;
    compaction_scheduler_probe&
    operator=(compaction_scheduler_probe&&) = delete;
    ~compaction_scheduler_probe() = default;

    void setup_metrics();

    void set_log_count(uint32_t log_count) { _log_count = log_count; }
    void set_compaction_queue_length(uint32_t compaction_queue_length) {
        _compaction_queue_length = compaction_queue_length;
    }
    void log_compacted() { ++_log_compactions; }

private:
    uint32_t _log_count{0};
    uint32_t _compaction_queue_length{0};
    uint64_t _log_compactions{0};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::l1
