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

#include "compaction/types.h"
#include "metrics/metrics.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics_registration.hh>

#include <cstdint>
#include <memory>

namespace cloud_topics::l1 {

class compaction_worker_probe {
public:
    using hist_t = log_hist_internal;
    compaction_worker_probe() = default;

    compaction_worker_probe(const compaction_worker_probe&) = delete;
    compaction_worker_probe& operator=(const compaction_worker_probe&) = delete;
    compaction_worker_probe(compaction_worker_probe&&) = delete;
    compaction_worker_probe& operator=(compaction_worker_probe&&) = delete;
    ~compaction_worker_probe() = default;

    void setup_metrics();

    std::unique_ptr<hist_t::measurement> auto_compaction_measurement() {
        return _compaction_runs.auto_measure();
    }

    void add_stats(const compaction::stats& stats) {
        _batches_processed += stats.batches_processed;
        _batches_removed += stats.batches_discarded;
        _records_removed += stats.records_discarded;
        _tombstones_removed += stats.expired_tombstones_discarded;
    }

private:
    hist_t _compaction_runs;

    uint64_t _batches_processed{0};
    uint64_t _batches_removed{0};
    uint64_t _records_removed{0};
    uint64_t _tombstones_removed{0};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::l1
