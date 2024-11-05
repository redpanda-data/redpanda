// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "metrics/metrics.h"

#include <cstdint>

namespace storage {

/// Log manager per-shard storage probe.
class log_manager_probe {
public:
    log_manager_probe() = default;
    log_manager_probe(const log_manager_probe&) = delete;
    log_manager_probe& operator=(const log_manager_probe&) = delete;
    log_manager_probe(log_manager_probe&&) = delete;
    log_manager_probe& operator=(log_manager_probe&&) = delete;
    ~log_manager_probe() = default;

public:
    void setup_metrics();
    void clear_metrics();

public:
    void set_log_count(uint32_t log_count) { _log_count = log_count; }
    void housekeeping_log_processed() { ++_housekeeping_log_processed; }
    void urgent_gc_run() { ++_urgent_gc_runs; }

private:
    uint32_t _log_count = 0;
    uint64_t _urgent_gc_runs = 0;
    uint64_t _housekeeping_log_processed = 0;

    metrics::internal_metric_groups _metrics;
};

}; // namespace storage
