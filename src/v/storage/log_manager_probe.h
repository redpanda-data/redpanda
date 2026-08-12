// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "container/chunked_hash_map.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "storage/recovery_report.h"
#include "storage/segment_appender.h"

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

    /// Keeps what recovery found for one log, and replaces an earlier report
    /// for the same log.
    void record_recovery(const model::ntp&, const recovery_report&);

    /// Drops the report for a log that the shard no longer holds. A log that
    /// moves to another shard keeps its files on disk, so the shard that
    /// receives it counts them.
    void forget_recovery(const model::ntp&);

    // Returns shared pointer to segment appender stats for this shard.
    // Segment appenders increment these stats directly.
    segment_appender::stats_ptr get_appender_stats() { return _appender_stats; }

private:
    void add_to_recovery_totals(const recovery_report&);
    void subtract_from_recovery_totals(const recovery_report&);

    uint32_t _log_count = 0;
    uint64_t _urgent_gc_runs = 0;
    uint64_t _housekeeping_log_processed = 0;

    /// Only the logs that hold at least one `.cannotrecover` file, which is
    /// usually none of them.
    chunked_hash_map<model::ntp, recovery_report> _recovery;
    recovery_report _recovery_totals;

    // Segment appender stats (accumulated across all segment appenders on this
    // shard)
    segment_appender::stats_ptr _appender_stats
      = ss::make_lw_shared<segment_appender::stats>();

    metrics::internal_metric_groups _metrics;
};

}; // namespace storage
