// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "config/configuration.h"
#include "config/property.h"
#include "metrics/metrics.h"
#include "ssx/rate_limited_function.h"
#include "utils/hdr_hist.h"

#include <cstdint>

namespace storage {

/// Log manager per-shard storage probe.
class log_manager_probe {
public:
    using try_get_segment_histogram_cb_t
      = ss::noncopyable_function<std::optional<hdr_hist>()>;
    using calc_segment_histogram_cb_t = std::function<bool()>;
    log_manager_probe(
      try_get_segment_histogram_cb_t try_get_segment_histogram_cb,
      calc_segment_histogram_cb_t calc_segment_histogram_cb)
      : _try_get_segment_histogram_cb(std::move(try_get_segment_histogram_cb))
      , _rate_limited_calc_segment_histogram_cb(
          std::move(calc_segment_histogram_cb),
          config::shard_local_cfg()
            .storage_segment_size_refresh_rate_ms.bind()) {}

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

    hdr_hist _segment_size_histogram{};

    // A cheap function to fetch an update to `_segment_size_histogram` if one
    // is available.
    try_get_segment_histogram_cb_t _try_get_segment_histogram_cb;

    // A function to schedule a new segment size histogram calculation, which is
    // potentially expensive.
    ssx::rate_limited_function<
      bool(),
      seastar::lowres_clock,
      config::binding<std::optional<std::chrono::milliseconds>>>
      _rate_limited_calc_segment_histogram_cb;

    metrics::internal_metric_groups _metrics;
};

}; // namespace storage
