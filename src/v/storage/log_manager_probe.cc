// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/log_manager_probe.h"

#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/rate_limited_function.h"

#include <seastar/core/metrics.hh>

namespace storage {

void log_manager_probe::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;

    auto group_name = prometheus_sanitize::metrics_name("storage:manager");

    _metrics.add_group(
      group_name,
      {
        sm::make_gauge(
          "logs",
          [this] { return _log_count; },
          sm::description("Number of logs managed")),
        sm::make_counter(
          "urgent_gc_runs",
          [this] { return _urgent_gc_runs; },
          sm::description("Number of urgent GC runs")),
        sm::make_counter(
          "housekeeping_log_processed",
          [this] { return _housekeeping_log_processed; },
          sm::description("Number of logs processed by housekeeping")),
      },
      {},
      {});

    _metrics.add_group(
      group_name,
      {
        sm::make_histogram(
          "segment_size",
          [this] {
              auto segment_size_collection_enabled
                = config::shard_local_cfg()
                    .storage_segment_size_refresh_rate_ms()
                    .has_value();

              if (segment_size_collection_enabled) {
                  auto new_segment_size_histogram
                    = _try_get_segment_histogram_cb();
                  if (new_segment_size_histogram.has_value()) {
                      _segment_size_histogram
                        = std::move(new_segment_size_histogram).value();
                  }

                  _rate_limited_calc_segment_histogram_cb();
              }

              return _segment_size_histogram.seastar_histogram_logform();
          },
          sm::description("Local segment size histogram in bytes.")),
      },
      {},
      {sm::shard_label});
}

void log_manager_probe::clear_metrics() { _metrics.clear(); }

} // namespace storage
