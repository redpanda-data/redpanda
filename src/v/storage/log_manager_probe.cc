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
}

void log_manager_probe::clear_metrics() { _metrics.clear(); }

} // namespace storage
