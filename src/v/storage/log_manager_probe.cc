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

    _metrics.add_group(
      "storage_manager",
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

    // segment appender metrics (which are per-shard, unlike log_probe stuff)
    _metrics.add_group(
      "segment_appender",
      {
        sm::make_counter(
          "merged_writes",
          [this] { return _appender_stats->merged_writes; },
          sm::description(
            "Number of writes merged into queued writes prior to dispatch.")),
        sm::make_counter(
          "bytes_requested",
          [this] { return _appender_stats->bytes_requested; },
          sm::description(
            "Logical bytes appended (the number of bytes appended by the upper "
            "layers, before alignment.")),
        sm::make_counter(
          "bytes_written",
          [this] { return _appender_stats->bytes_written; },
          sm::description(
            "Physical bytes appended (the number of bytes actually written via "
            "dma_write calls, which is in general higher than logical bytes "
            "due to alignment).")),
        sm::make_counter(
          "appends_requested",
          [this] { return _appender_stats->appends; },
          sm::description(
            "Logical number of append requests to segment appenders.")),
        sm::make_counter(
          "flushes",
          [this] { return _appender_stats->flushes; },
          sm::description("Number of flush calls to segment appenders.")),
        sm::make_counter(
          "fsyncs",
          [this] { return _appender_stats->fsyncs; },
          sm::description(
            "Number of disk fsync calls from segment appenders.")),
        sm::make_counter(
          "truncates",
          [this] { return _appender_stats->truncates; },
          sm::description(
            "Number of truncate operations on segment appenders.")),
        sm::make_counter(
          "fallocations",
          [this] { return _appender_stats->fallocations; },
          sm::description(
            "Number of fallocate operations on segment appenders.")),
        sm::make_counter(
          "last_page_hydrations",
          [this] { return _appender_stats->last_page_hydrations; },
          sm::description(
            "Number of last page hydrations in segment appenders.")),
        sm::make_counter(
          "split_writes",
          [this] { return _appender_stats->split_writes; },
          sm::description(
            "Number of writes that needed to be split across multiple chunks "
            "(i.e., the write didn't fit in the current chunk).")),
        sm::make_counter(
          "writes_completed",
          [this] { return _appender_stats->writes_completed; },
          sm::description(
            "Number of physical writes (dma_write calls) that completed "
            "successfully.")),
      },
      {},
      {});
}

void log_manager_probe::clear_metrics() { _metrics.clear(); }

} // namespace storage
