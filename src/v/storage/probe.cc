// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/probe.h"

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"
#include "storage/readers_cache_probe.h"
#include "storage/segment.h"

#include <seastar/core/metrics.hh>

namespace storage {
void probe::setup_metrics(const model::ntp& ntp) {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    auto ns_label = sm::label("namespace");
    auto topic_label = sm::label("topic");
    auto partition_label = sm::label("partition");
    const std::vector<sm::label_instance> labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };

    _metrics.add_group(
      prometheus_sanitize::metrics_name("storage:log"),
      {
        sm::make_total_bytes(
          "written_bytes",
          [this] { return _bytes_written; },
          sm::description("Total number of bytes written"),
          labels),
        sm::make_derive(
          "batches_written",
          [this] { return _batches_written; },
          sm::description("Total number of batches written"),
          labels),
        sm::make_total_bytes(
          "read_bytes",
          [this] { return _bytes_read; },
          sm::description("Total number of bytes read"),
          labels),
        sm::make_total_bytes(
          "cached_read_bytes",
          [this] { return _cached_bytes_read; },
          sm::description("Total number of cached bytes read"),
          labels),
        sm::make_derive(
          "batches_read",
          [this] { return _batches_read; },
          sm::description("Total number of batches read"),
          labels),
        sm::make_derive(
          "cached_batches_read",
          [this] { return _cached_batches_read; },
          sm::description("Total number of cached batches read"),
          labels),
        sm::make_derive(
          "log_segments_created",
          [this] { return _log_segments_created; },
          sm::description("Number of created log segments"),
          labels),
        sm::make_derive(
          "log_segments_removed",
          [this] { return _log_segments_removed; },
          sm::description("Number of removed log segments"),
          labels),
        sm::make_gauge(
          "log_segments_active",
          [this] { return _log_segments_active; },
          sm::description("Number of active log segments"),
          labels),
        sm::make_derive(
          "batch_parse_errors",
          [this] { return _batch_parse_errors; },
          sm::description("Number of batch parsing (reading) errors"),
          labels),
        sm::make_derive(
          "batch_write_errors",
          [this] { return _batch_write_errors; },
          sm::description("Number of batch write errors"),
          labels),
        sm::make_derive(
          "corrupted_compaction_indices",
          [this] { return _corrupted_compaction_index; },
          sm::description("Number of times we had to re-construct the "
                          ".compaction index on a segment"),
          labels),
        sm::make_derive(
          "compacted_segment",
          [this] { return _segment_compacted; },
          sm::description("Number of compacted segments"),
          labels),
        sm::make_gauge(
          "partition_size",
          [this] { return _partition_bytes; },
          sm::description("Current size of partition in bytes"),
          labels),
        sm::make_total_bytes(
          "compaction_ratio",
          [this] { return _compaction_ratio; },
          sm::description("Average segment compaction ratio"),
          labels),
      });
}

void probe::add_initial_segment(const segment& s) {
    _partition_bytes += s.reader().file_size();
}
void probe::delete_segment(const segment& s) {
    _partition_bytes -= s.reader().file_size();
}

void readers_cache_probe::setup_metrics(const model::ntp& ntp) {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    auto ns_label = sm::label("namespace");
    auto topic_label = sm::label("topic");
    auto partition_label = sm::label("partition");
    const std::vector<sm::label_instance> labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };

    _metrics.add_group(
      prometheus_sanitize::metrics_name("storage:log"),
      {
        sm::make_derive(
          "readers_added",
          [this] { return _readers_added; },
          sm::description("Number of readers added to cache"),
          labels),
        sm::make_derive(
          "readers_evicted",
          [this] { return _readers_evicted; },
          sm::description("Number of readers evicted from cache"),
          labels),
        sm::make_derive(
          "cache_hits",
          [this] { return _cache_hits; },
          sm::description("Reader cache hits"),
          labels),
        sm::make_derive(
          "cache_misses",
          [this] { return _cache_misses; },
          sm::description("Reader cache misses"),
          labels),
      });
}
} // namespace storage
