// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/probe.h"

#include "config/configuration.h"
#include "prometheus/aggregate_labels.h"
#include "prometheus/prometheus_sanitize.h"
#include "storage/readers_cache_probe.h"
#include "storage/segment.h"

#include <seastar/core/metrics.hh>

#include <type_traits>

namespace storage {

void node_probe::set_disk_metrics(
  uint64_t total_bytes, uint64_t free_bytes, disk_space_alert alert) {
    _disk = {
      .total_bytes = total_bytes,
      .free_bytes = free_bytes,
      .space_alert = alert};
}

void node_probe::setup_node_metrics() {
    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("storage:disk"),
      {
        sm::make_gauge(
          "total_bytes",
          [this] { return _disk.total_bytes; },
          sm::description("Total size of attached storage, in bytes."))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "free_bytes",
          [this] { return _disk.free_bytes; },
          sm::description("Disk storage bytes free."))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "free_space_alert",
          [this] {
              return static_cast<std::underlying_type_t<disk_space_alert>>(
                _disk.space_alert);
          },
          sm::description(
            "Status of low storage space alert. 0-OK, 1-Low Space 2-Degraded"))
          .aggregate({sm::shard_label}),
      });
}

void probe::setup_metrics(const model::ntp& ntp) {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    const std::vector<sm::label_instance> labels = {
      ssx::metrics::internal_labels::ns_label(ntp.ns()),
      ssx::metrics::internal_labels::topic_label(ntp.tp.topic()),
      ssx::metrics::internal_labels::partition_label(ntp.tp.partition()),
    };
    auto aggregate_labels = prometheus::aggregate_labels(
      {sm::shard_label, ssx::metrics::internal_labels::partition_label});

    _metrics.add_group(
      prometheus_sanitize::metrics_name("storage:log"),
      {sm::make_total_bytes(
         "written_bytes",
         [this] { return _bytes_written; },
         sm::description("Total number of bytes written"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "batches_written",
         [this] { return _batches_written; },
         sm::description("Total number of batches written"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_total_bytes(
         "read_bytes",
         [this] { return _bytes_read; },
         sm::description("Total number of bytes read"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_total_bytes(
         "cached_read_bytes",
         [this] { return _cached_bytes_read; },
         sm::description("Total number of cached bytes read"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "batches_read",
         [this] { return _batches_read; },
         sm::description("Total number of batches read"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "cached_batches_read",
         [this] { return _cached_batches_read; },
         sm::description("Total number of cached batches read"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "log_segments_created",
         [this] { return _log_segments_created; },
         sm::description(
           "Total number of local log segments created since node startup"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "log_segments_removed",
         [this] { return _log_segments_removed; },
         sm::description(
           "Total number of local log segments removed since node startup"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "log_segments_active",
         [this] { return _log_segments_active; },
         sm::description("Current number of local log segments"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "batch_parse_errors",
         [this] { return _batch_parse_errors; },
         sm::description("Number of batch parsing (reading) errors"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "batch_write_errors",
         [this] { return _batch_write_errors; },
         sm::description("Number of batch write errors"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "corrupted_compaction_indices",
         [this] { return _corrupted_compaction_index; },
         sm::description("Number of times we had to re-construct the "
                         ".compaction index on a segment"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "compacted_segment",
         [this] { return _segment_compacted; },
         sm::description("Number of compacted segments"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_gauge(
         "partition_size",
         [this] { return _partition_bytes; },
         sm::description("Current size of partition in bytes"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_total_bytes(
         "compaction_ratio",
         [this] { return _compaction_ratio; },
         sm::description("Average segment compaction ratio"),
         labels)
         .aggregate({sm::shard_label})});
}

void probe::add_initial_segment(const segment& s) {
    _partition_bytes += s.file_size();
}
void probe::delete_segment(const segment& s) {
    _partition_bytes -= s.file_size();
}

void readers_cache_probe::setup_metrics(const model::ntp& ntp) {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    auto aggregate_labels = prometheus::aggregate_labels(
      {sm::shard_label, ssx::metrics::internal_labels::partition_label});

    const std::vector<sm::label_instance> labels = {
      ssx::metrics::internal_labels::ns_label(ntp.ns()),
      ssx::metrics::internal_labels::topic_label(ntp.tp.topic()),
      ssx::metrics::internal_labels::partition_label(ntp.tp.partition()),
    };

    _metrics.add_group(
      prometheus_sanitize::metrics_name("storage:log"),
      {
        sm::make_counter(
          "readers_added",
          [this] { return _readers_added; },
          sm::description("Number of readers added to cache"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "readers_evicted",
          [this] { return _readers_evicted; },
          sm::description("Number of readers evicted from cache"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "cache_hits",
          [this] { return _cache_hits; },
          sm::description("Reader cache hits"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "cache_misses",
          [this] { return _cache_misses; },
          sm::description("Reader cache misses"),
          labels)
          .aggregate(aggregate_labels),
      });
}
} // namespace storage
