/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/read_path_probes.h"

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"
#include "ssx/metrics.h"

#include <seastar/core/metrics.hh>

namespace cloud_storage {

partition_probe::partition_probe(const model::ntp& ntp) {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    const auto partition_label = sm::label("partition");
    const std::vector<sm::label_instance> partition_labels = {
      sm::label("namespace")(ntp.ns()),
      sm::label("topic")(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };

    auto aggregate_labels = std::vector<sm::label>{sm::shard_label};

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_storage:partition"),
      {
        sm::make_total_bytes(
          "read_bytes",
          [this] { return _bytes_read; },
          sm::description("Total bytes read from remote partition"),
          partition_labels)
          .aggregate(aggregate_labels),

        sm::make_counter(
          "read_records",
          [this] { return _records_read; },
          sm::description("Total number of records read from remote partition"),
          partition_labels)
          .aggregate(aggregate_labels),

        sm::make_gauge(
          "chunk_size",
          [this] { return _chunk_size; },
          sm::description("Size of chunk downloaded from cloud storage"),
          partition_labels)
          .aggregate(aggregate_labels),
      });
}

ts_read_path_probe::ts_read_path_probe() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    auto aggregate_labels = std::vector<sm::label>{sm::shard_label};

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_storage:read_path"),
      {
        sm::make_gauge(
          "materialized_segments",
          [this] { return _cur_materialized_segments; },
          sm::description("Current number of materialized remote segments"))
          .aggregate(aggregate_labels),

        sm::make_gauge(
          "readers",
          [this] { return _cur_readers; },
          sm::description("Current number of remote partition readers"))
          .aggregate(aggregate_labels),

        sm::make_gauge(
          "spillover_manifest_bytes",
          [this] { return _spillover_manifest_bytes; },
          sm::description("Total amount of memory used by spillover manifests"))
          .aggregate(aggregate_labels),

        sm::make_gauge(
          "spillover_manifest_instances",
          [this] { return _spillover_manifest_instances; },
          sm::description(
            "Total number of spillover manifests stored in memory"))
          .aggregate(aggregate_labels),

        sm::make_counter(
          "spillover_manifest_hydrated",
          [this] { return _spillover_manifest_hydrated; },
          sm::description(
            "Number of times spillover manifests were saved to the cache"))
          .aggregate(aggregate_labels),

        sm::make_counter(
          "spillover_manifest_materialized",
          [this] { return _spillover_manifest_materialized; },
          sm::description(
            "Number of times spillover manifests were loaded from the cache"))
          .aggregate(aggregate_labels),

        sm::make_gauge(
          "segment_readers",
          [this] { return _cur_segment_readers; },
          sm::description("Current number of remote segment readers"))
          .aggregate(aggregate_labels),

        sm::make_histogram(
          "spillover_manifest_latency",
          [this] {
              return ssx::metrics::report_default_histogram(
                _spillover_mat_latency);
          },
          sm::description(
            "Spillover manifest materialization latency histogram"))
          .aggregate(aggregate_labels),

        sm::make_counter(
          "chunks_hydrated",
          [this] { return _chunks_hydrated; },
          sm::description("Total number of hydrated chunks (some may have been "
                          "evicted from the cache)"))
          .aggregate(aggregate_labels),

        sm::make_histogram(
          "chunk_hydration_latency",
          [this] {
              return ssx::metrics::report_default_histogram(
                _chunk_hydration_latency);
          },
          sm::description("Chunk hydration latency histogram"))
          .aggregate(aggregate_labels),
      });
}

} // namespace cloud_storage
