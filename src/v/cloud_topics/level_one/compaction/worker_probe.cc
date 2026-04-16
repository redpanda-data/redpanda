/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/worker_probe.h"

#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_topics::l1 {

void compaction_worker_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics:compaction_worker"),
      {
        sm::make_counter(
          "batches_processed_total",
          [this] { return _batches_processed; },
          sm::description(
            "Number of batches processed across all cloud topic partitions on "
            "this shard")),
        sm::make_counter(
          "batches_removed_total",
          [this] { return _batches_removed; },
          sm::description(
            "Number of batches removed across all cloud topic partitions on "
            "this shard")),
        sm::make_counter(
          "records_removed_total",
          [this] { return _records_removed; },
          sm::description(
            "Number of records removed across all cloud topic partitions on "
            "this shard")),
        sm::make_counter(
          "tombstones_removed_total",
          [this] { return _tombstones_removed; },
          sm::description(
            "Number of tombstone records removed across all cloud topic "
            "partitions on this shard")),
        sm::make_histogram(
          "compaction_duration_microseconds",
          [this] { return _compaction_runs.internal_histogram_logform(); },
          sm::description(
            "The duration of a compaction run for cloud topic partitions on "
            "this shard")),
      });
}

} // namespace cloud_topics::l1
