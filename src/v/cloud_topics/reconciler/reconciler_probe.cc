/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/reconciler/reconciler_probe.h"

#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_topics::reconciler {

void reconciler_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics:reconciler"),
      {
        // Counters.
        sm::make_counter(
          "objects_uploaded",
          [this] { return _objects_uploaded; },
          sm::description("Total objects uploaded, including orphans")),
        sm::make_counter(
          "bytes_reconciled",
          [this] { return _bytes_reconciled; },
          sm::description("Total bytes produced, including metadata")),
        sm::make_counter(
          "batches_reconciled",
          [this] { return _batches_reconciled; },
          sm::description("Total record batches reconciled")),
        sm::make_counter(
          "partitions_reconciled",
          [this] { return _partitions_reconciled; },
          sm::description("Counts each time a partition contributed a batch")),
        sm::make_counter(
          "metastore_retries",
          [this] { return _metastore_retries; },
          sm::description("Total metastore operation retries")),
        sm::make_counter(
          "offset_corrections",
          [this] { return _offset_corrections; },
          sm::description(
            "Total times the metastore returned a corrected offset")),

        // Histograms.
        sm::make_histogram(
          "l0_read_duration_microseconds",
          [this] { return _l0_read_duration.internal_histogram_logform(); },
          sm::description("Duration reading from L0")),
        sm::make_histogram(
          "object_build_duration_microseconds",
          [this] {
              return _object_build_duration.internal_histogram_logform();
          },
          sm::description("Duration building and uploading L1 objects")),
        sm::make_histogram(
          "metastore_add_objects_duration_microseconds",
          [this] {
              return _metastore_add_objects_duration
                .internal_histogram_logform();
          },
          sm::description("Duration of add_objects to metastore")),
        sm::make_histogram(
          "object_size_bytes",
          [this] { return _object_size_bytes.seastar_histogram_logform(); },
          sm::description("Distribution of built L1 object sizes in bytes")),
      });
}

} // namespace cloud_topics::reconciler
