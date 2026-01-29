/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/batch_cache/probe.h"

#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_types.hh>
#include <seastar/core/shared_ptr.hh>

namespace cloud_topics {

batch_cache_probe::batch_cache_probe(bool disable_metrics) {
    setup_internal_metrics(disable_metrics);
}

void batch_cache_probe::setup_internal_metrics(bool disable) {
    if (disable) {
        return;
    }
    namespace sm = ss::metrics;
    std::vector<sm::label_instance> labels;

    // Materialized cache metrics (record batches with offsets)
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics_batch_cache"),
      {
        sm::make_counter(
          "materialized_get_bytes",
          [this] { return _materialized_get_bytes; },
          sm::description(
            "Total bytes read from the materialized record batch cache."),
          labels),
        sm::make_counter(
          "materialized_put_bytes",
          [this] { return _materialized_put_bytes; },
          sm::description(
            "Total bytes written to the materialized record batch cache."),
          labels),
        sm::make_counter(
          "materialized_hits",
          [this] { return _materialized_hits; },
          sm::description("Number of materialized cache hits"),
          labels),
        sm::make_counter(
          "materialized_misses",
          [this] { return _materialized_misses; },
          sm::description("Number of materialized cache misses"),
          labels),
      });

    // Hydrated cache metrics (raw L0 object data)
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics_batch_cache"),
      {
        sm::make_counter(
          "hydrated_get_bytes",
          [this] { return _hydrated_get_bytes; },
          sm::description("Total bytes read from the hydrated L0 cache."),
          labels),
        sm::make_counter(
          "hydrated_put_bytes",
          [this] { return _hydrated_put_bytes; },
          sm::description("Total bytes written to the hydrated L0 cache."),
          labels),
        sm::make_counter(
          "hydrated_hits",
          [this] { return _hydrated_hits; },
          sm::description("Number of hydrated cache hits"),
          labels),
        sm::make_counter(
          "hydrated_misses",
          [this] { return _hydrated_misses; },
          sm::description("Number of hydrated cache misses"),
          labels),
      });
}

} // namespace cloud_topics
