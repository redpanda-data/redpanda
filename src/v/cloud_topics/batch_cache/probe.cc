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

    // Set up private metrics
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics_batch_cache"),
      {
        sm::make_counter(
          "get_bytes",
          [this] { return _get_bytes; },
          sm::description(
            "Total number of bytes read from the record batch cache."),
          labels),
        sm::make_counter(
          "put_bytes",
          [this] { return _put_bytes; },
          sm::description(
            "Total number of bytes written to the record batch cache."),
          labels),
        sm::make_counter(
          "hits",
          [this] { return _hits; },
          sm::description("Number of cache hits"),
          labels),
        sm::make_counter(
          "misses",
          [this] { return _misses; },
          sm::description("Number of cache misses"),
          labels),
      });
}

} // namespace cloud_topics
