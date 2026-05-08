/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"

#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_topics {

level_one_reader_probe::level_one_reader_probe() { setup_metrics(); }

void level_one_reader_probe::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics_level_one_reader"),
      {
        sm::make_counter(
          "footer_read_bytes",
          [this] { return _footer_bytes_read; },
          sm::description("Number of footer bytes read by L1 readers.")),
        sm::make_counter(
          "read_bytes",
          [this] { return _bytes_read; },
          sm::description("Number of bytes read by L1 readers.")),
        sm::make_counter(
          "skipped_bytes",
          [this] { return _bytes_skipped; },
          sm::description("Number of bytes skipped by L1 readers.")),
        sm::make_counter(
          "footer_cache_hits",
          [this] { return _footer_cache_hits; },
          sm::description("L1 footer cache hits across all readers.")),
        sm::make_counter(
          "footer_cache_misses",
          [this] { return _footer_cache_misses; },
          sm::description("L1 footer cache misses across all readers.")),
        sm::make_gauge(
          "footer_cache_size",
          [this] { return _footer_cache_size_fn(); },
          sm::description("Current L1 footer cache entry count.")),
      });
}

} // namespace cloud_topics
