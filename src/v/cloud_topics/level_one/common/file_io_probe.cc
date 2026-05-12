/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/file_io_probe.h"

#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_topics::l1 {

file_io_probe::file_io_probe() { setup_metrics(); }

void file_io_probe::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics_level_one_file_io"),
      {
        sm::make_counter(
          "reads",
          [this] { return _reads; },
          sm::description("L1 read_object calls on this shard.")),
        sm::make_counter(
          "cache_misses",
          [this] { return _cache_misses; },
          sm::description("L1 cloud cache lookup misses on this shard.")),
        sm::make_counter(
          "concurrent_read_merges",
          [this] { return _concurrent_read_merges; },
          sm::description(
            "Cache misses that joined an in-flight download for the "
            "same extent.")),
      });
}

} // namespace cloud_topics::l1
