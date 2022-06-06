/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/cache_probe.h"

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_storage {

cache_probe::cache_probe() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_storage:cache"),
      {
        sm::make_counter(
          "puts",
          [this] { return _num_puts; },
          sm::description("Total number of files put into cache.")),
        sm::make_counter(
          "gets",
          [this] { return _num_gets; },
          sm::description("Total number of cache get requests.")),
        sm::make_counter(
          "cached_gets",
          [this] { return _num_cached_gets; },
          sm::description(
            "Total number of get requests that are already in cache.")),

        sm::make_gauge(
          "size_bytes",
          [this] { return _cur_size_bytes; },
          sm::description("Current cache size in bytes.")),
        sm::make_gauge(
          "files",
          [this] { return _cur_num_files; },
          sm::description("Current number of files in cache.")),
        sm::make_gauge(
          "in_progress_files",
          [this] { return _cur_in_progress_files; },
          sm::description(
            "Current number of files that are being put to cache.")),
      });
}

} // namespace cloud_storage
