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
    namespace sm = ss::metrics;

    if (!config::shard_local_cfg().disable_metrics()) {
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

    if (!config::shard_local_cfg().disable_public_metrics()) {
        auto aggregate_labels = std::vector<sm::label>{sm::shard_label};

        // Total disk usage information is only maintained on shard 0
        if (ss::this_shard_id() == ss::shard_id{0}) {
            _public_metrics.add_group(
              prometheus_sanitize::metrics_name("cloud_storage_cache_space"),
              {
                sm::make_gauge(
                  "size_bytes",
                  [this] { return _cur_size_bytes; },
                  sm::description("Sum of size of cached objects."))
                  .aggregate(aggregate_labels),
                sm::make_gauge(
                  "hwm_size_bytes",
                  [this] { return _hwm_size_bytes; },
                  sm::description(
                    "High watermark of sum of size of cached objects."))
                  .aggregate(aggregate_labels),
                sm::make_gauge(
                  "files",
                  [this] { return _cur_num_files; },
                  sm::description("Number of objects in cache."))
                  .aggregate(aggregate_labels),
              });
        }

        // Put/get stats are local to each shard.
        _public_metrics.add_group(
          prometheus_sanitize::metrics_name("cloud_storage_cache_op"),
          {
            sm::make_counter(
              "put",
              [this] { return _num_puts; },
              sm::description("Number of objects written into cache."))
              .aggregate(aggregate_labels),
            sm::make_counter(
              "hit",
              [this] { return _num_cached_gets; },
              sm::description("Number of get requests for objects that are "
                              "already in cache."))
              .aggregate(aggregate_labels),
            sm::make_counter(
              "miss",
              [this] { return _num_miss_gets; },
              sm::description("Number of get requests that are not satisfied "
                              "from the cache."))
              .aggregate(aggregate_labels),
            sm::make_gauge(
              "in_progress_files",
              [this] { return _cur_in_progress_files; },
              sm::description("Number of files that are being put to cache."))
              .aggregate(aggregate_labels),
          });
    }
}

} // namespace cloud_storage
