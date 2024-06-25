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
#include "metrics/prometheus_sanitize.h"

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
                sm::make_gauge(
                  "hwm_files",
                  [this] { return _hwm_num_files; },
                  sm::description(
                    "High watermark of number of objects in cache."))
                  .aggregate(aggregate_labels),
                sm::make_counter(
                  "tracker_syncs",
                  [this] { return _tracker_syncs; },
                  sm::description(
                    "Number of times the access tracker was updated "
                    "with cache disk data"))
                  .aggregate(aggregate_labels),
                sm::make_gauge(
                  "tracker_size",
                  [this] { return _tracker_size; },
                  sm::description("Number of entries in cache access tracker"))
                  .aggregate(aggregate_labels),
              });

            _public_metrics.add_group(
              prometheus_sanitize::metrics_name("cloud_storage_cache_trim"),
              {
                sm::make_counter(
                  "fast_trims",
                  [this] { return _fast_trims; },
                  sm::description("Number of times we have trimmed the cache "
                                  "using the normal (fast) mode."))
                  .aggregate(aggregate_labels),
                sm::make_counter(
                  "exhaustive_trims",
                  [this] { return _exhaustive_trims; },
                  sm::description(
                    "Number of times we couldn't free enough space with a fast "
                    "trim and had to fall back to a slower exhaustive trim."))
                  .aggregate(aggregate_labels),
                sm::make_counter(
                  "carryover_trims",
                  [this] { return _carryover_trims; },
                  sm::description("Number of times we invoked carryover trim."))
                  .aggregate(aggregate_labels),
                sm::make_counter(
                  "failed_trims",
                  [this] { return _failed_trims; },
                  sm::description(
                    "Number of times could not free the expected amount of "
                    "space, indicating possible bug or configuration issue."))
                  .aggregate(aggregate_labels),
                sm::make_counter(
                  "in_mem_trims",
                  [this] { return _in_mem_trims; },
                  sm::description("Number of times we trimmed the cache using "
                                  "the in-memory access tracker."))
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
              sm::description("Number of failed get requests because of "
                              "missing object in the cache."))
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
