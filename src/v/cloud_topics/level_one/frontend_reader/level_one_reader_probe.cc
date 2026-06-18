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

level_one_reader_probe::level_one_reader_probe() {
    setup_metrics();
    setup_public_metrics();
}

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
      });
}

void level_one_reader_probe::setup_public_metrics() {
    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics_level_one_reader"),
      {
        sm::make_counter(
          "metastore_lookup_duration_ns",
          [this] { return _metastore_lookup_ns; },
          sm::description(
            "Cumulative wall-clock nanoseconds L1 readers spent in the "
            "metastore extent-metadata lookup (get_extent_metadata_forwards "
            "RPC).")),
        sm::make_counter(
          "metastore_lookup_count",
          [this] { return _metastore_lookup_count; },
          sm::description(
            "Number of metastore extent-metadata lookups by L1 readers.")),
        sm::make_counter(
          "footer_read_duration_ns",
          [this] { return _footer_read_ns; },
          sm::description(
            "Cumulative wall-clock nanoseconds L1 readers spent "
            "reading per-object footers.")),
        sm::make_counter(
          "footer_read_count",
          [this] { return _footer_read_count; },
          sm::description("Number of per-object footer reads by L1 readers.")),
        sm::make_counter(
          "stream_open_duration_ns",
          [this] { return _stream_open_ns; },
          sm::description(
            "Cumulative wall-clock nanoseconds L1 readers spent "
            "opening object data streams (read_object).")),
        sm::make_counter(
          "stream_open_count",
          [this] { return _stream_open_count; },
          sm::description(
            "Number of object data streams opened by L1 readers.")),
        sm::make_counter(
          "batch_read_duration_ns",
          [this] { return _batch_read_ns; },
          sm::description(
            "Cumulative wall-clock nanoseconds L1 readers spent "
            "streaming and parsing batches from an open object "
            "stream.")),
        sm::make_counter(
          "batch_read_count",
          [this] { return _batch_read_count; },
          sm::description(
            "Number of batch-streaming passes over an open object stream by "
            "L1 readers.")),
      });
}

} // namespace cloud_topics
