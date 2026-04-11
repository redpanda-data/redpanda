/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/leader_router_probe.h"

#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"

namespace cloud_topics::l1 {

void leader_router_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics:l1:leader_router"),
      {
        sm::make_histogram(
          "add_objects_duration_microseconds",
          [this] { return _add_objects.internal_histogram_logform(); },
          sm::description("Latency of local add_objects requests")),
        sm::make_histogram(
          "replace_objects_duration_microseconds",
          [this] { return _replace_objects.internal_histogram_logform(); },
          sm::description("Latency of local replace_objects requests")),
        sm::make_histogram(
          "get_first_offset_ge_duration_microseconds",
          [this] { return _get_first_offset_ge.internal_histogram_logform(); },
          sm::description("Latency of local get_first_offset_ge requests")),
        sm::make_histogram(
          "get_first_timestamp_ge_duration_microseconds",
          [this] {
              return _get_first_timestamp_ge.internal_histogram_logform();
          },
          sm::description("Latency of local get_first_timestamp_ge requests")),
        sm::make_histogram(
          "get_first_offset_for_bytes_duration_microseconds",
          [this] {
              return _get_first_offset_for_bytes.internal_histogram_logform();
          },
          sm::description(
            "Latency of local get_first_offset_for_bytes requests")),
        sm::make_histogram(
          "get_offsets_duration_microseconds",
          [this] { return _get_offsets.internal_histogram_logform(); },
          sm::description("Latency of local get_offsets requests")),
        sm::make_histogram(
          "get_size_duration_microseconds",
          [this] { return _get_size.internal_histogram_logform(); },
          sm::description("Latency of local get_size requests")),
        sm::make_histogram(
          "get_compaction_info_duration_microseconds",
          [this] { return _get_compaction_info.internal_histogram_logform(); },
          sm::description("Latency of local get_compaction_info requests")),
        sm::make_histogram(
          "get_term_for_offset_duration_microseconds",
          [this] { return _get_term_for_offset.internal_histogram_logform(); },
          sm::description("Latency of local get_term_for_offset requests")),
        sm::make_histogram(
          "get_end_offset_for_term_duration_microseconds",
          [this] {
              return _get_end_offset_for_term.internal_histogram_logform();
          },
          sm::description("Latency of local get_end_offset_for_term requests")),
        sm::make_histogram(
          "set_start_offset_duration_microseconds",
          [this] { return _set_start_offset.internal_histogram_logform(); },
          sm::description("Latency of local set_start_offset requests")),
        sm::make_histogram(
          "remove_topics_duration_microseconds",
          [this] { return _remove_topics.internal_histogram_logform(); },
          sm::description("Latency of local remove_topics requests")),
        sm::make_histogram(
          "get_compaction_infos_duration_microseconds",
          [this] { return _get_compaction_infos.internal_histogram_logform(); },
          sm::description("Latency of local get_compaction_infos requests")),
        sm::make_histogram(
          "get_extent_metadata_duration_microseconds",
          [this] { return _get_extent_metadata.internal_histogram_logform(); },
          sm::description("Latency of local get_extent_metadata requests")),
        sm::make_histogram(
          "flush_domain_duration_microseconds",
          [this] { return _flush_domain.internal_histogram_logform(); },
          sm::description("Latency of local flush_domain requests")),
        sm::make_histogram(
          "restore_domain_duration_microseconds",
          [this] { return _restore_domain.internal_histogram_logform(); },
          sm::description("Latency of local restore_domain requests")),
        sm::make_histogram(
          "preregister_objects_duration_microseconds",
          [this] { return _preregister_objects.internal_histogram_logform(); },
          sm::description("Latency of local preregister_objects requests")),
      });
}

} // namespace cloud_topics::l1
