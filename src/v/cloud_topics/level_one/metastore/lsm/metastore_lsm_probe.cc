/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/metastore_lsm_probe.h"

#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/sformat.h"

#include <seastar/core/metrics.hh>

namespace cloud_topics::l1 {

metastore_lsm_probe::metastore_lsm_probe(
  model::partition_id metastore_partition)
  : _probe(ss::make_lw_shared<lsm::probe>()) {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    const std::vector<sm::label_instance> labels = {
      sm::label("metastore_partition")(
        ssx::sformat("{}", metastore_partition())),
    };

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics:l1:metastore_lsm"),
      {
        sm::make_counter(
          "block_cache_hits_total",
          [this] { return _probe->block_cache_hit; },
          sm::description(
            "Number of LSM block cache hits for the L1 metastore database."),
          labels),
        sm::make_counter(
          "block_cache_misses_total",
          [this] { return _probe->block_cache_miss; },
          sm::description(
            "Number of LSM block cache misses for the L1 metastore database. "
            "A high miss rate against a working set larger than "
            "cloud_topics_metastore_block_cache_size indicates the cache is "
            "undersized for the workload."),
          labels),
        sm::make_counter(
          "table_cache_hits_total",
          [this] { return _probe->table_cache_hit; },
          sm::description(
            "Number of LSM table (SST file handle) cache hits for the L1 "
            "metastore database."),
          labels),
        sm::make_counter(
          "table_cache_misses_total",
          [this] { return _probe->table_cache_miss; },
          sm::description(
            "Number of LSM table cache misses for the L1 metastore database. "
            "Each miss opens an SST file."),
          labels),
        sm::make_counter(
          "throttled_writes_total",
          [this] { return _probe->throttled_writes; },
          sm::description(
            "Number of LSM writes that were throttled because L0 reached "
            "level_zero_slowdown_writes_trigger."),
          labels),
        sm::make_counter(
          "stalled_writes_total",
          [this] { return _probe->stalled_writes; },
          sm::description(
            "Number of LSM writes that were stalled because L0 reached "
            "level_zero_stop_writes_trigger."),
          labels),
        sm::make_histogram(
          "compaction_duration_microseconds",
          [this] {
              return _probe->compaction_latency.internal_histogram_logform();
          },
          sm::description("Duration of LSM compaction runs."),
          labels),
        sm::make_histogram(
          "flush_duration_microseconds",
          [this] { return _probe->flush_latency.internal_histogram_logform(); },
          sm::description("Duration of LSM memtable flushes."),
          labels),
        sm::make_histogram(
          "manifest_write_duration_microseconds",
          [this] {
              return _probe->manifest_write_latency
                .internal_histogram_logform();
          },
          sm::description("Duration of LSM manifest writes."),
          labels),
      });
}

} // namespace cloud_topics::l1
