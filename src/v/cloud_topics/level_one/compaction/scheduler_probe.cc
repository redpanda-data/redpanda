/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/scheduler_probe.h"

#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_topics::l1 {

void compaction_scheduler_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics:compaction_scheduler"),
      {
        sm::make_gauge(
          "managed_log_count",
          [this] { return _log_count; },
          sm::description(
            "Number of cloud topic logs managed by this compaction scheduler")),
        sm::make_gauge(
          "compaction_queue_length",
          [this] { return _compaction_queue_length; },
          sm::description(
            "Length of the compaction queue for this compaction scheduler")),
        sm::make_counter(
          "log_compactions_total",
          [this] { return _log_compactions; },
          sm::description(
            "Number of compaction rounds performed across all shards")),
      });
}

} // namespace cloud_topics::l1
