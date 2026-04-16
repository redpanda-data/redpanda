/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/domain/domain_manager_probe.h"

#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_topics::l1 {

void domain_manager_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics:l1:domain_manager"),
      {
        sm::make_counter(
          "objects_preregistered_total",
          [this] { return _objects_preregistered; },
          sm::description(
            "Total number of L1 objects preregistered in cloud storage.")),
        sm::make_counter(
          "gc_objects_deleted_total",
          [this] { return _gc_objects_deleted; },
          sm::description(
            "Total number of unreferenced L1 objects deleted from cloud "
            "storage by garbage collection.")),
        sm::make_counter(
          "gc_object_deletions_replicated_total",
          [this] { return _gc_object_deletions_replicated; },
          sm::description(
            "Total number of L1 object deletions replicated by garbage "
            "collection.")),
        sm::make_counter(
          "gc_objects_expired_total",
          [this] { return _gc_objects_expired; },
          sm::description(
            "Total number of stale preregistered L1 objects expired by "
            "garbage collection.")),
      });
}

} // namespace cloud_topics::l1
