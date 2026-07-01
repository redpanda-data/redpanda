/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/prefetch/prefetch_probe.h"

#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_topics::prefetch {

prefetch_probe::prefetch_probe() { setup_metrics(); }

void prefetch_probe::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics_l1_prefetch"),
      {
        sm::make_gauge(
          "reserved_bytes",
          [this] { return _reserved_bytes; },
          sm::description(
            "Bytes currently reserved against the per-shard L1 "
            "prefetch memory budget.")),
        sm::make_gauge(
          "in_flight_downloads",
          [this] { return _in_flight; },
          sm::description("Number of in-flight L1 prefetch downloads.")),
        sm::make_counter(
          "downloads",
          [this] { return _downloads; },
          sm::description("Total number of L1 prefetch downloads dispatched.")),
        sm::make_counter(
          "cache_hits",
          [this] { return _cache_hits; },
          sm::description(
            "Number of get_reader calls served by an existing "
            "warm stream.")),
        sm::make_counter(
          "cache_misses",
          [this] { return _cache_misses; },
          sm::description(
            "Number of get_reader calls that created a new "
            "stream (cold).")),
        sm::make_counter(
          "demand_waits",
          [this] { return _demand_waits; },
          sm::description(
            "Number of times a reader blocked waiting for the "
            "prefetcher to produce an offset.")),
        sm::make_counter(
          "borrows",
          [this] { return _borrows; },
          sm::description("Number of anti-starvation reservation borrows.")),
        sm::make_counter(
          "borrowed_bytes",
          [this] { return _borrowed_bytes; },
          sm::description(
            "Total bytes borrowed beyond the budget for "
            "anti-starvation.")),
        sm::make_counter(
          "evictions",
          [this] { return _evictions; },
          sm::description(
            "Number of streams reclaimed (LRU / partition "
            "stop).")),
        sm::make_gauge(
          "active_streams",
          [this] { return _active_streams; },
          sm::description("Number of streams with an attached reader.")),
        sm::make_gauge(
          "idle_streams",
          [this] { return _idle_streams; },
          sm::description("Number of streams with no attached reader.")),
      });
}

} // namespace cloud_topics::prefetch
