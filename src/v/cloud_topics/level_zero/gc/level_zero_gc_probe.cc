/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/gc/level_zero_gc_probe.h"

#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_types.hh>
#include <seastar/core/shared_ptr.hh>

namespace cloud_topics {

level_zero_gc_probe::level_zero_gc_probe(bool disable) {
    setup_internal_metrics(disable);
}

void level_zero_gc_probe::setup_internal_metrics(bool disable) {
    if (disable) {
        return;
    }

    namespace sm = ss::metrics;
    std::vector<sm::label_instance> labels;

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics_l0_gc"),
      {
        sm::make_counter(
          "objects_deleted",
          [this] { return objects_deleted_; },
          sm::description(
            "Number of L0 objects deleted by garbage collection."),
          labels),
      });
}

} // namespace cloud_topics
