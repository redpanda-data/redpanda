/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/partition_probe.h"

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_storage {

partition_probe::partition_probe(const model::ntp& ntp) {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    const std::vector<sm::label_instance> labels = {
      sm::label("namespace")(ntp.ns()),
      sm::label("topic")(ntp.tp.topic()),
      sm::label("partition")(ntp.tp.partition()),
    };

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_storage:partition"),
      {
        sm::make_total_bytes(
          "read_bytes",
          [this] { return _bytes_read; },
          sm::description("Total bytes read from remote partition"),
          labels),
        sm::make_counter(
          "read_records",
          [this] { return _records_read; },
          sm::description("Total number of records read from remote partition"),
          labels),

        sm::make_gauge(
          "materialized_segments",
          [this] { return _cur_materialized_segments; },
          sm::description("Current number of materialized remote segments"),
          labels),

        sm::make_gauge(
          "readers",
          [this] { return _cur_readers; },
          sm::description("Current number of remote partition readers"),
          labels),
        sm::make_gauge(
          "segment_readers",
          [this] { return _cur_segment_readers; },
          sm::description("Current number of remote segment readers"),
          labels),
      });
}

} // namespace cloud_storage
