/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/coordinator/coordinator_probe.h"

#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

namespace datalake::coordinator {

namespace {
const auto namespace_label = metrics::make_namespaced_label("namespace");
const auto topic_label = metrics::make_namespaced_label("topic");
const auto partition_label = metrics::make_namespaced_label("partition");
}; // namespace

coordinator_probe::coordinator_probe(model::ntp ntp)
  : _ntp(std::move(ntp)) {
    if (!config::shard_local_cfg().disable_public_metrics()) {
        _public_metrics.emplace();
        register_backpressure_metrics();
    }
}

void coordinator_probe::register_backpressure_metrics() {
    namespace sm = ss::metrics;
    std::vector<sm::label_instance> labels{
      namespace_label(_ntp.ns()),
      topic_label(_ntp.tp.topic()),
      partition_label(_ntp.tp.partition()),
    };
    _public_metrics->add_group(
      prometheus_sanitize::metrics_name("iceberg:coordinator"),
      {
        sm::make_counter(
          "add_files_backpressure_rejections",
          _add_files_backpressure,
          sm::description(
            "Number of add-files requests rejected because the coordinator had "
            "too many pending files"),
          labels)
          .aggregate({
            sm::shard_label,
            partition_label,
          }),
        sm::make_counter(
          "fetch_offsets_backpressure_signals",
          _fetch_offsets_backpressure,
          sm::description(
            "Number of fetch-offsets requests that signaled backpressure "
            "because the coordinator had too many pending files"),
          labels)
          .aggregate({
            sm::shard_label,
            partition_label,
          }),
      });
}

} // namespace datalake::coordinator
