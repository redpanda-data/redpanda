// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition_probe.h"

#include "cluster/partition.h"
#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cluster {
void partition_probe::setup_metrics(const model::ntp& ntp) {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    auto ns_label = sm::label("namespace");
    auto topic_label = sm::label("topic");
    auto partition_label = sm::label("partition");

    const std::vector<sm::label_instance> labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cluster:partition"),
      {
        sm::make_gauge(
          "leader",
          [this] { return _partition.is_leader() ? 1 : 0; },
          sm::description(
            "Flag indicating if this partition instance is a leader"),
          labels),
        sm::make_gauge(
          "last_stable_offset",
          [this] { return _partition.last_stable_offset(); },
          sm::description("Last stable offset"),
          labels),
        sm::make_gauge(
          "committed_offset",
          [this] { return _partition.committed_offset(); },
          sm::description("Committed offset"),
          labels),
        sm::make_derive(
          "records_produced",
          [this] { return _records_produced; },
          sm::description("Total number of records produced"),
          labels),
        sm::make_derive(
          "records_fetched",
          [this] { return _records_fetched; },
          sm::description("Total number of records fetched"),
          labels),
      });
}
} // namespace cluster
