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
#include "model/metadata.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cluster {

replicated_partition_probe::replicated_partition_probe(
  const partition& p) noexcept
  : _partition(p) {}

void replicated_partition_probe::setup_metrics(const model::ntp& ntp) {
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
      prometheus_sanitize::metrics_name("partition"),
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
          "high_watermark",
          [this] { return _partition.high_watermark(); },
          sm::description(
            "Partion high watermark i.e. highest consumable offset"),
          labels),

        sm::make_gauge(
          "under_replicated_replicas",
          [this] {
              auto metrics = _partition._raft->get_follower_metrics();
              return std::count_if(
                metrics.cbegin(),
                metrics.cend(),
                [](const raft::follower_metrics& fm) {
                    return fm.under_replicated;
                });
          },
          sm::description("Number of under replicated replicas"),
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
        sm::make_total_bytes(
          "bytes_produced_total",
          [this] { return _bytes_fetched; },
          sm::description("Total number of bytes produced"),
          labels),
        sm::make_total_bytes(
          "bytes_fetched_total",
          [this] { return _records_fetched; },
          sm::description("Total number of bytes fetched"),
          labels),
      });
}
partition_probe make_materialized_partition_probe() {
    // TODO: implement partition probe for materialized partitions
    class impl : public partition_probe::impl {
        void setup_metrics(const model::ntp&) final {}
        void add_records_fetched(uint64_t) final {}
        void add_records_produced(uint64_t) final {}
        void add_bytes_fetched(uint64_t) final {}
        void add_bytes_produced(uint64_t) final {}
    };
    return partition_probe(std::make_unique<impl>());
}
} // namespace cluster
