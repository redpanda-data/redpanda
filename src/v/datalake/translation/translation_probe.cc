/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/translation_probe.h"

#include "metrics/prometheus_sanitize.h"

namespace datalake {

namespace {
static const auto group_name = prometheus_sanitize::metrics_name(
  "iceberg:translation");
static const auto namespace_label = metrics::make_namespaced_label("namespace");
static const auto topic_label = metrics::make_namespaced_label("topic");
static const auto partition_label = metrics::make_namespaced_label("partition");
}; // namespace

void translation_probe::increment_invalid_record_action() noexcept {
    _invalid_record_action += 1;
}

void translation_probe::setup_public_metrics(
  const model::ntp& ntp, metrics::public_metric_groups& groups) {
    namespace sm = ss::metrics;

    std::vector<sm::label_instance> labels{
      namespace_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };

    groups.add_group(
      group_name,
      {
        sm::make_counter(
          "invalid_records",
          _invalid_record_action,
          sm::description("Number of invalid records handled by translation"),
          labels)
          .aggregate({
            sm::shard_label,
            partition_label,
          }),
      });
}

}; // namespace datalake
