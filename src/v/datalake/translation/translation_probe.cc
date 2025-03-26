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

#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

namespace datalake {

namespace {
static const auto group_name = prometheus_sanitize::metrics_name(
  "iceberg:translation");
static const auto namespace_label = metrics::make_namespaced_label("namespace");
static const auto topic_label = metrics::make_namespaced_label("topic");
static const auto partition_label = metrics::make_namespaced_label("partition");
static const auto cause_label = metrics::make_namespaced_label("cause");
}; // namespace

translation_probe::translation_probe(model::ntp ntp)
  : _ntp(std::move(ntp)) {
    if (!config::shard_local_cfg().disable_public_metrics()) {
        _public_metrics.emplace();
        register_created_files_metrics();
        register_invalid_record_metric();
    }
}

void translation_probe::register_created_files_metrics() {
    namespace sm = ss::metrics;

    std::vector<sm::label_instance> labels{
      namespace_label(_ntp.ns()),
      topic_label(_ntp.tp.topic()),
      partition_label(_ntp.tp.partition()),
    };

    _public_metrics->add_group(
      group_name,
      {
        sm::make_counter(
          "translations_finished",
          _translations_finished,
          sm::description("Number of finished translator executions"),
          labels)
          .aggregate({
            sm::shard_label,
            partition_label,
          }),
        sm::make_counter(
          "files_created",
          _files_created,
          sm::description(
            "Number of created parquet files (not counting the DLQ table)"),
          labels)
          .aggregate({
            sm::shard_label,
            partition_label,
          }),
        sm::make_counter(
          "parquet_rows_added",
          _parquet_rows_added,
          sm::description("Number of rows in created parquet files (not "
                          "counting the DLQ table)"),
          labels)
          .aggregate({
            sm::shard_label,
            partition_label,
          }),
        sm::make_counter(
          "parquet_bytes_added",
          _parquet_bytes_added,
          sm::description("Number of bytes in created parquet files (not "
                          "counting the DLQ table)"),
          labels)
          .aggregate({
            sm::shard_label,
            partition_label,
          }),
        sm::make_counter(
          "dlq_files_created",
          _dlq_files_created,
          sm::description("Number of created parquet files for the DLQ table"),
          labels)
          .aggregate({
            sm::shard_label,
            partition_label,
          }),
      });
}

void translation_probe::register_invalid_record_metric() {
    namespace sm = ss::metrics;

    for (auto cause :
         {invalid_record_cause::failed_kafka_schema_resolution,
          invalid_record_cause::failed_data_translation,
          invalid_record_cause::failed_iceberg_schema_resolution}) {
        std::vector<sm::label_instance> labels{
          namespace_label(_ntp.ns()),
          topic_label(_ntp.tp.topic()),
          partition_label(_ntp.tp.partition()),
          cause_label(
            prometheus_sanitize::metrics_name(fmt::format("{}", cause))),
        };

        _public_metrics->add_group(
          group_name,
          {
            sm::make_counter(
              "invalid_records",
              counter_ref(cause),
              sm::description(
                "Number of invalid records handled by translation"),
              labels)
              .aggregate({
                sm::shard_label,
                partition_label,
              }),
          });
    }
}

std::ostream&
operator<<(std::ostream& os, translation_probe::invalid_record_cause cause) {
    using enum translation_probe::invalid_record_cause;

    switch (cause) {
    case failed_kafka_schema_resolution:
        return os << "failed_kafka_schema_resolution";
    case failed_data_translation:
        return os << "failed_data_translation";
    case failed_iceberg_schema_resolution:
        return os << "failed_iceberg_schema_resolution";
    }
}

}; // namespace datalake
