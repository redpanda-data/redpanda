/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "model/compression.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics.hh>

#include <chrono>
#include <cstdint>

struct prod_consume_fixture;

namespace kafka {
class kafka_probe {
public:
    using hist_t = log_hist_internal;

    kafka_probe() = default;
    kafka_probe(const kafka_probe&) = delete;
    kafka_probe& operator=(const kafka_probe&) = delete;
    kafka_probe(kafka_probe&&) = delete;
    kafka_probe& operator=(kafka_probe&&) = delete;
    ~kafka_probe() = default;

    void setup_metrics() {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        std::vector<sm::label_instance> latency_labels{
          sm::label("latency_metric")("microseconds")};

        _metrics.add_group(
          prometheus_sanitize::metrics_name("kafka:latency"),
          {
            sm::make_histogram(
              "fetch_latency_us",
              sm::description("Fetch Latency"),
              latency_labels,
              [this] { return _fetch_latency.internal_histogram_logform(); }),
            sm::make_histogram(
              "produce_latency_us",
              sm::description("Produce Latency"),
              latency_labels,
              [this] { return _produce_latency.internal_histogram_logform(); }),
          },
          {},
          {sm::shard_label});

        for (auto compress_type : model::all_batch_compression_types) {
            auto compress_type_name = fmt::format("{}", compress_type);
            auto compression_label = sm::label("compression_type")(
              compress_type_name);

            _metrics.add_group(
              "kafka",
              {
                sm::make_counter(
                  "produced_bytes",
                  sm::description(
                    "Total bytes produced, broken down by "
                    "compression_type label."),
                  {compression_label},
                  [this, compress_type] {
                      auto idx = (size_t)compress_type;
                      // next check should "never" fail but just be extra
                      // careful
                      if (idx < _bytes_by_compression.size()) {
                          // NOLINTNEXTLINE:clang-tidy(cppcoreguidelines-pro-bounds-constant-array-index)
                          return _bytes_by_compression[idx];
                      }
                      return 0UL;
                  }),
              },
              {},
              {sm::shard_label});
        }

        _metrics.add_group(
          "kafka",
          {
            sm::make_histogram(
              "batch_size",
              sm::description(
                "Batch size across all topics measured at the kafka layer."),
              {},
              [this] { return _batch_size.batch_size_histogram_logform(); }),
          });

        _metrics.add_group(
          "kafka_rpc",
          {sm::make_counter(
            "produce_bad_create_time",
            [this] { return _produce_bad_create_time; },
            sm::description(
              "number of produce requests with timestamps too far "
              "in the future or in the past"))},
          {},
          {sm::shard_label});

        _metrics.add_group(
          "kafka",
          {sm::make_counter(
            "fetch_response_dropped_bytes",
            [this] { return _fetch_response_dropped_bytes; },
            sm::description(
              "Total bytes read from storage (including cloud storage) that "
              "were discarded in fill_fetch_responses because the response "
              "budget was already consumed by other partitions. Non-zero "
              "values indicate read amplification: data was fetched from S3 "
              "but not delivered to the consumer."))},
          {},
          {sm::shard_label});
    }

    void setup_public_metrics() {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_public_metrics()) {
            return;
        }
        _public_metrics.add_group(
          prometheus_sanitize::metrics_name("kafka"),
          {
            sm::make_histogram(
              "request_latency_seconds",
              sm::description("Internal latency of kafka produce requests"),
              {metrics::make_namespaced_label("request")("produce")},
              [this] { return _produce_latency.public_histogram_logform(); })
              .aggregate({sm::shard_label}),
            sm::make_histogram(
              "request_latency_seconds",
              sm::description("Internal latency of kafka consume requests"),
              {metrics::make_namespaced_label("request")("consume")},
              [this] { return _fetch_latency.public_histogram_logform(); })
              .aggregate({sm::shard_label}),
          });
    }

    // metric used to signal a produce request with a timestamp too far into the
    // future or too far in the past see configuration
    // log_message_timestamp_alert_after_ms and
    // log_message_timestamp_alert_before_ms
    void produce_bad_create_time() { _produce_bad_create_time++; }

    std::unique_ptr<hist_t::measurement> auto_produce_measurement() {
        return _produce_latency.auto_measure();
    }

    void record_fetch_latency(std::chrono::microseconds micros) {
        _fetch_latency.record(micros.count());
    }

    void add_fetch_response_dropped_bytes(uint64_t bytes) {
        _fetch_response_dropped_bytes += bytes;
    }

    void record_batch(uint64_t size, model::compression compression) {
        _batch_size.record(size);
        if (
          auto idx = (size_t)compression; idx < _bytes_by_compression.size()) {
            // NOLINTNEXTLINE:clang-tidy(cppcoreguidelines-pro-bounds-constant-array-index)
            _bytes_by_compression[idx] += size;
        }
    }

private:
    // for testing
    friend prod_consume_fixture;

    uint32_t _produce_bad_create_time = 0;
    uint64_t _fetch_response_dropped_bytes = 0;

    hist_t _produce_latency;
    hist_t _fetch_latency;
    // non partition or topic related as that is too expensive for histograms
    batch_size_hist _batch_size;

    // track the number of produced bytes using each compression type
    std::array<uint64_t, (size_t)model::compression::count>
      _bytes_by_compression{};

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace kafka
