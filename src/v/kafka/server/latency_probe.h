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

struct prod_consume_fixture;

namespace kafka {
class latency_probe {
public:
    using hist_t = log_hist_internal;

    latency_probe() = default;
    latency_probe(const latency_probe&) = delete;
    latency_probe& operator=(const latency_probe&) = delete;
    latency_probe(latency_probe&&) = delete;
    latency_probe& operator=(latency_probe&&) = delete;
    ~latency_probe() = default;

    void setup_metrics() {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        std::vector<sm::label_instance> labels{
          sm::label("latency_metric")("microseconds")};

        _metrics.add_group(
          prometheus_sanitize::metrics_name("kafka:latency"),
          {
            sm::make_histogram(
              "fetch_latency_us",
              sm::description("Fetch Latency"),
              labels,
              [this] { return _fetch_latency.internal_histogram_logform(); }),
            sm::make_histogram(
              "produce_latency_us",
              sm::description("Produce Latency"),
              labels,
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
                  sm::description("Total bytes produced, broken down by "
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

    std::unique_ptr<hist_t::measurement> auto_produce_measurement() {
        return _produce_latency.auto_measure();
    }

    void record_fetch_latency(std::chrono::microseconds micros) {
        _fetch_latency.record(micros.count());
    }

    void record_batch(uint64_t size, model::compression compression) {
        if (auto idx = (size_t)compression;
            idx < _bytes_by_compression.size()) {
            // NOLINTNEXTLINE:clang-tidy(cppcoreguidelines-pro-bounds-constant-array-index)
            _bytes_by_compression[idx] += size;
        }
    }

private:
    // for testing
    friend prod_consume_fixture;

    hist_t _produce_latency;
    hist_t _fetch_latency;

    // track the number of produced bytes using each compression type
    std::array<uint64_t, (size_t)model::compression::count>
      _bytes_by_compression{};

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace kafka
