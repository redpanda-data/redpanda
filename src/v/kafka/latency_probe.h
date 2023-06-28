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
#include "prometheus/aggregate_labels.h"
#include "prometheus/prometheus_sanitize.h"
#include "ssx/metrics.h"
#include "utils/hdr_hist.h"

#include <seastar/core/metrics.hh>

namespace kafka {
class latency_probe {
public:
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
        auto aggregate_labels = prometheus::aggregate_labels({sm::shard_label});

        _metrics.add_group(
          prometheus_sanitize::metrics_name("kafka:latency"),
          {sm::make_histogram(
             "fetch_latency_us",
             sm::description("Fetch Latency"),
             labels,
             [this] { return _fetch_latency.seastar_histogram_logform(); })
             .aggregate(aggregate_labels),
           sm::make_histogram(
             "produce_latency_us",
             sm::description("Produce Latency"),
             labels,
             [this] { return _produce_latency.seastar_histogram_logform(); })
             .aggregate(aggregate_labels)});
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
              {ssx::metrics::make_namespaced_label("request")("produce")},
              [this] {
                  return ssx::metrics::report_default_histogram(
                    _produce_latency);
              })
              .aggregate({sm::shard_label}),
            sm::make_histogram(
              "request_latency_seconds",
              sm::description("Internal latency of kafka consume requests"),
              {ssx::metrics::make_namespaced_label("request")("consume")},
              [this] {
                  return ssx::metrics::report_default_histogram(_fetch_latency);
              })
              .aggregate({sm::shard_label}),
          });
    }

    std::unique_ptr<hdr_hist::measurement> auto_produce_measurement() {
        return _produce_latency.auto_measure();
    }

    void record_fetch_latency(std::chrono::microseconds micros) {
        _fetch_latency.record(micros.count());
    }

private:
    hdr_hist _produce_latency;
    hdr_hist _fetch_latency;
    ss::metrics::metric_groups _metrics;
    ss::metrics::metric_groups _public_metrics{
      ssx::metrics::public_metrics_handle};
};

} // namespace kafka
