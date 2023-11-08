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
#include "prometheus/prometheus_sanitize.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics.hh>

#include <chrono>

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

        auto add_plan_and_execute_metric =
          [this, &labels](const std::string& fetch_label, hist_t& hist) {
              auto fetch_labels = labels;
              auto fetch_result_label = sm::label("fetch_result")(fetch_label);
              fetch_labels.push_back(fetch_result_label);
              _metrics.add_group(
                prometheus_sanitize::metrics_name("fetch_stats"),
                {// Measures latency off creating the fetch plan and
                 // subsequently executing it - aka "one poll loop"
                 sm::make_histogram(
                   "plan_and_execute_latency_us",
                   sm::description("Latency of fetch planning and excution"),
                   fetch_labels,
                   [&hist] { return hist.internal_histogram_logform(); })},
                {},
                {sm::shard_label});
          };

        add_plan_and_execute_metric(
          "non-empty", _fetch_plan_and_execute_latency);
        add_plan_and_execute_metric(
          "empty", _fetch_plan_and_execute_latency_empty);
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

    void record_fetch_plan_and_execute_measurement(
      std::chrono::microseconds micros, bool empty) {
        if (empty) {
            _fetch_plan_and_execute_latency_empty.record(micros.count());
        } else {
            _fetch_plan_and_execute_latency.record(micros.count());
        }
    }

private:
    hist_t _produce_latency;
    hist_t _fetch_latency;
    hist_t _fetch_plan_and_execute_latency;
    hist_t _fetch_plan_and_execute_latency_empty;
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace kafka
