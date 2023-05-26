/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/handlers/handler_probe.h"

#include "config/configuration.h"
#include "kafka/server/handlers/handler_interface.h"
#include "kafka/server/logger.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>

#include <chrono>
#include <memory>

namespace kafka {

handler_probe_manager::handler_probe_manager()
  : _metrics()
  , _probes(max_api_key() + 2) {
    const auto unknown_handler_key = max_api_key() + 1;
    for (size_t i = 0; i < _probes.size(); i++) {
        auto key = api_key{i};

        if (handler_for_key(key) || i == unknown_handler_key) {
            _probes[i].setup_metrics(_metrics, key);
        }
    }
}

handler_probe& handler_probe_manager::get_probe(api_key key) {
    if (!handler_for_key(key)) {
        return _probes.back();
    }

    return _probes[key];
}

handler_probe::handler_probe()
  : _last_recorded_in_progress(ss::lowres_clock::now()) {}

void handler_probe::setup_metrics(
  ss::metrics::metric_groups& metrics, api_key key) {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    const char* handler_name;
    if (auto handler = handler_for_key(key)) {
        handler_name = handler.value()->name();
    } else {
        handler_name = "unknown_handler";
    }

    std::vector<sm::label_instance> labels{sm::label("handler")(handler_name)};
    auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                              ? std::vector<sm::label>{sm::shard_label}
                              : std::vector<sm::label>{};

    metrics.add_group(
      prometheus_sanitize::metrics_name("kafka_handler"),
      {
        sm::make_counter(
          "requests_completed_total",
          [this] { return _requests_completed; },
          sm::description("Number of kafka requests completed"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "requests_errored_total",
          [this] { return _requests_errored; },
          sm::description("Number of kafka requests errored"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "requests_in_progress_total",
          [this] { return _requests_in_progress_every_ns / 1'000'000'000; },
          sm::description("A running total of kafka requests in progress"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "received_bytes_total",
          [this] { return _bytes_received; },
          sm::description("Number of bytes received from kafka requests"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "sent_bytes_total",
          [this] { return _bytes_sent; },
          sm::description("Number of bytes sent in kafka replies"),
          labels)
          .aggregate(aggregate_labels),
      });
}

/*
 * This roughly approximates an integral of `_requests_in_progress`.
 * By providing Prometheus with a counter of the integral rather than
 * a gauge for `_requests_in_progress` we avoid any bias in the value
 * that Prometheus's sampling rate could introduce.
 */
void handler_probe::sample_in_progress() {
    auto now = ss::lowres_clock::now();
    auto s_diff = (now - _last_recorded_in_progress)
                  / std::chrono::nanoseconds(1);

    _requests_in_progress_every_ns += _requests_in_progress * s_diff;
    _last_recorded_in_progress = now;
}

} // namespace kafka
