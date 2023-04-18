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
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics.hh>

#include <chrono>
#include <memory>

namespace kafka {

handler_probe_manager::handler_probe_manager()
  : _probes((size_t)max_handler_api_key + 2)
  , _unknown_handler_index((size_t)max_handler_api_key + 1) {
    for (size_t i = 0; i < _probes.size(); i++) {
        auto key = api_key{i};

        if (handler_for_key(key) || i == _unknown_handler_index) {
            _probes[i] = std::make_unique<handler_probe>(key);
            _probes.at(i)->setup_metrics();
        }
    }
}

handler_probe& handler_probe_manager::get_probe(api_key key) {
    if (!handler_for_key(key)) {
        key = api_key{_unknown_handler_index};
    }

    return *_probes.at(key);
}

handler_probe::handler_probe(api_key key)
  : _key(key) {}

void handler_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    const char* handler_name;
    if (auto handler = handler_for_key(_key)) {
        handler_name = handler.value()->name();
    } else {
        handler_name = "unknown_handler";
    }

    std::vector<sm::label_instance> labels{sm::label("handler")(handler_name)};
    auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                              ? std::vector<sm::label>{sm::shard_label}
                              : std::vector<sm::label>{};

    _metrics.add_group(
      prometheus_sanitize::metrics_name("kafka:handler"),
      {
        sm::make_counter(
          "requests_completed",
          [this] { return _requests_completed; },
          sm::description("Number of requests completed"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "requests_errored",
          [this] { return _requests_errored; },
          sm::description("Number of requests errored"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_counter(
          "requests_in_progress",
          [this] { return _requests_in_progress_every_ns / 1'000'000'000; },
          sm::description("Number of requests in progress"),
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
    auto s_diff = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now - _last_recorded_in_progress)
                    .count();

    if (s_diff > 0) {
        _requests_in_progress_every_ns += _requests_in_progress * s_diff;
        _last_recorded_in_progress = now;
    }
}

} // namespace kafka
