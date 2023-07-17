/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "probe.h"

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/thread_cputime_clock.hh>

namespace wasm {

void probe::setup_metrics(ss::sstring transform_name) {
    namespace sm = ss::metrics;

    auto name_label = sm::label("function_name");
    const std::vector<sm::label_instance> labels = {
      name_label(std::move(transform_name)),
    };
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("redpanda:wasm"),
      {
        sm::make_histogram(
          "latency_us",
          sm::description("Wasm Latency"),
          labels,
          [this] {
              return _transform_latency.seastar_histogram_logform(
                log_hist_public_scale);
          })
          .aggregate({ss::metrics::shard_label}),
        sm::make_counter(
          "count",
          [this] { return _transform_count; },
          sm::description("Wasm transforms total count"),
          labels)
          .aggregate({ss::metrics::shard_label}),
        sm::make_counter(
          "errors",
          [this] { return _transform_errors; },
          sm::description("Wasm errors"),
          labels)
          .aggregate({ss::metrics::shard_label}),
      });
}

} // namespace wasm
