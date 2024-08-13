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

#include "wasm/transform_probe.h"

#include "metrics/prometheus_sanitize.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/util/noncopyable_function.hh>

namespace wasm {

void transform_probe::setup_metrics(ss::sstring transform_name) {
    namespace sm = ss::metrics;

    auto name_label = sm::label("function_name");
    const std::vector<sm::label_instance> labels = {
      name_label(std::move(transform_name)),
    };
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("transform_execution"),
      {
        sm::make_histogram(
          "latency_sec",
          sm::description("A histogram of the latency in seconds of "
                          "transforming a single record"),
          labels,
          [this] { return _transform_latency.public_histogram_logform(); })
          .aggregate({ss::metrics::shard_label}),
        sm::make_counter(
          "errors",
          [this] { return _transform_errors; },
          sm::description("Data transform invocation errors"),
          labels)
          .aggregate({ss::metrics::shard_label}),
      });
}

} // namespace wasm
