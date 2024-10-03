/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "debug_bundle/probe.h"

#include "config/configuration.h"
#include "debug_bundle/types.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/shard_id.hh>

using namespace std::chrono_literals;

namespace debug_bundle {
void probe::setup_metrics() {
    namespace sm = ss::metrics;
    if (ss::this_shard_id() != service_shard) {
        return;
    }
    auto setup_common = [this]<typename MetricDef>() {
        std::vector<MetricDef> defs;
        defs.reserve(4);
        defs.emplace_back(
          sm::make_gauge(
            "last_successful_bundle_timestamp_seconds",
            [this] { return _last_successful_bundle.time_since_epoch() / 1s; },
            sm::description("Timestamp of last successful debug bundle "
                            "generation (seconds since epoch)"))
            .aggregate({}));
        defs.emplace_back(
          sm::make_gauge(
            "last_failed_bundle_timestamp_seconds",
            [this] { return _last_failed_bundle.time_since_epoch() / 1s; },
            sm::description("Timestamp of last failed debug bundle "
                            "generation (seconds since epoch)"))
            .aggregate({}));
        defs.emplace_back(
          sm::make_counter(
            "successful_generation_count",
            [this] { return _successful_generation_count; },
            sm::description(
              "Running count of successful debug bundle generations"))
            .aggregate({}));
        defs.emplace_back(
          sm::make_counter(
            "failed_generation_count",
            [this] { return _failed_generation_count; },
            sm::description("Running count of failed debug bundle generations"))
            .aggregate({}));

        return defs;
    };

    auto group_name = prometheus_sanitize::metrics_name("debug_bundle");

    if (!config::shard_local_cfg().disable_metrics()) {
        _metrics.add_group(
          group_name,
          setup_common
            .template operator()<ss::metrics::impl::metric_definition_impl>(),
          {},
          {});
    }

    if (!config::shard_local_cfg().disable_public_metrics()) {
        _public_metrics.add_group(
          group_name,
          setup_common.template operator()<ss::metrics::metric_definition>());
    }
}
} // namespace debug_bundle
