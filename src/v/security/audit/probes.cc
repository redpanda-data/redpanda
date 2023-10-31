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

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"
#include "security/audit/audit_log_manager.h"
#include "security/audit/client_probe.h"
#include "security/audit/probe.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>

#include <chrono>

namespace security::audit {

void audit_probe::setup_metrics(std::function<double()> get_usage_ratio) {
    namespace sm = ss::metrics;

    auto setup_common = [this]<typename MetricDef>(
                          const std::vector<sm::label>& aggregate_labels) {
        std::vector<MetricDef> defs;
        if (ss::this_shard_id() == audit_log_manager::client_shard_id) {
            defs.emplace_back(
              sm::make_counter(
                "last_event_timestamp_seconds",
                [this] { return _last_event.time_since_epoch() / 1s; },
                sm::description("Timestamp of last successful publish on the "
                                "audit log (seconds since epoch)"))
                .aggregate(aggregate_labels));
        }
        defs.emplace_back(
          sm::make_counter(
            "errors_total",
            [this] { return _audit_error_count; },
            sm::description("Running count of errors in creating/publishing "
                            "audit event log entries"))
            .aggregate(aggregate_labels));
        return defs;
    };

    auto group_name = prometheus_sanitize::metrics_name("security_audit");

    if (!config::shard_local_cfg().disable_metrics()) {
        auto defs = setup_common.template
                    operator()<ss::metrics::impl::metric_definition_impl>({});
        defs.emplace_back(sm::make_gauge(
          "buffer_usage_ratio",
          [fn = std::move(get_usage_ratio)] { return fn(); },
          sm::description("Audit event buffer usage ratio.")));

        _metrics.add_group(group_name, defs, {}, {sm::shard_label});
    }

    if (!config::shard_local_cfg().disable_public_metrics()) {
        _public_metrics.add_group(
          group_name,
          setup_common.template operator()<ss::metrics::metric_definition>(
            {sm::shard_label}));
    }
}

void client_probe::setup_metrics(std::function<double()> get_usage_ratio) {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
      prometheus_sanitize::metrics_name("security_audit_client"),
      {sm::make_gauge(
        "buffer_usage_ratio",
        [fn = std::move(get_usage_ratio)] { return fn(); },
        sm::description("Audit client send buffer usage ratio"))},
      {},
      {sm::shard_label});
}
} // namespace security::audit
