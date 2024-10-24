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

#pragma once

#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace kafka {
class sasl_probe {
public:
    sasl_probe() = default;
    sasl_probe(const sasl_probe&) = delete;
    sasl_probe& operator=(const sasl_probe&) = delete;
    sasl_probe(sasl_probe&&) = delete;
    sasl_probe& operator=(sasl_probe&&) = delete;
    ~sasl_probe() = default;

    void setup_metrics(std::string_view name) {
        namespace sm = ss::metrics;

        auto setup = [this]<typename MetricDef>(
                       const std::vector<sm::label>& aggregate_labels) {
            std::vector<MetricDef> defs;
            defs.emplace_back(
              sm::make_counter(
                "session_expiration_total",
                [this] { return _session_expiration_count; },
                sm::description("Total number of SASL session expirations"))
                .aggregate(aggregate_labels));
            defs.emplace_back(
              sm::make_counter(
                "session_reauth_attempts_total",
                [this] { return _session_reauth_attempts; },
                sm::description(
                  "Total number of SASL reauthentication attempts"))
                .aggregate(aggregate_labels));
            defs.emplace_back(
              sm::make_counter(
                "session_revoked_total",
                [this] { return _session_revoked_count; },
                sm::description("Total number of SASL sessions revoked"))
                .aggregate(aggregate_labels));
            return defs;
        };

        auto group_name = prometheus_sanitize::metrics_name(
          ssx::sformat("{}:sasl", name));

        if (!config::shard_local_cfg().disable_metrics()) {
            _metrics.add_group(
              group_name,
              setup.template
              operator()<ss::metrics::impl::metric_definition_impl>({}),
              std::vector<sm::label>{},
              std::vector<sm::label>{sm::shard_label});
        }

        if (!config::shard_local_cfg().disable_public_metrics()) {
            _public_metrics.add_group(
              group_name,
              setup.template operator()<ss::metrics::metric_definition>(
                std::vector<sm::label>{sm::shard_label}));
        }
    }

    void session_expired() { ++_session_expiration_count; }
    void session_reauth() { ++_session_reauth_attempts; }
    void session_revoked() { ++_session_revoked_count; }

private:
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
    uint32_t _session_expiration_count{0};
    uint32_t _session_reauth_attempts{0};
    uint32_t _session_revoked_count{0};
};

}; // namespace kafka
