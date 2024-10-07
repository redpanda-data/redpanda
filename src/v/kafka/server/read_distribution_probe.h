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

#pragma once

#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics.hh>

#include <chrono>

namespace kafka {
class read_distribution_probe {
public:
    using hist_t = log_hist_read_dist;

    read_distribution_probe() = default;
    read_distribution_probe(const read_distribution_probe&) = delete;
    read_distribution_probe& operator=(const read_distribution_probe&) = delete;
    read_distribution_probe(read_distribution_probe&&) = delete;
    read_distribution_probe& operator=(read_distribution_probe&&) = delete;
    ~read_distribution_probe() = default;

    template<typename dur_t>
    void add_read_event_delta_from_tip(dur_t delta_from_tip) {
        _read_distribution.record(delta_from_tip);
    };

    void setup_metrics() {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        _metrics.add_group(
          prometheus_sanitize::metrics_name("kafka_fetch"),
          {
            sm::make_histogram(
              "read_distribution",
              [this] {
                  return _read_distribution.read_dist_histogram_logform();
              },
              sm::description("Read path time distribution histogram"),
              {}),
          },
          {},
          {sm::shard_label});
    }

private:
    hist_t _read_distribution;
    metrics::internal_metric_groups _metrics;
};

} // namespace kafka
