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
#include "ssx/metrics.h"

#include <seastar/core/metrics.hh>

#include <vector>

namespace prometheus {

inline std::vector<ss::metrics::label>
aggregate_labels(const std::vector<ss::metrics::label>& labels) {
    std::vector<ss::metrics::label> result{};

    if (!config::shard_local_cfg().aggregate_metrics()) {
        return result;
    }

    const auto& metrics_aggregation_labels
      = config::shard_local_cfg().metrics_aggregation_labels();

    for (const auto& label : labels) {
        if (metrics_aggregation_labels.contains(label.name())) {
            result.emplace_back(label.name());
        }
    }

    return result;
}

} // namespace prometheus
