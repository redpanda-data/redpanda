/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/metrics_api.hh>

#include <optional>
#include <vector>

namespace ssx::metrics {

namespace filter {

struct config {
    enum class agg_op {
        sum,
        count,
        min,
        max,
        avg,
    };

    struct metric {
        ss::sstring name;
        std::vector<ss::metrics::label> allow;
        std::optional<agg_op> op;
    };

    std::vector<metric> allow;
};

} // namespace filter

ss::metrics::impl::value_map get_filtered(const filter::config& cfg);

} // namespace ssx::metrics
