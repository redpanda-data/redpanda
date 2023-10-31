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

#include "metrics/metrics.h"
#include "seastarx.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics_registration.hh>

namespace security::audit {
class client_probe {
    using clock_type = ss::lowres_system_clock;

public:
    client_probe() = default;
    client_probe(const client_probe&) = delete;
    client_probe& operator=(const client_probe&) = delete;
    client_probe(client_probe&&) = delete;
    client_probe& operator=(client_probe&&) = delete;
    ~client_probe() = default;

    void setup_metrics(std::function<double()> get_usage_ratio);

private:
    metrics::internal_metric_groups _metrics;
};

} // namespace security::audit
