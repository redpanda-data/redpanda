/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "metrics/metrics.h"

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
