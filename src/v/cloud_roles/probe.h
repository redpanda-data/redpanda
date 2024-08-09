/*
 * Copyright 2022 Redpanda Data, Inc.
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

#include <seastar/core/metrics_registration.hh>

namespace cloud_roles {

class auth_refresh_probe {
public:
    void setup_metrics();
    void reset() { _metrics.clear(); }

    void fetch_success() { ++_successful_fetches; }
    void fetch_failed() { ++_fetch_errors; }

    auth_refresh_probe() = default;
    auth_refresh_probe(const auth_refresh_probe&) = delete;
    auth_refresh_probe& operator=(const auth_refresh_probe&) = delete;
    auth_refresh_probe(auth_refresh_probe&&) = delete;
    auth_refresh_probe& operator=(auth_refresh_probe&&) = delete;
    ~auth_refresh_probe() = default;

private:
    uint64_t _successful_fetches{0};
    uint64_t _fetch_errors{0};
    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_roles
