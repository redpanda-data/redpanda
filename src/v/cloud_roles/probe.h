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

#include "seastarx.h"

#include <seastar/core/metrics_registration.hh>

namespace cloud_roles {

class auth_refresh_probe {
public:
    auth_refresh_probe();

    void fetch_success() { ++_successful_fetches; }
    void fetch_failed() { ++_fetch_errors; }

private:
    uint64_t _successful_fetches{0};
    uint64_t _fetch_errors{0};
    ss::metrics::metric_groups _metrics;
};

} // namespace cloud_roles
