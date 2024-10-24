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

class audit_probe {
    using clock_type = ss::lowres_system_clock;

public:
    audit_probe() = default;
    audit_probe(const audit_probe&) = delete;
    audit_probe& operator=(const audit_probe&) = delete;
    audit_probe(audit_probe&&) = delete;
    audit_probe& operator=(audit_probe&&) = delete;
    ~audit_probe() = default;

    void setup_metrics(std::function<double()> get_usage_ratio);

    void audit_event() { _last_event = clock_type::now(); }
    void audit_error() { ++_audit_error_count; }

private:
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;

    clock_type::time_point _last_event;
    uint32_t _audit_error_count{0};
};

} // namespace security::audit
