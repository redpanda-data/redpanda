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

#include "seastarx.h"
#include "ssx/metrics.h"

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
    ssx::metrics::metric_groups _metrics
      = ssx::metrics::metric_groups::make_internal();
    ssx::metrics::metric_groups _public_metrics
      = ssx::metrics::metric_groups::make_public();

    clock_type::time_point _last_event;
    uint32_t _audit_error_count{0};
};

} // namespace security::audit
