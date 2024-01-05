/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/seastarx.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>

namespace cluster {

class leader_balancer_probe final {
public:
    void setup_metrics();

    void leader_transfer_error() { ++_leader_transfer_error; }
    void leader_transfer_succeeded() { ++_leader_transfer_succeeded; }
    void leader_transfer_timeout() { ++_leader_transfer_timeout; }
    void leader_transfer_no_improvement() { ++_leader_transfer_no_improvement; }

private:
    uint64_t _leader_transfer_error = 0;
    uint64_t _leader_transfer_succeeded = 0;
    uint64_t _leader_transfer_timeout = 0;
    uint64_t _leader_transfer_no_improvement = 0;

    metrics::internal_metric_groups _metrics;
};

} // namespace cluster
