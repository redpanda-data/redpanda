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
#include "cluster/scheduling/leader_balancer_probe.h"

#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"
#include "model/fundamental.h"

#include <seastar/core/metrics.hh>

namespace cluster {

void leader_balancer_probe::setup_metrics() {
    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("leader_balancer"),
      {sm::make_counter(
         "leader_transfer_error",
         [this] { return _leader_transfer_error; },
         sm::description("Number of errors attempting to transfer leader")),
       sm::make_counter(
         "leader_transfer_succeeded",
         [this] { return _leader_transfer_succeeded; },
         sm::description("Number of successful leader transfers")),
       sm::make_counter(
         "leader_transfer_timeout",
         [this] { return _leader_transfer_timeout; },
         sm::description("Number of timeouts attempting to transfer leader")),
       sm::make_counter(
         "leader_transfer_no_improvement",
         [this] { return _leader_transfer_no_improvement; },
         sm::description("Number of times no balance improvement was found"))});
}

} // namespace cluster
