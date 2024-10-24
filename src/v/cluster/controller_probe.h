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

#include "cluster/fwd.h"
#include "metrics/metrics.h"
#include "raft/notification.h"

#include <seastar/core/metrics_registration.hh>

namespace cluster {

class controller_probe {
public:
    explicit controller_probe(cluster::controller&) noexcept;

    void start();
    void stop();

    void setup_metrics();

private:
    cluster::controller& _controller;
    std::unique_ptr<metrics::public_metric_groups> _public_metrics;
    raft::group_manager_notification_id _leadership_notification_handle;
};

} // namespace cluster
