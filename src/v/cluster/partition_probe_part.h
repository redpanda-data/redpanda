/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include "model/fundamental.h"

namespace cluster {

/// \see partition_probe::probe_part method.
class partition_probe_part {
public:
    /// This method is called to setup the public metrics for the partition
    /// probe part. For correct operations the metrics should be added to the
    /// provided group using the \c metrics::public_metric_groups::add_group
    /// method.
    virtual void
    setup_public_metrics(const model::ntp&, metrics::public_metric_groups&)
      = 0;

    virtual ~partition_probe_part() noexcept = default;
};

} // namespace cluster
