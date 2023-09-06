// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/health_monitor_backend.h"

namespace cluster {
// needed only for access to private members
struct health_report_accessor {
    // the original cap on leaderless and urp partitions
    static constexpr size_t original_limit = 128;

    using aggregated_report = health_monitor_backend::aggregated_report;
    using report_cache_t = health_monitor_backend::report_cache_t;

    static auto aggregate(report_cache_t& reports) {
        return health_monitor_backend::aggregate_reports(reports);
    }
};
} // namespace cluster
