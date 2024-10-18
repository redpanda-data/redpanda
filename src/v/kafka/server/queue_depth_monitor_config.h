/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include <chrono>

namespace kafka {

struct qdc_monitor_config {
    double latency_alpha;
    std::chrono::milliseconds max_latency;
    size_t window_count;
    std::chrono::milliseconds window_size;
    double depth_alpha;
    size_t idle_depth;
    size_t min_depth;
    size_t max_depth;
    std::chrono::milliseconds depth_update_freq;
};

} // namespace kafka
