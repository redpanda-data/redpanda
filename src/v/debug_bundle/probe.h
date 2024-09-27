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

#include "debug_bundle/types.h"
#include "metrics/metrics.h"

namespace debug_bundle {
class probe {
public:
    void setup_metrics();

    void successful_bundle_generation(clock::time_point time) {
        _last_successful_bundle = time;
        ++_successful_generation_count;
    }

    void failed_bundle_generation(clock::time_point time) {
        _last_failed_bundle = time;
        ++_failed_generation_count;
    }

private:
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;

    clock::time_point _last_successful_bundle;
    clock::time_point _last_failed_bundle;
    uint32_t _successful_generation_count{0};
    uint32_t _failed_generation_count{0};
};
} // namespace debug_bundle
