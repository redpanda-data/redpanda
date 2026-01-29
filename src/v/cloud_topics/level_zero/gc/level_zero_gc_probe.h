/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "metrics/metrics.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/metrics_types.hh>

namespace cloud_topics {

class level_zero_gc_probe {
public:
    explicit level_zero_gc_probe(bool disable);

    void objects_deleted(uint64_t count = 1) { objects_deleted_ += count; }

private:
    void setup_internal_metrics(bool disable);

    uint64_t objects_deleted_{0};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics
