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

#include "metrics/metrics.h"
#include "model/transform.h"

#include <seastar/core/smp.hh>

namespace transform::logging {

class logger_probe {
public:
    logger_probe() = default;
    logger_probe(const logger_probe&) = delete;
    logger_probe& operator=(const logger_probe&) = delete;
    logger_probe(logger_probe&&) = delete;
    logger_probe& operator=(logger_probe&&) = delete;
    ~logger_probe() {}

    void setup_metrics(model::transform_name_view transform_name);

    void log_event() { ++_total_log_events; }
    void dropped_log_event() { ++_total_dropped_log_events; }

private:
    uint64_t _total_log_events{0};
    uint64_t _total_dropped_log_events{0};
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

class manager_probe {
public:
    manager_probe() = default;
    manager_probe(const manager_probe&) = delete;
    manager_probe& operator=(const manager_probe&) = delete;
    manager_probe(manager_probe&&) = delete;
    manager_probe& operator=(manager_probe&&) = delete;
    ~manager_probe() = default;

    void setup_metrics(std::function<double()> get_usage_ratio);

    void write_error() { ++_total_write_errors; }

private:
    uint64_t _total_write_errors{0};
    metrics::internal_metric_groups _metrics;
};

} // namespace transform::logging
