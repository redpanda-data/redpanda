/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "ssx/metrics.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread_cputime_clock.hh>

#include <chrono>
#include <memory>

namespace wasm {

class probe;

// Per transform probe
class probe {
public:
    using hist_t = log_hist_public;

    probe() = default;
    probe(const probe&) = delete;
    probe& operator=(const probe&) = delete;
    probe(probe&&) = delete;
    probe& operator=(probe&&) = delete;
    ~probe() = default;

    std::unique_ptr<hist_t::measurement> latency_measurement() {
        return _transform_latency.auto_measure();
    }
    void transform_complete() { ++_transform_count; }
    void transform_error() { ++_transform_errors; }

    void setup_metrics(ss::sstring transform_name);
    void clear_metrics() { _public_metrics.clear(); }

private:
    uint64_t _transform_count{0};
    uint64_t _transform_errors{0};
    hist_t _transform_latency;
    ssx::metrics::metric_groups _public_metrics
      = ssx::metrics::metric_groups::make_public();
};
} // namespace wasm
