/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "metrics/metrics.h"
#include "model/fundamental.h"

namespace datalake::coordinator {

class coordinator_probe final {
public:
    explicit coordinator_probe(model::ntp ntp);

    // An add-files request was rejected because the coordinator had too many
    // pending files.
    void increment_add_files_backpressure() { ++_add_files_backpressure; }

    // A fetch-offsets request signaled backpressure to the translator because
    // the coordinator had too many pending files.
    void increment_fetch_offsets_backpressure() {
        ++_fetch_offsets_backpressure;
    }

private:
    void register_backpressure_metrics();

    model::ntp _ntp;
    std::optional<metrics::public_metric_groups> _public_metrics;

    size_t _add_files_backpressure = 0;
    size_t _fetch_offsets_backpressure = 0;
};

} // namespace datalake::coordinator
