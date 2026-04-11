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
#include "utils/hdr_hist.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics_registration.hh>

#include <cstdint>
#include <memory>

namespace cloud_topics::reconciler {

class reconciler_probe {
public:
    using hist_t = log_hist_internal;

    reconciler_probe() = default;

    reconciler_probe(const reconciler_probe&) = delete;
    reconciler_probe& operator=(const reconciler_probe&) = delete;
    reconciler_probe(reconciler_probe&&) = delete;
    reconciler_probe& operator=(reconciler_probe&&) = delete;
    ~reconciler_probe() = default;

    void setup_metrics();

    void increment_objects_uploaded() { ++_objects_uploaded; }
    void add_bytes_reconciled(uint64_t bytes) { _bytes_reconciled += bytes; }
    void add_batches_reconciled(uint64_t batches) {
        _batches_reconciled += batches;
    }
    void increment_partitions_reconciled() { ++_partitions_reconciled; }
    void increment_metastore_retries() { ++_metastore_retries; }
    void increment_offset_corrections() { ++_offset_corrections; }

    void record_object_size_bytes(uint64_t size) {
        _object_size_bytes.record(size);
    }
    std::unique_ptr<hist_t::measurement> measure_l0_read_duration() {
        return _l0_read_duration.auto_measure();
    }
    std::unique_ptr<hist_t::measurement> measure_object_build_duration() {
        return _object_build_duration.auto_measure();
    }

    std::unique_ptr<hist_t::measurement>
    measure_metastore_add_objects_duration() {
        return _metastore_add_objects_duration.auto_measure();
    }

    auto get_metastore_add_objects_duration_for_tests() const {
        return _metastore_add_objects_duration.internal_histogram_logform();
    }
    auto get_object_size_bytes_for_tests() const {
        return _object_size_bytes.seastar_histogram_logform();
    }

private:
    metrics::internal_metric_groups _metrics;

    uint64_t _objects_uploaded{0};
    uint64_t _bytes_reconciled{0};
    uint64_t _batches_reconciled{0};
    uint64_t _partitions_reconciled{0};
    uint64_t _metastore_retries{0};
    uint64_t _offset_corrections{0};

    // Histograms.
    hist_t _l0_read_duration;
    hist_t _object_build_duration;
    hist_t _metastore_add_objects_duration;
    hdr_hist _object_size_bytes;
};

} // namespace cloud_topics::reconciler
