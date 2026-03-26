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

#include "cloud_topics/types.h"
#include "metrics/metrics.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/metrics_types.hh>

#include <optional>

namespace cloud_topics {

class level_zero_gc_probe {
public:
    explicit level_zero_gc_probe(bool disable);

    void objects_deleted(uint64_t count = 1) { objects_deleted_ += count; }
    void bytes_deleted(uint64_t bytes) { bytes_deleted_ += bytes; }
    void objects_listed(uint64_t count) { objects_listed_ += count; }
    void object_skipped_not_eligible() { objects_skipped_not_eligible_++; }
    void object_skipped_too_young() { objects_skipped_too_young_++; }
    void collection_round() { collection_rounds_++; }
    void delete_request() { delete_requests_++; }
    void list_request() { list_requests_++; }
    void list_error() { list_errors_++; }
    void delete_error() { delete_errors_++; }
    void add_backpressure(double seconds) { backpressure_seconds_ += seconds; }
    void safety_blocked() { safety_blocked_rounds_++; }
    void set_max_gc_eligible_epoch(cluster_epoch epoch) {
        max_gc_eligible_epoch_ = epoch;
    }
    void set_min_partition_gc_epoch(cluster_epoch epoch) {
        min_partition_gc_epoch_ = epoch;
    }
    void report_deletion_epoch(cluster_epoch epoch);
    /// Accept the next deletion epoch, unconditionally, but don't reset the
    /// currently cached value yet. This way we won't see little spikes in lag
    /// between GC iterations.
    void reset_deletion_epoch() { reset_deletion_epoch_ = true; }

private:
    void setup_internal_metrics(bool disable);
    cloud_topics::cluster_epoch::type epoch_lag() const;

    uint64_t objects_deleted_{0};
    uint64_t bytes_deleted_{0};
    uint64_t objects_listed_{0};
    uint64_t objects_skipped_not_eligible_{0};
    uint64_t objects_skipped_too_young_{0};
    uint64_t collection_rounds_{0};
    uint64_t delete_requests_{0};
    uint64_t list_requests_{0};
    uint64_t list_errors_{0};
    uint64_t delete_errors_{0};
    double backpressure_seconds_{0};
    uint64_t safety_blocked_rounds_{0};

    std::optional<cluster_epoch> min_partition_gc_epoch_;
    std::optional<cluster_epoch> max_gc_eligible_epoch_;
    std::optional<cluster_epoch> min_deletion_epoch_;
    std::optional<cluster_epoch> max_deleted_epoch_;
    bool reset_deletion_epoch_{true};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics
