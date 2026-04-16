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

#include <seastar/core/metrics_registration.hh>

#include <cstdint>

namespace cloud_topics::l1 {

class domain_manager_probe {
public:
    domain_manager_probe() = default;

    domain_manager_probe(const domain_manager_probe&) = delete;
    domain_manager_probe& operator=(const domain_manager_probe&) = delete;
    domain_manager_probe(domain_manager_probe&&) = delete;
    domain_manager_probe& operator=(domain_manager_probe&&) = delete;
    ~domain_manager_probe() = default;

    void setup_metrics();

    void objects_preregistered(uint64_t count) {
        _objects_preregistered += count;
    }
    void gc_objects_deleted(uint64_t count) { _gc_objects_deleted += count; }
    void gc_object_deletions_replicated(uint64_t count) {
        _gc_object_deletions_replicated += count;
    }
    void gc_objects_expired(uint64_t count) { _gc_objects_expired += count; }

private:
    uint64_t _objects_preregistered{0};
    uint64_t _gc_objects_deleted{0};
    uint64_t _gc_object_deletions_replicated{0};
    uint64_t _gc_objects_expired{0};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::l1
