/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

namespace cluster {

class shard_placement_table_probe {
public:
    shard_placement_table_probe() = default;
    shard_placement_table_probe(const shard_placement_table_probe&) = delete;
    shard_placement_table_probe& operator=(const shard_placement_table_probe&)
      = delete;
    shard_placement_table_probe(shard_placement_table_probe&&) = delete;
    shard_placement_table_probe& operator=(shard_placement_table_probe&&)
      = delete;

    void update_assigned(int64_t delta) { _total_assigned += delta; }
    void update_hosted(int64_t delta) { _total_hosted += delta; }
    void update_to_reconcile(int64_t delta) { _to_reconcile += delta; }
    void partition_remade() { ++_remade_partitions; }

    uint32_t remade_partitions() const { return _remade_partitions; }

    void setup_metrics();

private:
    int64_t _total_assigned = 0;
    int64_t _total_hosted = 0;
    int64_t _to_reconcile = 0;
    uint32_t _remade_partitions = 0;

    metrics::internal_metric_groups _metrics;
};

} // namespace cluster
