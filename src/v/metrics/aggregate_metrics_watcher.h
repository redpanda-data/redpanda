/*
 * Copyright 2023 Redpanda Data, Inc.
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
#include "config/property.h"
#include "metrics/metrics_registry.h"

// Very simple class that watches the `aggregate_metrics` config and triggers a
// global update of aggregation labels via the `metrics_registry`
class aggregate_metrics_watcher {
public:
    aggregate_metrics_watcher()
      : _aggregate_metrics(config::shard_local_cfg().aggregate_metrics.bind()) {
        _aggregate_metrics.watch([this]() {
            metrics_registry::local().update_aggregation_labels(
              _aggregate_metrics());
        });
    }

private:
    config::binding<bool> _aggregate_metrics;
};
