/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/partition_leaders_table.h"
#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "ssx/metrics.h"

#include <seastar/core/metrics.hh>

class scheduling_groups_probe {
public:
    void wire_up(const scheduling_groups& scheduling_groups) {
        if (config::shard_local_cfg().disable_public_metrics()) {
            return;
        }

        auto groups = scheduling_groups.all_scheduling_groups();
        for (const auto& group_ref : groups) {
            _public_metrics.add_group(
              prometheus_sanitize::metrics_name("scheduler"),
              {seastar::metrics::make_counter(
                "runtime_seconds_total",
                [group_ref] {
                    auto runtime_duration = group_ref.get().get_stats().runtime;
                    return std::chrono::duration<double>(runtime_duration)
                      .count();
                },
                seastar::metrics::description(
                  "Accumulated runtime of task queue associated with this "
                  "scheduling group"),
                {ssx::metrics::make_namespaced_label("scheduling_group")(
                  group_ref.get().name())})});
        }
    }

    void clear() { _public_metrics.clear(); }

private:
    seastar::metrics::metric_groups _public_metrics{
      ssx::metrics::public_metrics_handle};
};
