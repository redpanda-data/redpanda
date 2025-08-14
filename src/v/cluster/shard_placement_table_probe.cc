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

#include "cluster/shard_placement_table_probe.h"

namespace cluster {

void shard_placement_table_probe::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cluster:shard_placement"),
      {
        sm::make_gauge(
          "assigned_partitions",
          [this] { return _total_assigned; },
          sm::description("Number of partitions assigned to this shard")),
        sm::make_gauge(
          "hosted_partitions",
          [this] { return _total_hosted; },
          sm::description("Number of partitions hosted on this shard")),
        sm::make_gauge(
          "partitions_to_reconcile",
          [this] { return _to_reconcile; },
          sm::description("Number of partitions needing reconciliation of "
                          "shard-local state")),
        sm::make_gauge(
          "remade_partitions",
          [this] { return _remade_partitions; },
          sm::description(
            "Number of partitions that were forced to be remade")),
      });
}
} // namespace cluster
