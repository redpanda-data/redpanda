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
#include "metrics.h"

#include "base/vassert.h"
#include "config/configuration.h"

namespace metrics {

internal_metric_groups& internal_metric_groups::add_group(
  const ss::metrics::group_name_type& name,
  std::vector<ss::metrics::impl::metric_definition_impl> metrics,
  const std::vector<ss::metrics::label>& non_aggregated_labels,
  const std::vector<ss::metrics::label>& aggregated_labels) {
    /*
     * We use the `metrics_registry` under the hood such that the
     * aggregation labels will be automatically updated when the config flag
     * changes.
     *
     * Implementation note: Taking `impl::metric_definition_impl` isn't
     * great but that's a consequence of how the seastar metrics API works.
     * `ss::metrics::metric_definition` is just a thin wrapper around
     * `impl::metric_definition_impl` which doesn't expose any of the
     * members (why?). Further it actually takes a
     * `impl::metric_definition_impl` as its main constructor argument which
     * is returned by all the `make_` functions. Hence we are forced to use
     * the `impl` type here as well as we need access to the metric names.
     */

    std::vector<ss::metrics::metric_definition> transformed;
    transformed.reserve(metrics.size());
    for (auto& metric : metrics) {
        vassert(
          metric.aggregate_labels.empty(),
          "Must not use individual aggregation labels when using the "
          "aggregation label per group overload");

        metric.aggregate(
          config::shard_local_cfg().aggregate_metrics()
            ? aggregated_labels
            : non_aggregated_labels);
        transformed.emplace_back(metric);

        metrics_registry::local().register_metric(
          name, metric.name, non_aggregated_labels, aggregated_labels);
    }
    _underlying.add_group(name, transformed);
    return *this;
}

} // namespace metrics
