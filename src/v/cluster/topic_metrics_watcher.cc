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

#include "topic_metrics_watcher.h"

#include "cluster/logger.h"
#include "metrics/metrics_registry.h"

namespace cluster {

topic_metrics_watcher::topic_metrics_watcher(
  topic_table& tt, config::binding<std::optional<size_t>>&& topic_agg_limit)
  : _tt(tt)
  , _topic_agg_limit(std::move(topic_agg_limit)) {
    tt.register_topic_delta_notification(
      [this](const auto&) { maybe_aggregate_topic_label(); });
    _topic_agg_limit.watch([this] { maybe_aggregate_topic_label(); });

    maybe_aggregate_topic_label();
}

ss::future<> topic_metrics_watcher::stop() { return _gate.close(); }

void topic_metrics_watcher::maybe_aggregate_topic_label() {
    auto holder = _gate.try_hold();
    if (!holder) {
        return;
    }

    const auto agg_limit_opt = _topic_agg_limit();
    if (!agg_limit_opt) {
        if (is_aggregated) {
            metrics_registry::local().disable_topic_label_aggregation();
            is_aggregated = false;
        }
        return;
    }

    const auto total_topics = _tt.all_topics_count();
    if (
      ((total_topics * hysteresis_coeff) <= *agg_limit_opt) && is_aggregated) {
        vlog(
          clusterlog.info,
          "topic count({}) is below the aggregation limit({}); "
          "un-aggregating the topic label.",
          total_topics,
          *agg_limit_opt);
        metrics_registry::local().disable_topic_label_aggregation();
        is_aggregated = false;

    } else if ((total_topics > *agg_limit_opt) && !is_aggregated) {
        vlog(
          clusterlog.info,
          "topic count({}) exceeded aggregation limit({}); aggregating the "
          "topic label.",
          total_topics,
          *agg_limit_opt);
        metrics_registry::local().enable_topic_label_aggregation();
        is_aggregated = true;
    }
}

} // namespace cluster
