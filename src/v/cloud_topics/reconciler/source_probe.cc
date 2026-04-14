/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/reconciler/source_probe.h"

#include "cloud_topics/reconciler/reconciliation_source.h"
#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace cloud_topics::reconciler {

source_probe::source_probe(const model::ntp& ntp, source& src)
  : _source(src) {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    const std::vector<sm::label_instance> labels = {
      metrics::namespace_label(ntp.ns()),
      metrics::topic_label(ntp.tp.topic()),
      metrics::partition_label(ntp.tp.partition()),
    };

    _metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_topics:reconciler"),
      {sm::make_gauge(
        "pending_offset_lag",
        [this] { return _source.pending_offset_lag(); },
        sm::description(
          "Number of offsets pending reconciliation from L0 to L1"),
        labels)},
      {},
      {sm::shard_label, metrics::partition_label});
}

} // namespace cloud_topics::reconciler
