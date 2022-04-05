/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace kafka {
class group_offset_probe {
public:
    explicit group_offset_probe(model::offset& offset) noexcept
      : _offset(offset) {}

    void setup_metrics(
      const kafka::group_id& group_id, const model::topic_partition& tp) {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        auto group_label = sm::label("group");
        auto topic_label = sm::label("topic");
        auto partition_label = sm::label("partition");
        std::vector<sm::label_instance> labels{
          group_label(group_id()),
          topic_label(tp.topic()),
          partition_label(tp.partition())};
        _metrics.add_group(
          prometheus_sanitize::metrics_name("kafka:group"),
          {sm::make_gauge(
            "offset",
            [this] { return _offset; },
            sm::description("Group topic partition offset"),
            labels)});
    }

private:
    model::offset& _offset;
    ss::metrics::metric_groups _metrics;
};

} // namespace kafka
