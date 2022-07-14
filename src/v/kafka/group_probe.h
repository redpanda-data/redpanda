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
#include "kafka/server/member.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "prometheus/prometheus_sanitize.h"
#include "ssx/metrics.h"

#include <seastar/core/metrics.hh>

#include <absl/container/node_hash_map.h>

namespace kafka {
class group_offset_probe {
public:
    explicit group_offset_probe(model::offset& offset) noexcept
      : _offset(offset)
      , _public_metrics(ssx::metrics::public_metrics_handle) {}

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

    void setup_public_metrics(
      const kafka::group_id& group_id, const model::topic_partition& tp) {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_public_metrics()) {
            return;
        }

        auto group_label = ssx::metrics::make_namespaced_label("group");
        auto topic_label = ssx::metrics::make_namespaced_label("topic");
        auto partition_label = ssx::metrics::make_namespaced_label("partition");
        std::vector<sm::label_instance> labels{
          group_label(group_id()),
          topic_label(tp.topic()),
          partition_label(tp.partition())};

        _public_metrics.add_group(
          prometheus_sanitize::metrics_name("kafka:consumer:group"),
          {sm::make_gauge(
             "committed_offset",
             [this] { return _offset; },
             sm::description("Consumer group committed offset"),
             labels)
             .aggregate({sm::shard_label})});
    }

private:
    model::offset& _offset;
    ss::metrics::metric_groups _metrics;
    ss::metrics::metric_groups _public_metrics;
};

template<typename KeyType, typename ValType>
class group_probe {
    using member_map = absl::node_hash_map<kafka::member_id, member_ptr>;
    using static_member_map
      = absl::node_hash_map<kafka::group_instance_id, kafka::member_id>;
    using offsets_map = absl::node_hash_map<KeyType, ValType>;

public:
    explicit group_probe(
      member_map& members,
      static_member_map& static_members,
      offsets_map& offsets) noexcept
      : _members(members)
      , _static_members(static_members)
      , _offsets(offsets)
      , _public_metrics(ssx::metrics::public_metrics_handle) {}

    void setup_public_metrics(const kafka::group_id& group_id) {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_public_metrics()) {
            return;
        }

        auto group_label = ssx::metrics::make_namespaced_label("group");

        std::vector<sm::label_instance> labels{group_label(group_id())};

        _public_metrics.add_group(
          prometheus_sanitize::metrics_name("kafka:consumer:group"),
          {sm::make_gauge(
             "consumers",
             [this] { return _members.size() + _static_members.size(); },
             sm::description("Number of consumers in a group"),
             labels)
             .aggregate({sm::shard_label}),

           sm::make_gauge(
             "topics",
             [this] { return _offsets.size(); },
             sm::description("Number of topics in a group"),
             labels)
             .aggregate({sm::shard_label})});
    }

private:
    member_map& _members;
    static_member_map& _static_members;
    offsets_map& _offsets;
    ss::metrics::metric_groups _public_metrics;
};

} // namespace kafka
