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
#include "container/chunked_hash_map.h"
#include "kafka/server/member.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "model/fundamental.h"
#include "model/namespace.h"

#include <seastar/core/metrics.hh>

#include <absl/container/node_hash_map.h>

namespace kafka {
class group_offset_probe {
public:
    explicit group_offset_probe(model::offset& offset) noexcept
      : _offset(offset) {}
    group_offset_probe(const group_offset_probe&) = delete;
    group_offset_probe& operator=(const group_offset_probe&) = delete;
    group_offset_probe(group_offset_probe&&) = delete;
    group_offset_probe& operator=(group_offset_probe&&) = delete;
    ~group_offset_probe() = default;

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

        auto group_label = metrics::make_namespaced_label("group");
        auto topic_label = metrics::make_namespaced_label("topic");
        auto partition_label = metrics::make_namespaced_label("partition");
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
            labels)});
    }

private:
    model::offset& _offset;
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

template<typename KeyType, typename ValType>
class group_probe {
    using member_map = absl::node_hash_map<kafka::member_id, member_ptr>;
    using static_member_map
      = chunked_hash_map<kafka::group_instance_id, kafka::member_id>;
    using offsets_map = chunked_hash_map<KeyType, ValType>;

public:
    explicit group_probe(
      member_map& members,
      static_member_map& static_members,
      offsets_map& offsets) noexcept
      : _members(members)
      , _static_members(static_members)
      , _offsets(offsets) {}

    group_probe(const group_probe&) = delete;
    group_probe& operator=(const group_probe&) = delete;
    group_probe(group_probe&&) = delete;
    group_probe& operator=(group_probe&&) = delete;
    ~group_probe() = default;

    void setup_public_metrics(const kafka::group_id& group_id) {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_public_metrics()) {
            return;
        }

        auto group_label = metrics::make_namespaced_label("group");

        std::vector<sm::label_instance> labels{group_label(group_id())};

        _public_metrics.add_group(
          prometheus_sanitize::metrics_name("kafka:consumer:group"),
          {sm::make_gauge(
             "consumers",
             [this] { return _members.size(); },
             sm::description("Number of consumers in a group"),
             labels),

           sm::make_gauge(
             "topics",
             [this] { return _offsets.size(); },
             sm::description("Number of topics in a group"),
             labels)});
    }

private:
    member_map& _members;
    static_member_map& _static_members;
    offsets_map& _offsets;
    metrics::public_metric_groups _public_metrics;
};

} // namespace kafka
