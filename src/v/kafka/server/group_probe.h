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

#include "absl/container/node_hash_map.h"
#include "config/configuration.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "kafka/server/member.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "model/fundamental.h"
#include "model/namespace.h"

#include <seastar/core/metrics.hh>

#include <algorithm>

namespace kafka {

struct enabled_metrics {
    bool group;
    bool partition;
    bool consumer_lag;

    static auto from_vector(const std::vector<ss::sstring>& metrics) {
        return enabled_metrics{
          .group = std::ranges::contains(metrics, "group"),
          .partition = std::ranges::contains(metrics, "partition"),
          .consumer_lag = std::ranges::contains(metrics, "consumer_lag")};
    }

    constexpr auto operator<=>(const enabled_metrics&) const = default;
};

using metrics_conversion_binding
  = config::conversion_binding<enabled_metrics, std::vector<ss::sstring>>;

class group_offset_probe {
public:
    explicit group_offset_probe(model::offset& offset) noexcept
      : _offset(offset) {}
    group_offset_probe(const group_offset_probe&) = delete;
    group_offset_probe& operator=(const group_offset_probe&) = delete;
    group_offset_probe(group_offset_probe&&) = delete;
    group_offset_probe& operator=(group_offset_probe&&) = delete;
    ~group_offset_probe() = default;

    void register_metrics(
      const kafka::group_id& group_id, const model::topic_partition& tp) {
        if (_internal_metrics.has_value()) {
            return;
        }

        _internal_metrics.emplace();
        _setup_metrics(group_id, tp);
    }
    void deregister_metrics() { _internal_metrics.reset(); }

    void register_public_metrics(
      const kafka::group_id& group_id, const model::topic_partition& tp) {
        if (_public_metrics.has_value()) {
            return;
        }

        _public_metrics.emplace();
        _setup_public_metrics(group_id, tp);
    }
    void deregister_public_metrics() { _public_metrics.reset(); }

    void reset() {
        deregister_metrics();
        deregister_public_metrics();
    }

private:
    void _setup_metrics(
      const kafka::group_id& group_id, const model::topic_partition& tp) {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        auto group_label = sm::label("group");
        std::vector<sm::label_instance> labels{
          group_label(group_id()),
          metrics::topic_label(tp.topic()),
          metrics::partition_label(tp.partition())};
        _internal_metrics.value().add_group(
          prometheus_sanitize::metrics_name("kafka:group"),
          {sm::make_gauge(
            "offset",
            [this] { return _offset; },
            sm::description("Group topic partition offset"),
            labels)});
    }

    void _setup_public_metrics(
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

        _public_metrics.value().add_group(
          prometheus_sanitize::metrics_name("kafka:consumer:group"),
          {sm::make_gauge(
            "committed_offset",
            [this] { return _offset; },
            sm::description("Consumer group committed offset"),
            labels)});
    }

    struct metric_groups {
        metrics::internal_metric_groups internal_metrics;
        metrics::public_metric_groups public_metrics;
    };

    model::offset& _offset;
    std::optional<metrics::internal_metric_groups> _internal_metrics;
    std::optional<metrics::public_metric_groups> _public_metrics;
};

struct consumer_lag_metrics {
    size_t sum{};
    size_t max{};
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
      const offsets_map& offsets,
      consumer_lag_metrics& _lag_metrics) noexcept
      : _members(members)
      , _static_members(static_members)
      , _offsets(offsets)
      , _lag_metrics(_lag_metrics) {}

    group_probe(const group_probe&) = delete;
    group_probe& operator=(const group_probe&) = delete;
    group_probe(group_probe&&) = delete;
    group_probe& operator=(group_probe&&) = delete;
    ~group_probe() = default;

    void register_group_metrics(const kafka::group_id& group_id) {
        if (_public_group_metrics.has_value()) {
            return;
        }

        _public_group_metrics.emplace();
        _setup_public_metrics(group_id);
    }

    void deregister_group_metrics() { _public_group_metrics.reset(); }

    void register_consumer_lag_metrics(const kafka::group_id& group_id) {
        if (_public_consumer_lag_metrics.has_value()) {
            return;
        }

        _public_consumer_lag_metrics.emplace();
        _setup_consumer_lag_metrics(group_id);
    }

    void deregister_consumer_lag_metrics() {
        _public_consumer_lag_metrics.reset();
    }

    void reset() {
        deregister_group_metrics();
        deregister_consumer_lag_metrics();
    }

private:
    void _setup_public_metrics(const kafka::group_id& group_id) {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_public_metrics()) {
            return;
        }

        auto group_label = metrics::make_namespaced_label("group");

        std::vector<sm::label_instance> labels{group_label(group_id())};

        _public_group_metrics.value().add_group(
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

    void _setup_consumer_lag_metrics(const kafka::group_id& group_id) {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_public_metrics()) {
            return;
        }

        auto group_label = metrics::make_namespaced_label("group");
        std::vector<sm::label_instance> labels{
          group_label(group_id()), sm::shard_label("")};

        _public_consumer_lag_metrics.value().add_group(
          prometheus_sanitize::metrics_name("kafka:consumer:group"),
          {sm::make_gauge(
             "lag_sum",
             [this] { return _lag_metrics.sum; },
             sm::description(
               "Sum of consumer group lag for all topic-partitions"),
             labels),
           sm::make_gauge(
             "lag_max",
             [this] { return _lag_metrics.max; },
             sm::description(
               "Maximum consumer group lag across topic-partitions"),
             labels)});
    }

    member_map& _members;
    static_member_map& _static_members;
    const offsets_map& _offsets;
    consumer_lag_metrics& _lag_metrics;
    std::optional<metrics::public_metric_groups> _public_group_metrics;
    std::optional<metrics::public_metric_groups> _public_consumer_lag_metrics;
};

} // namespace kafka
