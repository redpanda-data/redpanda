// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_table_probe.h"

#include "cluster/cluster_utils.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

namespace cluster {

topic_table_probe::topic_table_probe(const topic_table& topic_table)
  : _topic_table(topic_table)
  , _node_id(*config::node().node_id()) {
    setup_metrics();
}

void topic_table_probe::setup_metrics() {
    setup_public_metrics();
    setup_internal_metrics();
}

void topic_table_probe::setup_internal_metrics() {
    if (
      config::shard_local_cfg().disable_metrics() || ss::this_shard_id() != 0) {
        return;
    }

    // TODO: Delete this metric after public is supported in scraping
    // configurations
    namespace sm = ss::metrics;
    _internal_metrics.add_group(
      prometheus_sanitize::metrics_name("cluster:partition"),
      {
        sm::make_gauge(
          "moving_to_node",
          [this] { return _moving_to_partitions; },
          sm::description("Amount of partitions that are moving to node"))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "moving_from_node",
          [this] { return _moving_from_partitions; },
          sm::description("Amount of partitions that are moving from node"))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "node_cancelling_movements",
          [this] { return _cancelling_movements; },
          sm::description("Amount of cancelling partition movements for node"))
          .aggregate({sm::shard_label}),
      });
}

void topic_table_probe::setup_public_metrics() {
    if (
      config::shard_local_cfg().disable_public_metrics()
      || ss::this_shard_id() != 0) {
        return;
    }
    namespace sm = ss::metrics;
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("cluster:partition"),
      {
        sm::make_gauge(
          "moving_to_node",
          [this] { return _moving_to_partitions; },
          sm::description("Amount of partitions that are moving to node"))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "moving_from_node",
          [this] { return _moving_from_partitions; },
          sm::description("Amount of partitions that are moving from node"))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "node_cancelling_movements",
          [this] { return _cancelling_movements; },
          sm::description("Amount of cancelling partition movements for node"))
          .aggregate({sm::shard_label}),
      });
}

void topic_table_probe::handle_update(
  const std::vector<model::broker_shard>& previous_replicas,
  const std::vector<model::broker_shard>& result_replicas) {
    if (moving_from_node(_node_id, previous_replicas, result_replicas)) {
        _moving_from_partitions += 1;
    } else if (moving_to_node(_node_id, previous_replicas, result_replicas)) {
        _moving_to_partitions += 1;
    }
}

void topic_table_probe::handle_update_finish(
  const std::vector<model::broker_shard>& previous_replicas,
  const std::vector<model::broker_shard>& result_replicas) {
    if (moving_from_node(_node_id, previous_replicas, result_replicas)) {
        _moving_from_partitions -= 1;
    } else if (moving_to_node(_node_id, previous_replicas, result_replicas)) {
        _moving_to_partitions -= 1;
    }
}

void topic_table_probe::handle_update_cancel(
  const std::vector<model::broker_shard>& previous_replicas,
  const std::vector<model::broker_shard>& result_replicas) {
    if (
      moving_from_node(_node_id, previous_replicas, result_replicas)
      || moving_to_node(_node_id, previous_replicas, result_replicas)) {
        _cancelling_movements += 1;
    }
}

void topic_table_probe::handle_update_cancel_finish(
  const std::vector<model::broker_shard>& previous_replicas,
  const std::vector<model::broker_shard>& result_replicas) {
    if (
      moving_from_node(_node_id, previous_replicas, result_replicas)
      || moving_to_node(_node_id, previous_replicas, result_replicas)) {
        _cancelling_movements -= 1;
    }
}

void topic_table_probe::handle_topic_creation(
  create_topic_cmd::key_t topic_namespace) {
    if (
      config::shard_local_cfg().disable_public_metrics()
      || ss::this_shard_id() != 0) {
        return;
    }

    const auto labels = {
      metrics::make_namespaced_label("namespace")(topic_namespace.ns()),
      metrics::make_namespaced_label("topic")(topic_namespace.tp())};

    namespace sm = ss::metrics;

    auto [it, inserted] = _topics_metrics.emplace(
      topic_namespace, metrics::public_metrics_handle);

    it->second.add_group(
      prometheus_sanitize::metrics_name("kafka"),
      {sm::make_gauge(
         "replicas",
         [this, topic_namespace] {
             auto md = _topic_table.get_topic_metadata_ref(topic_namespace);
             if (md) {
                 return md.value().get().get_replication_factor();
             }

             return cluster::replication_factor{0};
         },
         sm::description("Configured number of replicas for the topic"),
         labels)
         .aggregate({sm::shard_label}),
       sm::make_gauge(
         "partitions",
         [this, topic_namespace] {
             auto md = _topic_table.get_topic_metadata_ref(topic_namespace);
             if (md) {
                 return md.value().get().get_configuration().partition_count;
             }

             return int32_t{0};
         },
         sm::description("Configured number of partitions for the topic"),
         labels)
         .aggregate({sm::shard_label})});
}

void topic_table_probe::handle_topic_deletion(
  const delete_topic_cmd::key_t& topic_namespace) {
    if (
      config::shard_local_cfg().disable_public_metrics()
      || ss::this_shard_id() != 0) {
        return;
    }

    _topics_metrics.erase(topic_namespace);
}

} // namespace cluster
