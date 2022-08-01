// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_table_probe.h"

#include "cluster/topic_table.h"
#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"

namespace cluster {

topic_table_probe::topic_table_probe(const topic_table& topic_table)
  : _topic_table(topic_table) {}

void topic_table_probe::handle_topic_creation(
  create_topic_cmd::key_t topic_namespace) {
    if (
      config::shard_local_cfg().disable_public_metrics()
      || ss::this_shard_id() != 0) {
        return;
    }

    const auto labels = {
      ssx::metrics::make_namespaced_label("namespace")(topic_namespace.ns()),
      ssx::metrics::make_namespaced_label("topic")(topic_namespace.tp())};

    namespace sm = ss::metrics;

    sm::metric_groups topic_metrics{ssx::metrics::public_metrics_handle};

    topic_metrics.add_group(
      prometheus_sanitize::metrics_name("kafka"),
      {sm::make_gauge(
         "replicas",
         [this, topic_namespace] {
             auto md = _topic_table.get_topic_metadata_ref(topic_namespace);
             if (md) {
                 return md.value().get().get_configuration().replication_factor;
             }

             return int16_t{0};
         },
         sm::description("Configured number of replicas for the topic"),
         labels)
         .aggregate({sm::shard_label})});

    _topics_metrics.emplace(
      std::move(topic_namespace), std::move(topic_metrics));
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
