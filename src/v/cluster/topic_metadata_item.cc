/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/topic_metadata_item.h"

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>

namespace cluster {

topic_metadata_item::topic_metadata_item(topic_metadata metadata) noexcept
  : _metadata(std::move(metadata)) {
    setup_metrics();
}

topic_metadata_item::topic_metadata_item(topic_metadata_item&& other) noexcept
  : _metadata(std::move(other._metadata))
  , _replica_revisions(std::move(other._replica_revisions))
  , _public_metrics(std::move(other._public_metrics)) {
    setup_metrics();
}

topic_metadata_item&
topic_metadata_item::operator=(topic_metadata_item&& other) noexcept {
    _metadata = std::move(other._metadata);
    _replica_revisions = std::move(other._replica_revisions);
    _public_metrics = std::move(other._public_metrics);

    setup_metrics();

    return *this;
}

const topic_metadata& topic_metadata_item::get_metadata() const {
    return _metadata;
}

topic_metadata& topic_metadata_item::get_metadata() { return _metadata; }

const replica_revisions& topic_metadata_item::get_replica_revisions() const {
    return _replica_revisions;
}

replica_revisions& topic_metadata_item::get_replica_revisions() {
    return _replica_revisions;
}

bool topic_metadata_item::is_topic_replicable() const {
    return _metadata.is_topic_replicable();
}

assignments_set& topic_metadata_item::get_assignments() {
    return _metadata.get_assignments();
}

const assignments_set& topic_metadata_item::get_assignments() const {
    return _metadata.get_assignments();
}

model::revision_id topic_metadata_item::get_revision() const {
    return _metadata.get_revision();
}

std::optional<model::initial_revision_id>
topic_metadata_item::get_remote_revision() const {
    return _metadata.get_remote_revision();
}

const model::topic& topic_metadata_item::get_source_topic() const {
    return _metadata.get_source_topic();
}

const topic_configuration& topic_metadata_item::get_configuration() const {
    return _metadata.get_configuration();
}

topic_configuration& topic_metadata_item::get_configuration() {
    return _metadata.get_configuration();
}

void topic_metadata_item::setup_metrics() {
    namespace sm = ss::metrics;

    if (ss::this_shard_id() != 0) {
        return;
    }

    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    if (!is_topic_replicable()) {
        return;
    }

    _public_metrics.clear();

    auto tp_ns = get_configuration().tp_ns;
    std::vector<sm::label_instance> labels = {
      ssx::metrics::make_namespaced_label("namespace")(tp_ns.ns()),
      ssx::metrics::make_namespaced_label("topic")(tp_ns.tp())};

    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("kafka"),
      {
        sm::make_gauge(
          "replicas",
          [this] { return get_configuration().replication_factor; },
          sm::description("Number of configured replicas per topic"),
          labels)
          .aggregate({sm::shard_label}),
      });
}

} // namespace cluster
