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

#include "cluster_link/tests/source_cluster.h"

#include <cstdint>

namespace cluster_link::tests {

source_topic::source_topic(
  ::model::node_id controller_id,
  cluster::topic_configuration tp_cfg,
  int32_t ops)
  : _tp_cfg(std::move(tp_cfg))
  , _tp_ns(_tp_cfg.tp_ns)
  , _metadata(create_topic_metadata(controller_id, _tp_cfg))
  , _authorized_operations(ops) {}

::model::topic_namespace_view source_topic::tp_ns() const { return _tp_ns; }
const ::model::topic_metadata& source_topic::model_metadata() const {
    return _metadata;
}
int32_t source_topic::authorized_operations() const {
    return _authorized_operations;
}
const cluster::topic_configuration& source_topic::topic_configuration() {
    return _tp_cfg;
}
void source_topic::update_metadata(::model::topic_metadata md) {
    _metadata = std::move(md);
}
void source_topic::set_authorized_operations(int32_t ops) {
    _authorized_operations = ops;
}

::model::topic_metadata source_topic::create_topic_metadata(
  ::model::node_id controller_id, const cluster::topic_configuration& tp_cfg) {
    const auto create_partitions =
      [controller_id, &tp_cfg]() -> std::vector<::model::partition_metadata> {
        std::vector<::model::partition_metadata> partitions;
        partitions.reserve(tp_cfg.partition_count);
        for (model::partition_id i = model::partition_id(0);
             i < tp_cfg.partition_count;
             ++i) {
            ::model::partition_metadata pm(i);
            pm.leader_node = controller_id;
            pm.replicas.emplace_back(
              ::model::broker_shard{.node_id = controller_id, .shard = 0});
            partitions.emplace_back(std::move(pm));
        }
        return partitions;
    };
    ::model::topic_metadata md(tp_cfg.tp_ns);
    md.partitions = create_partitions();
    return md;
}

source_cluster::source_cluster(
  ss::sstring cluster_id, ::model::node_id controller_id, int32_t ops)
  : _cluster_id(std::move(cluster_id))
  , _controller_id(controller_id)
  , _cluster_authorized_operations(ops) {}
const ss::sstring& source_cluster::cluster_id() const { return _cluster_id; }
::model::node_id source_cluster::controller_id() const {
    return _controller_id;
}
int32_t source_cluster::cluster_authorized_operations() const {
    return _cluster_authorized_operations;
}
void source_cluster::set_cluster_authorized_operations(int32_t ops) {
    _cluster_authorized_operations = ops;
}
void source_cluster::add_topic(source_topic topic) {
    auto name = ::model::topic_namespace{topic.tp_ns()};
    _topics.emplace(std::move(name), std::move(topic));
}
std::optional<std::reference_wrapper<source_topic>>
source_cluster::find_topic(::model::topic_namespace_view tp_ns) {
    auto it = _topics.find(::model::topic_namespace{tp_ns});
    if (it != _topics.end()) {
        return std::ref(it->second);
    }
    return std::nullopt;
}
std::optional<::model::topic_metadata>
source_cluster::get_topic_metadata(::model::topic_namespace_view tp_ns) const {
    auto it = _topics.find(::model::topic_namespace{tp_ns});
    if (it != _topics.end()) {
        return it->second.model_metadata();
    }
    return std::nullopt;
}
chunked_vector<::model::topic_namespace> source_cluster::all_topics() const {
    chunked_vector<::model::topic_namespace> topics;
    topics.reserve(_topics.size());
    for (const auto& tp_ns : std::views::keys(_topics)) {
        topics.push_back(tp_ns);
    }
    return topics;
}
void source_cluster::delete_topic(::model::topic_namespace_view tp_ns) {
    _topics.erase(::model::topic_namespace{tp_ns});
}
} // namespace cluster_link::tests
