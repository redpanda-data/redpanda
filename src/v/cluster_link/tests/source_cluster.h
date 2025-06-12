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

#pragma once

#include "cluster/topic_configuration.h"
#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"

namespace cluster_link::tests {

class source_topic {
public:
    source_topic(::model::node_id, cluster::topic_configuration, int32_t);
    source_topic(const source_topic&) = delete;
    source_topic(source_topic&&) = default;
    source_topic& operator=(const source_topic&) = delete;
    source_topic& operator=(source_topic&&) = default;
    ~source_topic() = default;
    ::model::topic_namespace_view tp_ns() const;
    const ::model::topic_metadata& model_metadata() const;
    const cluster::topic_configuration& topic_configuration();
    int32_t authorized_operations() const;

    void update_metadata(::model::topic_metadata);
    void set_authorized_operations(int32_t);

private:
    static ::model::topic_metadata create_topic_metadata(
      ::model::node_id, const cluster::topic_configuration&);

private:
    cluster::topic_configuration _tp_cfg;
    ::model::topic_namespace _tp_ns;
    ::model::topic_metadata _metadata;
    int32_t _authorized_operations{0};
};

class source_cluster {
public:
    source_cluster(ss::sstring, ::model::node_id, int32_t);
    source_cluster(const source_cluster&) = delete;
    source_cluster(source_cluster&&) = delete;
    source_cluster& operator=(const source_cluster&) = delete;
    source_cluster& operator=(source_cluster&&) = delete;
    ~source_cluster() = default;

    const ss::sstring& cluster_id() const;
    ::model::node_id controller_id() const;
    int32_t cluster_authorized_operations() const;

    void set_cluster_authorized_operations(int32_t);

    void add_topic(source_topic);
    std::optional<std::reference_wrapper<source_topic>>
      find_topic(::model::topic_namespace_view);
    std::optional<::model::topic_metadata>
      get_topic_metadata(::model::topic_namespace_view) const;
    chunked_vector<::model::topic_namespace> all_topics() const;
    void delete_topic(::model::topic_namespace_view);

private:
    ss::sstring _cluster_id;
    ::model::node_id _controller_id;
    int32_t _cluster_authorized_operations{0};
    chunked_hash_map<::model::topic_namespace, source_topic> _topics;
};
} // namespace cluster_link::tests
