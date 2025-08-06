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

#include "cluster_link/errc.h"
#include "cluster_link/fwd.h"
#include "cluster_link/model/types.h"
#include "kafka/client/cluster.h"
#include "kafka/data/rpc/deps.h"
#include "model/fundamental.h"

namespace cluster_link {

/**
 * @brief Abstract class that provides accessors to cluster link table
 *
 */
class link_registry {
public:
    link_registry() = default;
    link_registry(const link_registry&) = delete;
    link_registry(link_registry&&) = delete;
    link_registry& operator=(const link_registry&) = delete;
    link_registry& operator=(link_registry&&) = delete;
    virtual ~link_registry() = default;

    virtual ss::future<::cluster::cluster_link::errc>
    upsert_link(model::metadata md, ::model::timeout_clock::time_point) = 0;

    virtual std::optional<std::reference_wrapper<const model::metadata>>
      find_link_by_id(model::id_t) const = 0;

    virtual std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_name(const model::name_t&) const = 0;

    virtual chunked_vector<model::id_t> get_all_link_ids() const = 0;

    virtual ss::future<::cluster::cluster_link::errc> add_mirror_topic(
      model::id_t,
      model::add_mirror_topic_cmd,
      ::model::timeout_clock::time_point)
      = 0;

    virtual ss::future<::cluster::cluster_link::errc> update_mirror_topic_state(
      model::id_t,
      model::update_mirror_topic_state_cmd,
      ::model::timeout_clock::time_point)
      = 0;

    virtual ss::future<::cluster::cluster_link::errc>
      update_mirror_topic_properties(
        model::id_t,
        model::update_mirror_topic_properties_cmd,
        ::model::timeout_clock::time_point)
      = 0;

    virtual std::optional<chunked_hash_map<
      ::model::topic,
      ::cluster_link::model::mirror_topic_metadata>>
    get_mirror_topics_for_link(model::id_t id) const = 0;
};

/**
 * @brief Factory abstract class to create new links
 *
 */
class link_factory {
public:
    link_factory() = default;
    link_factory(const link_factory&) = delete;
    link_factory(link_factory&&) = delete;
    link_factory& operator=(const link_factory&) = delete;
    link_factory& operator=(link_factory&&) = delete;
    virtual ~link_factory() = default;

    virtual std::unique_ptr<link> create_link(
      ::model::node_id self,
      model::id_t link_id,
      manager* manager,
      model::metadata config,
      kafka::client::cluster cluster_connection)
      = 0;
};

/**
 * @brief Abstract class used to create cluster links
 *
 */
class cluster_factory {
public:
    cluster_factory() = default;
    cluster_factory(const cluster_factory&) = delete;
    cluster_factory(cluster_factory&&) = delete;
    cluster_factory& operator=(const cluster_factory&) = delete;
    cluster_factory& operator=(cluster_factory&&) = delete;
    virtual ~cluster_factory() = default;

    virtual kafka::client::cluster create_cluster(const model::metadata& md);
};
} // namespace cluster_link
