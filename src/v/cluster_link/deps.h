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

    virtual ss::future<::cluster::cluster_link::errc>
      delete_link(model::name_t, ::model::timeout_clock::time_point) = 0;

    virtual std::optional<std::reference_wrapper<const model::metadata>>
      find_link_by_id(model::id_t) const = 0;

    virtual std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_name(const model::name_t&) const = 0;

    virtual std::optional<model::id_t>
    find_link_id_by_name(const model::name_t&) const = 0;

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

    virtual ss::future<::cluster::cluster_link::errc>
      update_cluster_link_configuration(
        model::id_t,
        model::update_cluster_link_configuration_cmd,
        ::model::timeout_clock::time_point)
      = 0;
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
      std::unique_ptr<kafka::client::cluster> cluster_connection)
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

    virtual std::unique_ptr<kafka::client::cluster>
    create_cluster(const model::metadata& md);
};

/**
 * Cluster linking entry point for consumer group operations in the cluster
 */
class consumer_groups_router {
public:
    consumer_groups_router() = default;
    consumer_groups_router(const consumer_groups_router&) = delete;
    consumer_groups_router(consumer_groups_router&&) = delete;
    consumer_groups_router& operator=(const consumer_groups_router&) = delete;
    consumer_groups_router& operator=(consumer_groups_router&&) = delete;
    virtual ~consumer_groups_router() = default;

    virtual std::optional<::model::partition_id>
    partition_for(const kafka::group_id&) const = 0;

    virtual ss::future<kafka::offset_commit_response>
      offset_commit(kafka::offset_commit_request) = 0;

    virtual ss::future<bool> assure_topic_exists() = 0;
};

/**
 * Cluster linking entry point for retrieving partition metadata information
 */
class partition_metadata_provider {
public:
    partition_metadata_provider() = default;
    partition_metadata_provider(const partition_metadata_provider&) = delete;
    partition_metadata_provider(partition_metadata_provider&&) = delete;
    partition_metadata_provider& operator=(const partition_metadata_provider&)
      = delete;
    partition_metadata_provider& operator=(partition_metadata_provider&&)
      = delete;
    virtual ~partition_metadata_provider() = default;

    /**
     * Returns the high watermark for a given topic partition. If the
     * information is missing or error occurs, returns std::nullopt.
     */
    virtual ss::future<std::optional<kafka::offset>>
      get_partition_high_watermark(::model::topic_partition_view) = 0;
};

/// \brief This interface class provides access to the cluster security
/// subsystem
class security_service {
public:
    security_service() = default;
    security_service(const security_service&) = delete;
    security_service(security_service&&) = delete;
    security_service& operator=(const security_service&) = delete;
    security_service& operator=(security_service&&) = delete;
    virtual ~security_service() = default;

    static std::unique_ptr<security_service>
    make_default(ss::sharded<cluster::security_frontend>*);

    virtual ss::future<std::vector<cluster::errc>> create_acls(
      std::vector<security::acl_binding>, ::model::timeout_clock::duration)
      = 0;
};
} // namespace cluster_link
