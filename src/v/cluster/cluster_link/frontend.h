/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cluster/cluster_link/table.h"
#include "cluster/cluster_link/types.h"
#include "cluster/commands.h"
#include "cluster/controller_stm.h"
#include "cluster/fwd.h"
#include "cluster_link/model/types.h"
#include "container/chunked_vector.h"
#include "features/feature_table.h"
#include "features/fwd.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"
#include "rpc/fwd.h"

#include <seastar/core/sharded.hh>

namespace cluster::cluster_link {
class frontend : public ss::peering_sharded_service<frontend> {
    using cluster_link_cmd = std::variant<
      cluster::cluster_link_upsert_cmd,
      cluster::cluster_link_remove_cmd,
      cluster::cluster_link_add_mirror_topic_cmd,
      cluster::cluster_link_delete_mirror_topic_cmd,
      cluster::cluster_link_update_mirror_topic_status_cmd,
      cluster::cluster_link_update_mirror_topic_properties_cmd,
      cluster::cluster_link_update_cluster_link_configuration_cmd>;

public:
    frontend(
      model::node_id,
      cluster::partition_leaders_table*,
      table*,
      cluster::controller_stm*,
      rpc::connection_cache*,
      features::feature_table*,
      ss::abort_source*);

    using notification_id = table::notification_id;
    using notification_callback = table::notification_callback;

    ss::future<errc> upsert_cluster_link(
      ::cluster_link::model::metadata, model::timeout_clock::time_point);
    ss::future<errc> remove_cluster_link(
      ::cluster_link::model::name_t,
      bool force,
      model::timeout_clock::time_point);
    ss::future<errc> add_mirror_topic(
      ::cluster_link::model::id_t,
      ::cluster_link::model::add_mirror_topic_cmd,
      model::timeout_clock::time_point);
    ss::future<errc> delete_mirror_topic(
      ::cluster_link::model::id_t,
      ::cluster_link::model::delete_mirror_topic_cmd,
      model::timeout_clock::time_point);
    ss::future<errc> update_mirror_topic_status(
      ::cluster_link::model::id_t,
      ::cluster_link::model::update_mirror_topic_status_cmd,
      model::timeout_clock::time_point);
    ss::future<errc> update_mirror_topic_properties(
      ::cluster_link::model::id_t,
      ::cluster_link::model::update_mirror_topic_properties_cmd,
      model::timeout_clock::time_point);
    ss::future<errc> update_cluster_link_configuration(
      ::cluster_link::model::id_t,
      ::cluster_link::model::update_cluster_link_configuration_cmd,
      model::timeout_clock::time_point);

    ss::future<errc> failover_link_topics(
      ::cluster_link::model::id_t, model::timeout_clock::duration);

    bool cluster_link_active() const;

    bool cluster_linking_enabled() const;

    notification_id register_for_updates(notification_callback);
    void unregister_for_updates(notification_id);

    ::cluster_link::model::metadata_ptr
    find_link_by_id(::cluster_link::model::id_t id) const;

    ::cluster_link::model::metadata_ptr
    find_link_by_name(const ::cluster_link::model::name_t& name) const;

    std::optional<::cluster_link::model::id_t>
    find_link_id_by_name(const ::cluster_link::model::name_t& name) const;

    std::optional<::cluster_link::model::id_t>
    find_link_id_by_topic(model::topic_view topic) const;

    chunked_vector<::cluster_link::model::id_t> get_all_link_ids() const;

    std::optional<chunked_hash_map<
      ::model::topic,
      ::cluster_link::model::mirror_topic_metadata>>
    get_mirror_topics_for_link(::cluster_link::model::id_t id) const;

    std::optional<::model::revision_id>
    get_last_update_revision(const ::cluster_link::model::id_t& id) const;

    /**
     * Topics that are part of an active shadow link cannot be modified
     * through the Kafka API unless the topics are in a failed over state.
     */
    bool is_topic_mutable_for_kafka_api(const model::topic&) const;

    /**
     * @brief Checks if a topic is mirrored in any link and it is covered by an
     * autocreate filter
     */
    bool is_autocreate_mirror_topic(const model::topic& topic) const;

    /**
     * @brief Utility function to simplify deleting a mirrored topic.
     *
     * This is a wrapper around the delete_mirror_topic command that retrieves
     * the link id and forwards the call.
     *
     */
    ss::future<topic_result>
      delete_mirror_topic(model::topic, model::timeout_clock::time_point);

    /**
     * @brief Utility function to simplify deleting multiple mirrored topics at
     * once.
     */
    ss::future<chunked_vector<topic_result>> delete_mirror_topics(
      chunked_vector<model::topic>, model::timeout_clock::time_point);

    /**
     * @brief Returns true if schema registry topic is being shadowed
     */
    bool schema_registry_shadowing_active() const;

private:
    ss::future<errc>
      do_mutation(cluster_link_cmd, model::timeout_clock::time_point);
    ss::future<errc> dispatch_mutation_to_remote(
      model::node_id, cluster_link_cmd, model::timeout_clock::duration);
    ss::future<errc>
      do_local_mutation(cluster_link_cmd, model::timeout_clock::time_point);

    cluster::cluster_link::errc
    validate_mutation(const cluster_link_cmd&) const;

    bool is_sanctioned();

public:
    /// Class used to validate the incoming mutation request
    /// Made public for testing purposes
    class validator {
    public:
        explicit validator(
          table*,
          size_t max_links,
          absl::flat_hash_set<std::string_view> excluded_topic_properties);

        cluster::cluster_link::errc
        validate_mutation(const cluster_link_cmd&) const;

    private:
        cluster::cluster_link::errc validate_connection_config(
          const ::cluster_link::model::connection_config& config) const;
        cluster::cluster_link::errc validate_metadata_mirroring_config(
          const ::cluster_link::model::topic_metadata_mirroring_config& config)
          const;

    private:
        table* _table;
        size_t _max_links;
        absl::flat_hash_set<std::string_view> _excluded_topic_properties;
    };

private:
    model::node_id _self;
    cluster::partition_leaders_table* _leaders;
    rpc::connection_cache* _connections;
    table* _table;
    ss::abort_source* _as;
    cluster::controller_stm* _controller;
    features::feature_table* _features;

    ssx::mutex _mu{"panda-link::frontend::mu"};
};
} // namespace cluster::cluster_link
