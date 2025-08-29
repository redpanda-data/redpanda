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
#include "cluster/commands.h"
#include "cluster/controller_stm.h"
#include "cluster/fwd.h"
#include "cluster_link/model/types.h"
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
      cluster::cluster_link_update_mirror_topic_state_cmd,
      cluster::cluster_link_update_mirror_topic_properties_cmd>;

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
      ::cluster_link::model::name_t, model::timeout_clock::time_point);
    ss::future<errc> add_mirror_topic(
      ::cluster_link::model::id_t,
      ::cluster_link::model::add_mirror_topic_cmd,
      model::timeout_clock::time_point);
    ss::future<errc> update_mirror_topic_state(
      ::cluster_link::model::id_t,
      ::cluster_link::model::update_mirror_topic_state_cmd,
      model::timeout_clock::time_point);
    ss::future<errc> update_mirror_topic_properties(
      ::cluster_link::model::id_t,
      ::cluster_link::model::update_mirror_topic_properties_cmd,
      model::timeout_clock::time_point);

    bool cluster_linking_enabled() const;

    notification_id register_for_updates(notification_callback);
    void unregister_for_updates(notification_id);

    std::optional<std::reference_wrapper<const ::cluster_link::model::metadata>>
    find_link_by_id(::cluster_link::model::id_t id) const;

    std::optional<std::reference_wrapper<const ::cluster_link::model::metadata>>
    find_link_by_name(const ::cluster_link::model::name_t& name) const;

    chunked_vector<::cluster_link::model::id_t> get_all_link_ids() const;

    std::optional<chunked_hash_map<
      ::model::topic,
      ::cluster_link::model::mirror_topic_metadata>>
    get_mirror_topics_for_link(::cluster_link::model::id_t id) const;

private:
    ss::future<errc>
      do_mutation(cluster_link_cmd, model::timeout_clock::time_point);
    ss::future<errc> dispatch_mutation_to_remote(
      model::node_id, cluster_link_cmd, model::timeout_clock::duration);
    ss::future<errc>
      do_local_mutation(cluster_link_cmd, model::timeout_clock::time_point);

    cluster::cluster_link::errc
    validate_mutation(const cluster_link_cmd&) const;

public:
    /// Class used to validate the incoming mutation request
    /// Made public for testing purposes
    class validator {
    public:
        explicit validator(
          table*,
          size_t max_links,
          chunked_vector<ss::sstring> excluded_topic_properties);

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
        chunked_vector<ss::sstring> _excluded_topic_properties;
    };

private:
    model::node_id _self;
    cluster::partition_leaders_table* _leaders;
    rpc::connection_cache* _connections;
    table* _table;
    ss::abort_source* _as;
    cluster::controller_stm* _controller;
    [[maybe_unused]] features::feature_table* _features;

    mutex _mu{"panda-link::frontend::mu"};
};
} // namespace cluster::cluster_link
