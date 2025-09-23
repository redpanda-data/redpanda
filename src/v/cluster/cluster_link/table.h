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

#include "cluster/cluster_link/errc.h"
#include "cluster/controller_snapshot.h"
#include "cluster_link/model/types.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/sharded.hh>

namespace cluster::cluster_link {
/**
 * @brief Table that holds information about cluster links
 */
class table : public ss::peering_sharded_service<table> {
public:
    using map_t = chunked_hash_map<
      ::cluster_link::model::id_t,
      ::cluster_link::model::metadata>;
    table() = default;
    table(const table&) = delete;
    table(table&&) = delete;
    table& operator=(const table&) = delete;
    table& operator=(table&&) = delete;
    ~table() = default;

    using notification_id
      = named_type<size_t, struct cluster_link_notification_tag>;
    using notification_callback
      = ss::noncopyable_function<void(::cluster_link::model::id_t)>;

    /// Number of links in the table
    size_t size() const;

    /// Finds link by name
    std::optional<std::reference_wrapper<const ::cluster_link::model::metadata>>
    find_link_by_name(const ::cluster_link::model::name_t& name) const;
    /// Finds link by id
    std::optional<std::reference_wrapper<const ::cluster_link::model::metadata>>
    find_link_by_id(::cluster_link::model::id_t id) const;
    /// Finds link ID by name
    std::optional<::cluster_link::model::id_t>
    find_id_by_name(const ::cluster_link::model::name_t& name) const;
    /// Finds a link ID by the mirror topic name
    std::optional<::cluster_link::model::id_t>
    find_id_by_topic(model::topic_view tp) const;
    /// Find the state of a mirror topic by its name, otherwise returns
    /// std::nullopt
    std::optional<::cluster_link::model::mirror_topic_state>
    find_mirror_topic_state(model::topic_view tp) const;

    /// Returns a list of all link IDs in the table
    chunked_vector<::cluster_link::model::id_t> get_all_link_ids() const;

    bool is_batch_applicable(const model::record_batch&) const;
    ss::future<std::error_code> apply_update(model::record_batch);

    ss::future<> fill_snapshot(cluster::controller_snapshot&) const;
    ss::future<>
    apply_snapshot(model::offset, const cluster::controller_snapshot&);

    notification_id register_for_updates(notification_callback);
    void unregister_for_updates(notification_id);

    /// Returns whether or not there is at least one cluster link present and
    /// active
    bool cluster_link_active() const;

private:
    /// Snapshot copy of all the cluster links
    map_t all_links() const;
    /// Restores a cluster link table from a snapshot
    void reset_links(map_t);

    /// Upserts a link, if the ID classes, throws a std::logic_error
    cluster::cluster_link::errc
      upsert_link(::cluster_link::model::id_t, ::cluster_link::model::metadata);
    /// Removes a link by ID
    cluster::cluster_link::errc
    remove_link(const ::cluster_link::model::name_t&);

    cluster::cluster_link::errc add_mirror_topic(
      ::cluster_link::model::id_t,
      const ::cluster_link::model::add_mirror_topic_cmd& cmd);

    cluster::cluster_link::errc update_mirror_topic_state(
      ::cluster_link::model::id_t,
      const ::cluster_link::model::update_mirror_topic_state_cmd& cmd);

    cluster::cluster_link::errc update_mirror_topic_properties(
      ::cluster_link::model::id_t,
      const ::cluster_link::model::update_mirror_topic_properties_cmd&);

    cluster::cluster_link::errc update_cluster_link_configuration(
      ::cluster_link::model::id_t,
      const ::cluster_link::model::update_cluster_link_configuration_cmd&);

    void run_callbacks(::cluster_link::model::id_t);

private:
    using name_index_t = chunked_hash_map<
      ::cluster_link::model::name_t,
      ::cluster_link::model::id_t>;

    using topic_name_index_t
      = chunked_hash_map<model::topic, ::cluster_link::model::id_t>;

    map_t _link_metadata;
    name_index_t _name_index;
    topic_name_index_t _topic_name_index;

    chunked_hash_map<notification_id, notification_callback> _callbacks;
    notification_id _latest_id{0};
};
} // namespace cluster::cluster_link
