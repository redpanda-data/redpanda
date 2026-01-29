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

#include "cluster_link/deps.h"
#include "cluster_link/model/types.h"
#include "cluster_link/replication/deps.h"
#include "cluster_link/replication/link_replication_mgr.h"
#include "cluster_link/task.h"
#include "cluster_link/types.h"
#include "kafka/client/cluster.h"
#include "kafka/data/rpc/deps.h"
#include "ssx/mutex.h"
#include "utils/notification_list.h"

namespace cluster_link {

class link_probe;

/**
 * @brief The link class represents a link between two clusters
 */
class link {
public:
    explicit link(
      ::model::node_id self,
      model::id_t link_id,
      manager* manager,
      ss::lowres_clock::duration task_reconciler_interval,
      model::metadata_ptr config,
      std::unique_ptr<kafka::client::cluster> cluster_connection,
      std::unique_ptr<replication::link_configuration_provider>,
      std::unique_ptr<replication::data_source_factory>,
      std::unique_ptr<replication::data_sink_factory>);
    link(const link&) = delete;
    link(link&&) = delete;
    link& operator=(const link&) = delete;
    link& operator=(link&&) = delete;
    virtual ~link() noexcept;

    virtual ss::future<> start();
    virtual ss::future<> stop() noexcept;

    ss::future<cl_result<void>> register_task(task_factory*);

    void update_config(model::metadata_ptr, ::model::revision_id);

    // at the link scope
    bool requires_active_replicators() const;
    // per topic
    bool requires_active_replicators(const ::model::topic&) const;

    ss::future<> handle_on_leadership_change(
      ::model::ntp ntp,
      ntp_leader is_ntp_leader,
      std::optional<::model::term_id>);

    bool task_is_registered(std::string_view) const noexcept;

    using task_state_notification_id
      = named_type<size_t, struct task_state_notification_id_tag>;
    /// Callback for when a task changes state.  Callback reports the link name,
    /// task name, and the state change information
    using task_state_change_cb = ss::noncopyable_function<void(
      model::name_t, std::string_view, task::state_change)>;

    task_state_notification_id
    register_for_task_state_changes(task_state_change_cb cb);

    void
    unregister_for_task_state_changes(task_state_notification_id id) noexcept;

    model::link_task_status_report get_task_status_report() const;

    ss::future<::cluster::cluster_link::errc>
    add_mirror_topic(model::add_mirror_topic_cmd cmd);

    ss::future<::cluster::cluster_link::errc>
    update_mirror_topic_state(model::update_mirror_topic_status_cmd cmd);

    ss::future<::cluster::cluster_link::errc> update_mirror_topic_properties(
      model::update_mirror_topic_properties_cmd cmd);

    model::metadata_ptr get_config() const noexcept;

    kafka::data::rpc::topic_metadata_cache& topic_metadata_cache() noexcept;

    kafka::data::rpc::partition_leader_cache& partition_leader_cache() noexcept;

    security_service& get_security_service() noexcept;

    const kafka::data::rpc::partition_leader_cache&
    partition_leader_cache() const noexcept;

    kafka::data::rpc::partition_manager& partition_manager() noexcept;

    const kafka::data::rpc::partition_manager&
    partition_manager() const noexcept;

    kafka::data::rpc::topic_creator& topic_creator() noexcept;

    kafka::client::cluster& get_cluster_connection() noexcept;

    consumer_groups_router& get_group_router();

    partition_metadata_provider& get_partition_metadata_provider();

    kafka_rpc_client_service& get_kafka_rpc_client_service();

    std::optional<
      chunked_hash_map<::model::topic, model::mirror_topic_metadata>>
    get_mirror_topics_for_link() const;

    ::model::node_id self() const { return _self; }

    chunked_hash_map<::model::ntp, replication::partition_offsets_report>
    get_partition_offsets_report() const;

    members_table_provider& get_members_table_provider() noexcept;

private:
    bool should_start_task(task* t) const;
    bool should_stop_task(task* t) const;
    bool should_pause_task(task* t) const;
    ss::future<> run_task_reconciler();
    ss::future<cl_result<void>> do_register_task(std::unique_ptr<task>);
    void maybe_update_connection_configuration();
    void handle_new_topics_to_replicate(chunked_vector<::model::topic>);

private:
    ::model::node_id _self;
    model::id_t _link_id;
    manager* _manager;
    chunked_hash_map<ss::sstring, std::unique_ptr<task>> _tasks;
    model::metadata_ptr _config;
    std::unique_ptr<kafka::client::cluster> _cluster_connection;
    replication::link_replication_manager _replication_mgr;

    notification_list<task_state_change_cb, task_state_notification_id>
      _task_state_change_notifications;
    ss::lowres_clock::duration _task_reconciler_interval;
    ssx::mutex _task_reconciler_mutex{"cluster_link::link::task_reconciler"};
    ss::timer<ss::lowres_clock> _task_reconciler;
    std::unique_ptr<link_probe> _probe;
    ss::gate _gate;
    ss::abort_source _as;
};
} // namespace cluster_link
