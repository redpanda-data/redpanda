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

#include "absl/container/flat_hash_map.h"
#include "cluster_link/deps.h"
#include "cluster_link/link.h"
#include "cluster_link/link_status_reconciler.h"
#include "cluster_link/logger.h"
#include "cluster_link/model/types.h"
#include "cluster_link/task.h"
#include "cluster_link/topic_reconciler.h"
#include "cluster_link/types.h"
#include "container/chunked_vector.h"
#include "kafka/data/rpc/deps.h"
#include "kafka/data/rpc/fwd.h"
#include "model/fundamental.h"
#include "ssx/mutex.h"
#include "ssx/work_queue.h"

namespace cluster_link {

/**
 * @brief Class used to manage cluster links
 *
 * This class will be notified of changes in Redpanda to create, modify, or
 * remove cluster links and to handle leadership changes of partitions.
 */
class manager {
public:
    using notification_id = named_type<uint32_t, struct mgr_notification_tag>;
    using link_cfg_change_notification_cb
      = ss::noncopyable_function<void(model::id_t, model::metadata_ptr)>;
    manager(
      ::model::node_id self,
      std::unique_ptr<kafka::data::rpc::partition_leader_cache>
        partition_leader_cache,
      std::unique_ptr<kafka::data::rpc::partition_manager> partition_manager,
      std::unique_ptr<kafka::data::rpc::topic_metadata_cache>
        topic_metadata_cache,
      std::unique_ptr<kafka::data::rpc::topic_creator> topic_creator,
      std::unique_ptr<security_service> security_service,
      std::unique_ptr<link_registry> registry,
      std::unique_ptr<link_factory> link_factory,
      std::unique_ptr<cluster_factory> cluster_factory,
      std::unique_ptr<consumer_groups_router> group_router,
      std::unique_ptr<partition_metadata_provider> partition_metadata_provider,
      std::unique_ptr<kafka_rpc_client_service> kafka_rpc_client_service,
      std::unique_ptr<members_table_provider> members_table_provider,
      ss::lowres_clock::duration task_reconciler_interval,
      config::binding<int16_t> default_topic_replication,
      ss::scheduling_group scheduling_group);
    manager(const manager&) = delete;
    manager(manager&&) = delete;
    manager& operator=(const manager&) = delete;
    manager& operator=(manager&&) = delete;
    virtual ~manager() = default;

    ss::future<> start();
    ss::future<> stop();

    /**
     * @brief Creates or updates a cluster link
     */
    ss::future<cl_result<model::metadata_ptr>>
    upsert_cluster_link(model::metadata md);
    /**
     * @brief Get the cluster link object by name
     */
    cl_result<model::metadata_ptr> get_cluster_link(const model::name_t& name);
    /**
     * @brief Returns list of cluster links
     */
    cl_result<chunked_vector<model::metadata_ptr>> list_cluster_links();
    /**
     * @brief Updates the configuration of a cluster link
     */
    ss::future<cl_result<model::metadata_ptr>> update_cluster_link(
      model::name_t name, model::update_cluster_link_configuration_cmd cmd);

    /**
     * @brief Update the status of a mirror topic
     *
     * @return Result containing metadata of updated mirror topic or an error.
     */
    ss::future<cl_result<model::metadata_ptr>> update_mirror_topic_status(
      model::name_t link_name,
      ::model::topic,
      model::mirror_topic_status,
      bool force_update = false);

    /**
     * @brief Fails over the topics of a cluster link
     *
     * @return Result containing metadata of failed over topics or an error.
     */
    ss::future<cl_result<model::metadata_ptr>>
    failover_link_topics(model::name_t link_name);

    /**
     * @brief Delete the cluster link object by name
     */
    ss::future<cl_result<void>>
    delete_cluster_link(model::name_t name, bool force_delete_link);

    /**
     * @brief Removes a shadow topic from a shadow link.  Will remove all state
     * of the topic but will not delete the topic
     */
    ss::future<cl_result<model::metadata_ptr>> remove_shadow_topic_from_link(
      model::name_t link_name, ::model::topic shadow_topic);

    /// Used to notify that a cluster link has been updated
    void on_link_change(model::id_t id, ::model::revision_id);
    /// Used to notify manager in a change of NTP leadership
    void handle_partition_state_change(
      ::model::ntp ntp,
      ntp_leader is_ntp_leader,
      std::optional<::model::term_id>);
    /// Handles creation and start of a link
    ss::future<> handle_on_link_change(model::id_t id, ::model::revision_id);
    /// Handles leadership changes for a given NTP
    /// term will be set if partition still exists on the shard
    /// Will definitely be set if is_ntp_leader == true because assuming
    // leadership implies the partition is still present
    ss::future<> handle_on_leadership_change(
      ::model::ntp, ntp_leader, std::optional<::model::term_id>);
    /// Used to add a mirror topic to a cluster link
    ss::future<::cluster::cluster_link::errc>
    add_mirror_topic(model::id_t link_id, model::add_mirror_topic_cmd cmd);
    ss::future<::cluster::cluster_link::errc> update_mirror_topic_state(
      model::id_t link_id, model::update_mirror_topic_status_cmd cmd);
    ss::future<::cluster::cluster_link::errc> update_mirror_topic_properties(
      model::id_t link_id, model::update_mirror_topic_properties_cmd cmd);

    std::optional<
      chunked_hash_map<::model::topic, model::mirror_topic_metadata>>
    get_mirror_topics_for_link(model::id_t id) const;

    /// Registers a task factory that will be used to create tasks when links
    /// are created
    template<typename T, typename... Args>
    ss::future<> register_task_factory(Args&&... args) {
        auto fut = co_await ss::coroutine::as_future(
          _link_task_reconciler_mutex.get_units(_as));
        if (fut.failed()) {
            auto ex = fut.get_exception();
            if (ssx::is_shutdown_exception(ex)) {
                vlog(
                  cllog.info,
                  "Task factory registration skipped due to shutdown");
            } else {
                vlog(cllog.error, "Task factory registration failed: {}", ex);
            }
            co_return;
        }
        _task_factories.emplace_back(
          std::make_unique<T>(std::forward<Args>(args)...));
    }

    model::cluster_link_task_status_report get_task_status_report() const;

    cl_result<model::link_task_status_report>
    get_task_status_report(model::id_t link_id) const;

    kafka::data::rpc::topic_metadata_cache& topic_metadata_cache() noexcept;

    kafka::data::rpc::partition_leader_cache& partition_leader_cache() noexcept;

    security_service& get_security_service() noexcept;

    const kafka::data::rpc::partition_leader_cache&
    partition_leader_cache() const noexcept;

    kafka::data::rpc::partition_manager& partition_manager() noexcept;

    const kafka::data::rpc::partition_manager&
    partition_manager() const noexcept;

    consumer_groups_router& get_group_router() noexcept;

    kafka::data::rpc::topic_creator& topic_creator() noexcept;

    partition_metadata_provider& get_partition_metadata_provider() noexcept;

    kafka_rpc_client_service& get_kafka_rpc_client_service() noexcept;

    ss::scheduling_group scheduling_group() const noexcept {
        return _scheduling_group;
    }

    notification_id
    register_link_config_changes_callback(link_cfg_change_notification_cb cb);
    void unregister_link_config_changes_callback(notification_id cb);

    std::unique_ptr<link_registry>& registry() noexcept { return _registry; }

    cl_result<
      chunked_hash_map<::model::ntp, replication::partition_offsets_report>>
    get_partition_offsets_report_for_link(const model::name_t& name) const;

    cl_result<
      chunked_hash_map<::model::ntp, replication::partition_offsets_report>>
    get_partition_offsets_report_for_link(model::id_t link_id) const;

    members_table_provider& get_members_table_provider() noexcept;

private:
    /// Called periodically to reconcile registered tasks on created links
    ss::future<> link_task_reconciler();
    ss::future<> on_controller_leadership(::model::term_id);
    ss::future<> on_controller_stepdown();
    ss::future<err_info>
    broker_preflight_check(::model::node_id, kafka::client::cluster&);
    ss::future<err_info> link_preflight_checks(const model::metadata& md);

private:
    ::model::node_id _self;
    std::unique_ptr<kafka::data::rpc::partition_leader_cache>
      _partition_leader_cache;
    std::unique_ptr<kafka::data::rpc::partition_manager> _partition_manager;
    std::unique_ptr<kafka::data::rpc::topic_metadata_cache>
      _topic_metadata_cache;
    std::unique_ptr<kafka::data::rpc::topic_creator> _topic_creator;
    std::unique_ptr<security_service> _security_service;
    std::unique_ptr<link_registry> _registry;
    std::unique_ptr<link_factory> _link_factory;
    std::unique_ptr<cluster_factory> _cluster_factory;
    std::unique_ptr<topic_reconciler> _topic_reconciler;
    std::unique_ptr<link_status_reconciler> _link_status_reconciler;
    std::unique_ptr<consumer_groups_router> _group_router;
    std::unique_ptr<partition_metadata_provider> _partition_metadata_provider;
    std::unique_ptr<kafka_rpc_client_service> _kafka_rpc_client_service;
    std::unique_ptr<members_table_provider> _members_table_provider;
    ssx::work_queue _queue;

    chunked_vector<std::unique_ptr<task_factory>> _task_factories;
    absl::flat_hash_map<id_t, std::unique_ptr<link>> _links;
    notification_list<link_cfg_change_notification_cb, notification_id>
      _cfg_change_notifications;

    ss::lowres_clock::duration _task_reconciler_interval;
    ssx::mutex _link_task_reconciler_mutex{
      "cluster_link::manager::link_task_reconciler"};
    ss::timer<ss::lowres_clock> _link_task_reconciler_timer;
    config::binding<int16_t> _default_topic_replication;
    ss::scheduling_group _scheduling_group;
    ss::condition_variable _link_created_cv;
    ss::abort_source _as;
    ss::gate _g;
    ntp_leader _is_controller_leader{ntp_leader::no};
};
} // namespace cluster_link
