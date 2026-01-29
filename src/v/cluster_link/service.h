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

#include "base/seastarx.h"
#include "cluster/cluster_link/fwd.h"
#include "cluster/fwd.h"
#include "cluster/utils/partition_change_notifier.h"
#include "cluster_link/errc.h"
#include "cluster_link/fwd.h"
#include "cluster_link/model/types.h"
#include "kafka/data/rpc/fwd.h"
#include "kafka/server/fwd.h"
#include "model/fundamental.h"
#include "rpc/fwd.h"
#include "ssx/mutex.h"
#include "ssx/work_queue.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

namespace cluster_link {
namespace replication {
class data_source;
class data_sink;
class mux_remote_consumer;
} // namespace replication
/**
 * @brief API access for cluster link service
 */
class service : public ss::peering_sharded_service<service> {
public:
    service(
      ::model::node_id self,
      config::binding<bool> enable_shadow_linking,
      ss::sharded<::cluster::cluster_link::frontend>* plf,
      std::unique_ptr<cluster::partition_change_notifier> notifications,
      ss::sharded<cluster::partition_manager>* partition_manager,
      ss::sharded<cluster::partition_leaders_table>* partition_leaders_table,
      ss::sharded<cluster::shard_table>* shard_table,
      ss::sharded<cluster::metadata_cache>* metadata_cache,
      ss::sharded<::rpc::connection_cache>* connections,
      cluster::controller* controller,
      ss::sharded<kafka::group_router>* group_router,
      ss::sharded<kafka::snc_quota_manager>* snc_quota_mgr,
      ss::sharded<cluster::health_monitor_frontend>* hm_frontend,
      ss::sharded<cluster::security_frontend>* security_fe,
      ss::sharded<kafka::data::rpc::client>* kafka_data_rpc_client,
      ss::sharded<cluster::id_allocator_frontend>* id_alloc,
      ss::smp_service_group smp_group,
      ss::scheduling_group scheduling_group);

    service(const service&) = delete;
    service(service&&) = delete;
    service& operator=(const service&) = delete;
    service& operator=(service&&) = delete;
    virtual ~service();

    ss::future<> start();
    ss::future<> stop();
    /**
     * @brief Upserts a cluster link (creation or update)
     *
     * @param md The metadata containing the settings for the new or existing
     * cluster link
     * @return Result containing either the created/updated link or an error
     */
    ss::future<cl_result<model::metadata_ptr>>
    upsert_cluster_link(model::metadata md);
    /**
     * @brief Get the cluster link object
     *
     * @param name The name of the link
     * @return Either the existing link or an error
     */
    cl_result<model::metadata_ptr> get_cluster_link(const model::name_t& name);
    /**
     * @brief Returns a list of existing cluster links
     *
     * @return List of cluster links
     */
    cl_result<chunked_vector<model::metadata_ptr>> list_cluster_links();
    /**
     * @brief Updates the configuration of a cluster link
     *
     * @param name The name of the link
     * @param cmd The command containing the new configuration
     * @return Result containing the updated link
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
      ::model::topic topic,
      model::mirror_topic_status,
      bool force_update = false);

    /**
     * @brief Failover the topics of a cluster link
     *
     * @return Result containing metadata of failed over topics or an error.
     */
    ss::future<cl_result<model::metadata_ptr>>
    failover_link_topics(model::name_t link_name);

    /**
     * @brief Delete the cluster link object
     *
     * @param name The name of the link
     * @return nothing on success or an error
     */
    ss::future<cl_result<void>>
    delete_cluster_link(model::name_t name, bool force_delete_link);

    /**
     * @brief Removes a shadow topic from a shadow link, removing all state but
     * leaving the topic
     *
     * @param link_name Name of the link
     * @param shadow_topic The name of the shadow topic
     * @return Updated metadata information
     */
    ss::future<cl_result<model::metadata_ptr>>
    delete_shadow_topic_from_shadow_link(
      model::name_t link_name, ::model::topic shadow_topic);

    /**
     * @brief Reports the status of a shard-local topic in the given link
     */
    rpc::shadow_topic_report_response
    shard_local_topic_report(const model::id_t&, const ::model::topic&);

    /**
     * @brief Reports the status of a node-local topic in the given link
     * This is the aggregate of reports from all shards.
     */
    ss::future<rpc::shadow_topic_report_response>
      node_local_shadow_topic_report(rpc::shadow_topic_report_request);

    /**
     * @brief Shadow topic report aggregated from all the brokers hosting
     * partition replicas of the topic.
     */
    ss::future<model::report_result_t>
    shadow_topic_report(model::id_t, const ::model::topic&);

    /**
     * @brief Returns a node local shadow link report
     */
    ss::future<rpc::shadow_link_status_report_response>
      node_local_shadow_link_report(rpc::shadow_link_status_report_request);

    rpc::shadow_link_status_report_response
      shard_local_shadow_link_report(model::id_t);

    ss::future<model::status_report_ret_t>
    shadow_link_report(model::name_t name);

private:
    void register_notifications();
    void unregister_notifications();
    errc check_manager_state();
    void handle_enable_shadow_link_change();
    ss::future<> do_handle_enable_shadow_link_change();
    ss::future<> maybe_start_manager();
    ss::future<> maybe_stop_manager();

    template<typename Func, typename Ret = std::invoke_result_t<Func, manager*>>
    requires std::invocable<Func, manager*>
    auto with_manager(Func&& func) -> Ret {
        using return_type = Ret;

        if (auto ec = check_manager_state(); ec != errc::success) {
            if constexpr (ss::is_future<return_type>::value) {
                // Asynchronous case - return a future
                using inner_type = typename return_type::value_type;
                return ss::make_ready_future<inner_type>(err_info{ec});

            } else {
                // Synchronous case - return directly
                return err_info{ec};
            }
        }
        return func(_manager.get());
    }

    ss::future<rpc::shadow_link_status_report_response> shadow_link_report(
      ::model::node_id, rpc::shadow_link_status_report_request);

private:
    /**
     * @brief Reports the status of a shadow topic in the given link
     * on the input node_id.
     */
    ss::future<rpc::shadow_topic_report_response>
      shadow_topic_report(::model::node_id, rpc::shadow_topic_report_request);

    ss::gate _gate;
    // Need explicit namespace due to having a `cluster_link::model` namespace
    ::model::node_id _self;
    config::binding<bool> _enable_shadow_linking;
    ss::sharded<::cluster::cluster_link::frontend>* _plf;
    std::unique_ptr<cluster::partition_change_notifier> _notifications;
    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<cluster::partition_leaders_table>* _partition_leaders_table;
    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<cluster::metadata_cache>* _metadata_cache;
    ss::sharded<::rpc::connection_cache>* _connections;
    cluster::controller* _controller;
    ss::sharded<kafka::group_router>* _group_router;
    ss::sharded<kafka::snc_quota_manager>* _snc_quota_mgr;
    ss::sharded<cluster::health_monitor_frontend>* _hm_frontend;
    ss::sharded<cluster::security_frontend>* _security_fe;
    ss::sharded<kafka::data::rpc::client>* _kafka_data_rpc_client;
    ss::sharded<cluster::id_allocator_frontend>* _id_allocator_frontend;
    ss::smp_service_group _smp_group;
    ss::scheduling_group _scheduling_group;
    std::unique_ptr<manager> _manager;
    std::vector<ss::deferred_action<ss::noncopyable_function<void()>>>
      _notification_cleanups;
    ssx::work_queue _queue;

    ss::abort_source _as;
    ssx::mutex _shadow_link_config_mutex{"shadow_link/config"};
};

std::unique_ptr<replication::data_source> make_default_data_source(
  const ::model::topic_partition& tp,
  replication::mux_remote_consumer& consumer);

std::unique_ptr<replication::data_sink> make_default_data_sink(
  ss::lw_shared_ptr<cluster::partition> partition,
  const cluster::metadata_cache&,
  cluster::id_allocator_frontend&);
} // namespace cluster_link
