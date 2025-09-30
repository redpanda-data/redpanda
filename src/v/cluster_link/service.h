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
#include "kafka/server/fwd.h"
#include "model/fundamental.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

namespace cluster_link {
/**
 * @brief API access for cluster link service
 */
class service {
public:
    service(
      ::model::node_id self,
      ss::sharded<::cluster::cluster_link::frontend>* plf,
      std::unique_ptr<cluster::partition_change_notifier> notifications,
      ss::sharded<cluster::partition_manager>* partition_manager,
      ss::sharded<cluster::partition_leaders_table>* partition_leaders_table,
      ss::sharded<cluster::shard_table>* shard_table,
      ss::sharded<cluster::metadata_cache>* metadata_cache,
      cluster::controller* controller,
      ss::sharded<kafka::group_router>* group_router,
      ss::sharded<cluster::health_monitor_frontend>* hm_frontend,
      ss::sharded<cluster::security_frontend>* security_fe,
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
    ss::future<result<model::metadata>> upsert_cluster_link(model::metadata md);
    /**
     * @brief Get the cluster link object
     *
     * @param name The name of the link
     * @return Either the existing link or an error
     */
    result<model::metadata> get_cluster_link(const model::name_t& name);
    /**
     * @brief Returns a list of existing cluster links
     *
     * @return List of cluster links
     */
    result<chunked_vector<model::metadata>> list_cluster_links();
    /**
     * @brief Updates the configuration of a cluster link
     *
     * @param name The name of the link
     * @param cmd The command containing the new configuration
     * @return Result containing the updated link
     */
    ss::future<result<model::metadata>> update_cluster_link(
      model::name_t name, model::update_cluster_link_configuration_cmd cmd);
    /**
     * @brief Delete the cluster link object
     *
     * @param name The name of the link
     * @return nothing on success or an error
     */
    ss::future<result<void>> delete_cluster_link(const model::name_t& name);

private:
    void register_notifications();
    void unregister_notifications();

private:
    ss::gate _gate;
    // Need explicit namespace due to having a `cluster_link::model` namespace
    ::model::node_id _self;
    ss::sharded<::cluster::cluster_link::frontend>* _plf;
    std::unique_ptr<cluster::partition_change_notifier> _notifications;
    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<cluster::partition_leaders_table>* _partition_leaders_table;
    ss::sharded<cluster::shard_table>* _shard_table;
    ss::sharded<cluster::metadata_cache>* _metadata_cache;
    cluster::controller* _controller;
    ss::sharded<kafka::group_router>* _group_router;
    ss::sharded<cluster::health_monitor_frontend>* _hm_frontend;
    ss::sharded<cluster::security_frontend>* _security_fe;
    ss::smp_service_group _smp_group;
    ss::scheduling_group _scheduling_group;
    std::unique_ptr<manager> _manager;
    std::vector<ss::deferred_action<ss::noncopyable_function<void()>>>
      _notification_cleanups;
};
} // namespace cluster_link
