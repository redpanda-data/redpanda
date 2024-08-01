/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "cluster/fwd.h"
#include "cluster/health_monitor_types.h"
#include "cluster/node_status_table.h"
#include "config/property.h"
#include "controller_stm.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "storage/types.h"

#include <seastar/core/sharded.hh>

namespace cluster {

/**
 * The health_monitor_frontend is the main cluster health monitor entry point.
 * It provides an interface that allow callers to query cluster health and
 * request current node state collection proccess.
 * Health monitor frontend is available on every node and dispatches requests to
 * health monitor backend which lives on single shard.
 * Most requests are forwarded to the backend shard, except cluster-level disk
 * health, which is kept cached on each core for fast access.
 *
 * Health monitor frontend is also an entry point for querying information about
 * the node liveness status.
 */
class health_monitor_frontend
  : public seastar::peering_sharded_service<health_monitor_frontend> {
public:
    static constexpr auto default_timeout = std::chrono::seconds(5);
    static constexpr std::chrono::seconds disk_health_refresh_interval{5};
    static constexpr ss::shard_id refresher_shard
      = cluster::controller_stm_shard;

    explicit health_monitor_frontend(
      ss::sharded<health_monitor_backend>&,
      ss::sharded<node_status_table>&,
      config::binding<std::chrono::milliseconds>);

    ss::future<> start();
    ss::future<> stop();
    ss::future<> refresh_info();

    // Reports cluster health. Cluster health is based on the cluster health
    // state that is cached on current node. If force_refresh flag is set. It
    // will always refresh cluster health metadata
    ss::future<result<cluster_health_report>> get_cluster_health(
      cluster_report_filter, force_refresh, model::timeout_clock::time_point);

    storage::disk_space_alert get_cluster_disk_health();

    // Collects or return cached version of current node health report.
    ss::future<result<node_health_report_ptr>> get_current_node_health();

    /**
     * Return drain status for a given node.
     */
    ss::future<result<std::optional<cluster::drain_manager::drain_status>>>
    get_node_drain_status(
      model::node_id node_id, model::timeout_clock::time_point deadline);

    /**
     *  Return cluster health overview
     *
     *  Health overview is based on the information available in health monitor.
     *  Cluster is considered as healthy when follwing conditions are met:
     *
     * - all nodes that are are responding
     * - all partitions have leaders
     * - cluster controller is present (_raft0 leader)
     */
    ss::future<cluster_health_overview>
      get_cluster_health_overview(model::timeout_clock::time_point);

    ss::future<bool> does_raft0_have_leader();
    /**
     * Method validating if a node is known and alive. It will return an empty
     * optional if the node is not present in node status table.
     */
    std::optional<alive> is_alive(model::node_id) const;

private:
    template<typename Func>
    auto dispatch_to_backend(Func&& f) {
        return _backend.invoke_on(
          health_monitor_backend_shard, std::forward<Func>(f));
    }

    ss::sharded<health_monitor_backend>& _backend;
    ss::sharded<node_status_table>& _node_status_table;
    config::binding<std::chrono::milliseconds> _alive_timeout;

    // Currently the worst / max of all nodes' disk space state
    storage::disk_space_alert _cluster_disk_health{
      storage::disk_space_alert::ok};
    ss::timer<ss::lowres_clock> _refresh_timer;
    ss::gate _refresh_gate;
    void disk_health_tick();
    ss::future<> update_other_shards(const storage::disk_space_alert);
    ss::future<> update_frontend_and_backend_cache();
};
} // namespace cluster
