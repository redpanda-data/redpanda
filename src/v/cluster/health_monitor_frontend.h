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
#include "cluster/health_monitor_backend.h"
#include "cluster/health_monitor_types.h"
#include "controller_stm.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "storage/types.h"

#include <seastar/core/sharded.hh>

#include <utility>

namespace cluster {

/**
 * The health_monitor_frontend is the main cluster health monitor entry point.
 * It provides an interface that allow callers to query cluster health and
 * request current node state collection proccess.
 * Health monitor frontend is available on every node and dispatches requests to
 * health monitor backend which lives on single shard.
 * Most requests are forwarded to the backend shard, except cluster-level disk
 * health, which is kept cached on each core for fast access.
 */
class health_monitor_frontend
  : public seastar::peering_sharded_service<health_monitor_frontend> {
public:
    static constexpr auto default_timeout = std::chrono::seconds(5);
    static constexpr std::chrono::seconds disk_health_refresh_interval{5};
    static constexpr ss::shard_id refresher_shard
      = cluster::controller_stm_shard;

    explicit health_monitor_frontend(ss::sharded<health_monitor_backend>&);

    ss::future<> start();
    ss::future<> stop();
    ss::future<> refresh_info();

    // Reports cluster health. Cluster health is based on the cluster health
    // state that is cached on current node. If force_refresh flag is set. It
    // will always refresh cluster health metadata
    ss::future<result<cluster_health_report>> get_cluster_health(
      cluster_report_filter, force_refresh, model::timeout_clock::time_point);

    storage::disk_space_alert get_cluster_disk_health();

    // Collcts and returns current node health report according to provided
    // filters list
    ss::future<result<node_health_report>>
      collect_node_health(node_report_filter);

    // Return status of all nodes
    ss::future<result<std::vector<node_state>>>
      get_nodes_status(model::timeout_clock::time_point);

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
     * All health metadata is refreshed automatically via the
     * health_monitor_backend on a timer. The cloud storage stats are an
     * exception, this method is for external events to trigger this refresh.
     */
    ss::future<> maybe_refresh_cloud_health_stats();

private:
    template<typename Func>
    auto dispatch_to_backend(Func&& f) {
        return _backend.invoke_on(
          health_monitor_backend::shard, std::forward<Func>(f));
    }

    ss::sharded<health_monitor_backend>& _backend;

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
