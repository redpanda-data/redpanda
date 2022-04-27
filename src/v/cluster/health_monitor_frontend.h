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
#include "cluster/controller_backend.h"
#include "cluster/fwd.h"
#include "cluster/health_monitor_backend.h"
#include "cluster/health_monitor_types.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"

#include <seastar/core/sharded.hh>

#include <utility>

namespace cluster {

/**
 * The health_monitor_frontend is cluster health monitor entry point.
 * It provides interaface that allow caller
 * to query cluster health and request current node state collection proccess.
 * Health monitor frontend is available on every node and dispatches requests to
 * health monitor backend which lives on single shard.
 */
class health_monitor_frontend {
public:
    explicit health_monitor_frontend(ss::sharded<health_monitor_backend>&);
    // Reports cluster health. Cluster health is based on the cluster health
    // state that is cached on current node. If force_refresh flag is set. It
    // will always refresh cluster health metadata
    ss::future<result<cluster_health_report>> get_cluster_health(
      cluster_report_filter, force_refresh, model::timeout_clock::time_point);

    // Returns currently available cluster health report snapshot
    ss::future<cluster_health_report>
      get_current_cluster_health_snapshot(cluster_report_filter);

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

private:
    template<typename Func>
    auto dispatch_to_backend(Func&& f) {
        return _backend.invoke_on(
          health_monitor_backend::shard, std::forward<Func>(f));
    }

    ss::sharded<health_monitor_backend>& _backend;
};
} // namespace cluster
