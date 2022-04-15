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
#include "cluster/health_monitor_frontend.h"

#include "cluster/errc.h"

namespace cluster {

health_monitor_frontend::health_monitor_frontend(
  ss::sharded<health_monitor_backend>& backend)
  : _backend(backend) {}

ss::future<result<cluster_health_report>>
health_monitor_frontend::get_cluster_health(
  cluster_report_filter f,
  force_refresh force_refresh,
  model::timeout_clock::time_point deadline) {
    return dispatch_to_backend(
      [f = std::move(f), force_refresh, deadline](health_monitor_backend& be) {
          return be.get_cluster_health(f, force_refresh, deadline);
      });
}

ss::future<cluster_health_report>
health_monitor_frontend::get_current_cluster_health_snapshot(
  cluster_report_filter f) {
    return dispatch_to_backend([f = std::move(f)](health_monitor_backend& be) {
        return be.get_current_cluster_health_snapshot(f);
    });
}

// Collcts and returns current node health report according to provided
// filters list
ss::future<result<node_health_report>>
health_monitor_frontend::collect_node_health(node_report_filter f) {
    return dispatch_to_backend(
      [f = std::move(f)](health_monitor_backend& be) mutable {
          return be.collect_current_node_health(std::move(f));
      });
}

// Return status of single node
ss::future<result<std::vector<node_state>>>
health_monitor_frontend::get_nodes_status(
  model::timeout_clock::time_point deadline) {
    return dispatch_to_backend([deadline](health_monitor_backend& be) {
        // build filter
        cluster_report_filter filter{
          .node_report_filter = node_report_filter{
            .include_partitions = include_partitions_info::no,
          }};
        return be.get_cluster_health(filter, force_refresh::no, deadline)
          .then([](result<cluster_health_report> res) {
              using ret_t = result<std::vector<node_state>>;
              if (!res) {
                  return ret_t(res.error());
              }

              return ret_t(std::move(res.value().node_states));
          });
    });
}

ss::future<cluster_health_overview>
health_monitor_frontend::get_cluster_health_overview(
  model::timeout_clock::time_point deadline) {
    return dispatch_to_backend([deadline](health_monitor_backend& be) {
        return be.get_cluster_health_overview(deadline);
    });
}
} // namespace cluster
