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

#include "cluster/logger.h"
#include "model/timeout_clock.h"

#include <seastar/util/later.hh>

namespace cluster {

health_monitor_frontend::health_monitor_frontend(
  ss::sharded<health_monitor_backend>& backend)
  : _backend(backend) {}

ss::future<> health_monitor_frontend::start() {
    if (ss::this_shard_id() == refresher_shard) {
        _refresh_timer.set_callback([this] { disk_health_tick(); });
        _refresh_timer.arm(disk_health_refresh_interval);
    }
    co_return;
}

ss::future<> health_monitor_frontend::stop() {
    vlog(clusterlog.info, "Stopping Health Monitor Frontend...");
    if (ss::this_shard_id() == refresher_shard) {
        if (_refresh_timer.armed()) {
            _refresh_timer.cancel();
        }
        co_await _refresh_gate.close();
    }
}

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

ss::future<> health_monitor_frontend::maybe_refresh_cloud_health_stats() {
    return dispatch_to_backend([](health_monitor_backend& be) {
        return be.maybe_refresh_cloud_health_stats();
    });
}

storage::disk_space_alert health_monitor_frontend::get_cluster_disk_health() {
    return _cluster_disk_health;
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

ss::future<result<std::optional<cluster::drain_manager::drain_status>>>
health_monitor_frontend::get_node_drain_status(
  model::node_id node_id, model::timeout_clock::time_point deadline) {
    return dispatch_to_backend([node_id, deadline](health_monitor_backend& be) {
        return be.get_node_drain_status(node_id, deadline);
    });
}

ss::future<cluster_health_overview>
health_monitor_frontend::get_cluster_health_overview(
  model::timeout_clock::time_point deadline) {
    return dispatch_to_backend([deadline](health_monitor_backend& be) {
        return be.get_cluster_health_overview(deadline);
    });
}

ss::future<> health_monitor_frontend::update_other_shards(
  const storage::disk_space_alert dsa) {
    co_await container().invoke_on_others(
      [dsa](health_monitor_frontend& fe) { fe._cluster_disk_health = dsa; });
}

ss::future<> health_monitor_frontend::update_frontend_and_backend_cache() {
    auto deadline = model::time_from_now(default_timeout);
    auto disk_health = co_await dispatch_to_backend(
      [deadline](health_monitor_backend& be) {
          return be.get_cluster_disk_health(force_refresh::no, deadline);
      });
    if (disk_health != _cluster_disk_health) {
        vlog(
          clusterlog.debug,
          "Update disk health cache {} -> {}",
          _cluster_disk_health,
          disk_health);
        _cluster_disk_health = disk_health;
        co_await update_other_shards(disk_health);
    }
}

// Handler for refresh_shard's update timer
void health_monitor_frontend::disk_health_tick() {
    if (_refresh_gate.is_closed()) {
        return;
    }
    ssx::spawn_with_gate(_refresh_gate, [this]() {
        // Ensure that this node's cluster health data is not too stale.
        return update_frontend_and_backend_cache()
          .handle_exception([](const std::exception_ptr& e) {
              vlog(
                clusterlog.warn, "failed to update disk health cache: {}", e);
          })
          .finally(
            [this] { _refresh_timer.arm(disk_health_refresh_interval); });
    });
}

ss::future<bool> health_monitor_frontend::does_raft0_have_leader() {
    return dispatch_to_backend(
      [](health_monitor_backend& be) { return be.does_raft0_have_leader(); });
}

ss::future<> health_monitor_frontend::refresh_info() {
    // start() checks that the refresh timer is run on the refresher_shard, so
    // invoke a refresh on that shard
    vlog(clusterlog.info, "Refreshing disk health info");
    co_await container().invoke_on(
      refresher_shard, [](health_monitor_frontend& fe) {
          return ssx::spawn_with_gate_then(fe._refresh_gate, [&fe]() {
              return fe.update_frontend_and_backend_cache();
          });
      });
}

} // namespace cluster
