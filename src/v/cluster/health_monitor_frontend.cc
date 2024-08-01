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

#include "cluster/health_monitor_backend.h"
#include "cluster/logger.h"
#include "config/property.h"
#include "model/timeout_clock.h"

#include <seastar/util/later.hh>

#include <chrono>
#include <ctime>
#include <optional>

namespace cluster {

health_monitor_frontend::health_monitor_frontend(
  ss::sharded<health_monitor_backend>& backend,
  ss::sharded<node_status_table>& node_status_table,
  config::binding<std::chrono::milliseconds> alive_timeout)
  : _backend(backend)
  , _node_status_table(node_status_table)
  , _alive_timeout(std::move(alive_timeout)) {}

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

storage::disk_space_alert health_monitor_frontend::get_cluster_disk_health() {
    return _cluster_disk_health;
}

/**
 * Gets cached or collects a node health report.
 */
ss::future<result<node_health_report_ptr>>
health_monitor_frontend::get_current_node_health() {
    return dispatch_to_backend([](health_monitor_backend& be) mutable {
        return be.get_current_node_health();
    });
}
std::optional<alive>
health_monitor_frontend::is_alive(model::node_id id) const {
    auto status = _node_status_table.local().get_node_status(id);
    if (!status) {
        return std::nullopt;
    }
    return alive(
      status->last_seen + _alive_timeout() >= model::timeout_clock::now());
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
