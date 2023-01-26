/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/node_isolation_watcher.h"

#include "cluster/metadata_cache.h"
#include "config/node_config.h"
#include "ssx/future-util.h"

namespace cluster {

node_isolation_watcher::node_isolation_watcher(
  ss::sharded<metadata_cache>& metadata_cache,
  ss::sharded<health_monitor_frontend>& health_monitor,
  ss::sharded<node_status_table>& node_status_table)
  : _metadata_cache(metadata_cache)
  , _health_monitor(health_monitor)
  , _node_status_table(node_status_table) {}

void node_isolation_watcher::start() { start_isolation_watch_timer(); }

ss::future<> node_isolation_watcher::stop() {
    _isolation_watch_timer.cancel();
    return _gate.close();
}

void node_isolation_watcher::start_isolation_watch_timer() {
    _isolation_watch_timer.set_callback([this] { update_isolation_status(); });
    rearm_isolation_watch_timer();
}

void node_isolation_watcher::rearm_isolation_watch_timer() {
    _isolation_watch_timer.arm(
      model::timeout_clock::now() + _isolation_check_ms);
}

void node_isolation_watcher::update_isolation_status() {
    ssx::spawn_with_gate(
      _gate, [this] { return do_update_isolation_status(); });
}

ss::future<> node_isolation_watcher::do_update_isolation_status() {
    bool isolated_status = co_await is_node_isolated();
    if (isolated_status != _last_is_isolated_status) {
        vlog(
          clusterlog.warn,
          "Change is_isolated status. Is node isolated: {}",
          isolated_status);
        co_await _metadata_cache.invoke_on_all(
          [isolated_status](cluster::metadata_cache& mc) {
              mc.set_is_node_isolated_status(isolated_status);
          });
        _last_is_isolated_status = isolated_status;
    }
    rearm_isolation_watch_timer();
}

ss::future<bool> node_isolation_watcher::is_node_isolated() {
    if (!_node_status_table.local().is_isolated()) {
        co_return false;
    }

    bool does_raft0_have_leader
      = co_await _health_monitor.local().does_raft0_have_leader();
    if (does_raft0_have_leader) {
        co_return false;
    }

    auto nodes_status_res = co_await _health_monitor.local().get_nodes_status(
      config::shard_local_cfg().metadata_status_wait_timeout_ms()
      + model::timeout_clock::now());

    if (nodes_status_res.has_value()) {
        auto& nodes_status = nodes_status_res.value();

        // All nodes from health report should be not alive for isolation
        bool is_one_of_node_not_alive = std::any_of(
          nodes_status.begin(),
          nodes_status.end(),
          [](const node_state& node_state) {
              // Current node should not influence on answer.
              if (config::node().node_id() == node_state.id) {
                  return cluster::alive::no;
              }
              return node_state.is_alive;
          });

        if (is_one_of_node_not_alive) {
            co_return false;
        }
    }

    co_return true;
}

} // namespace cluster