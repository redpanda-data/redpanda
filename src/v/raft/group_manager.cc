// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/group_manager.h"

#include "config/configuration.h"
#include "model/metadata.h"
#include "prometheus/prometheus_sanitize.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/scheduling.hh>

#include <optional>

namespace raft {

group_manager::group_manager(
  model::node_id self,
  model::timeout_clock::duration disk_timeout,
  ss::scheduling_group raft_sg,
  std::chrono::milliseconds heartbeat_interval,
  std::chrono::milliseconds heartbeat_timeout,
  ss::sharded<rpc::connection_cache>& clients,
  ss::sharded<storage::api>& storage,
  ss::sharded<recovery_throttle>& recovery_throttle)
  : _self(self)
  , _disk_timeout(disk_timeout)
  , _raft_sg(raft_sg)
  , _client(make_rpc_client_protocol(self, clients))
  , _heartbeats(heartbeat_interval, _client, _self, heartbeat_timeout)
  , _storage(storage.local())
  , _recovery_throttle(recovery_throttle.local()) {
    setup_metrics();
}

ss::future<> group_manager::start() { return _heartbeats.start(); }

ss::future<> group_manager::stop() {
    auto f = _gate.close();
    if (!_heartbeats.is_stopped()) {
        // In normal redpanda process shutdown, heartbeats would
        // have been stopped earlier.  Do it here if that didn't happen,
        // e.g. in a unit test.
        f = f.then([this] { return _heartbeats.stop(); });
    }

    return f.then([this] {
        return ss::parallel_for_each(
          _groups,
          [](ss::lw_shared_ptr<consensus> raft) { return raft->stop(); });
    });
}

ss::future<> group_manager::stop_heartbeats() { return _heartbeats.stop(); }

ss::future<ss::lw_shared_ptr<raft::consensus>> group_manager::create_group(
  raft::group_id id, std::vector<model::broker> nodes, storage::log log) {
    auto revision = log.config().get_revision();
    auto raft = ss::make_lw_shared<raft::consensus>(
      _self,
      id,
      raft::group_configuration(std::move(nodes), revision),
      raft::timeout_jitter(
        config::shard_local_cfg().raft_election_timeout_ms()),
      log,
      scheduling_config(_raft_sg, raft_priority()),
      _disk_timeout,
      _client,
      [this](raft::leadership_status st) {
          trigger_leadership_notification(std::move(st));
      },
      _storage,
      _recovery_throttle);

    return ss::with_gate(_gate, [this, raft] {
        return _heartbeats.register_group(raft).then([this, raft] {
            _groups.push_back(raft);
            return raft;
        });
    });
}

ss::future<> group_manager::remove(ss::lw_shared_ptr<raft::consensus> c) {
    return c->stop()
      .then([c] { return c->remove_persistent_state(); })
      .then(
        [this, id = c->group()] { return _heartbeats.deregister_group(id); })
      .finally([this, c] {
          _groups.erase(
            std::remove(_groups.begin(), _groups.end(), c), _groups.end());
      });
}

ss::future<> group_manager::shutdown(ss::lw_shared_ptr<raft::consensus> c) {
    return c->stop()
      .then(
        [this, id = c->group()] { return _heartbeats.deregister_group(id); })
      .finally([this, c] {
          _groups.erase(
            std::remove(_groups.begin(), _groups.end(), c), _groups.end());
      });
}

void group_manager::trigger_leadership_notification(
  raft::leadership_status st) {
    std::optional<model::node_id> leader_id;
    if (st.current_leader) {
        leader_id = st.current_leader->id();
    }

    for (auto& cb : _notifications) {
        cb.second(st.group, st.term, leader_id);
    }
}

void group_manager::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;

    _metrics.add_group(
      prometheus_sanitize::metrics_name("raft"),
      {sm::make_gauge(
        "group_count",
        [this] { return _groups.size(); },
        sm::description("Number of raft groups"))});
}

} // namespace raft
