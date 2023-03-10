// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/group_manager.h"

#include "config/configuration.h"
#include "features/feature_table.h"
#include "likely.h"
#include "model/metadata.h"
#include "prometheus/prometheus_sanitize.h"
#include "raft/group_configuration.h"
#include "raft/rpc_client_protocol.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/scheduling.hh>

#include <optional>

namespace raft {

group_manager::group_manager(
  model::node_id self,
  ss::scheduling_group raft_sg,
  group_manager::config_provider_fn cfg,
  recovery_memory_quota::config_provider_fn recovery_mem_cfg,
  ss::sharded<rpc::connection_cache>& clients,
  ss::sharded<storage::api>& storage,
  ss::sharded<recovery_throttle>& recovery_throttle,
  ss::sharded<features::feature_table>& feature_table)
  : _self(self)
  , _raft_sg(raft_sg)
  , _client(make_rpc_client_protocol(self, clients))
  , _configuration(cfg())
  , _heartbeats(
      _configuration.heartbeat_interval,
      _client,
      _self,
      _configuration.heartbeat_timeout)
  , _storage(storage.local())
  , _recovery_throttle(recovery_throttle.local())
  , _recovery_mem_quota(std::move(recovery_mem_cfg))
  , _feature_table(feature_table.local())
  , _is_ready(false) {
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
void group_manager::set_ready() {
    _is_ready = true;
    std::for_each(
      _groups.begin(), _groups.end(), [](ss::lw_shared_ptr<consensus>& c) {
          c->reset_node_priority();
      });
}

ss::future<> group_manager::stop_heartbeats() { return _heartbeats.stop(); }

ss::future<ss::lw_shared_ptr<raft::consensus>> group_manager::create_group(
  raft::group_id id,
  std::vector<model::broker> nodes,
  storage::log log,
  with_learner_recovery_throttle enable_learner_recovery_throttle,
  keep_snapshotted_log keep_snapshotted_log) {
    auto revision = log.config().get_revision();
    auto raft_cfg = create_initial_configuration(std::move(nodes), revision);

    auto raft = ss::make_lw_shared<raft::consensus>(
      _self,
      id,
      std::move(raft_cfg),
      raft::timeout_jitter(
        config::shard_local_cfg().raft_election_timeout_ms()),
      log,
      scheduling_config(_raft_sg, raft_priority()),
      _configuration.raft_io_timeout_ms,
      _client,
      [this](raft::leadership_status st) {
          trigger_leadership_notification(std::move(st));
      },
      _storage,
      enable_learner_recovery_throttle
        ? std::make_optional<std::reference_wrapper<recovery_throttle>>(
          _recovery_throttle)
        : std::nullopt,
      _recovery_mem_quota,
      _feature_table,
      _is_ready ? std::nullopt : std::make_optional(min_voter_priority),
      keep_snapshotted_log);

    return ss::with_gate(_gate, [this, raft] {
        return _heartbeats.register_group(raft).then([this, raft] {
            _groups.push_back(raft);
            return raft;
        });
    });
}

raft::group_configuration group_manager::create_initial_configuration(
  std::vector<model::broker> initial_brokers,
  model::revision_id revision) const {
    /**
     * Decide which raft configuration to use for the partition, if all nodes
     * are able to understand configuration without broker information the
     * configuration will only use raft::vnode
     */
    if (likely(_feature_table.is_active(
          features::feature::membership_change_controller_cmds))) {
        std::vector<vnode> nodes;
        nodes.reserve(initial_brokers.size());
        for (auto& b : initial_brokers) {
            nodes.emplace_back(b.id(), revision);
        }

        return {std::move(nodes), revision};
    }

    // old configuration with brokers
    auto raft_cfg = raft::group_configuration(
      std::move(initial_brokers), revision);
    if (unlikely(!_feature_table.is_active(
          features::feature::raft_improved_configuration))) {
        raft_cfg.set_version(group_configuration::v_3);
    }
    return raft_cfg;
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

    _notifications.notify(st.group, st.term, leader_id);
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
