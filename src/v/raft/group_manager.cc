// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/group_manager.h"

#include "base/likely.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "metrics/prometheus_sanitize.h"
#include "model/metadata.h"
#include "raft/buffered_protocol.h"
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
  ss::sharded<coordinated_recovery_throttle>& recovery_throttle,
  ss::sharded<features::feature_table>& feature_table)
  : _self(self)
  , _raft_sg(raft_sg)
  , _configuration(cfg())
  , _buffered_protocol(ss::make_shared<buffered_protocol>(
      make_rpc_client_protocol(self, clients),
      _configuration.max_inflight_requests_per_node,
      _configuration.max_buffered_bytes_per_node))
  , _heartbeats(
      _configuration.heartbeat_interval,
      consensus_client_protocol(_buffered_protocol),
      _self,
      _configuration.heartbeat_timeout,
      _configuration.enable_lw_heartbeat,
      feature_table.local())
  , _storage(storage.local())
  , _recovery_throttle(recovery_throttle.local())
  , _recovery_mem_quota(std::move(recovery_mem_cfg))
  , _recovery_scheduler(
      _configuration.recovery_concurrency_per_shard,
      _configuration.heartbeat_interval)
  , _feature_table(feature_table.local())
  // we use a reasonable default not to bloat the configuration properties
  , _metric_collection_interval(5s)
  , _metrics_timer([this] {
      try {
          collect_learner_metrics();
      } catch (...) {
          vlog(
            raftlog.error,
            "failed to collect learner metrics - {}",
            std::current_exception());
      }
  })
  , _is_ready(false) {
    _configuration.write_caching.watch(
      [this]() { trigger_config_update_notification(); });
    _configuration.write_caching_flush_ms.watch(
      [this]() { trigger_config_update_notification(); });
    _configuration.write_caching_flush_bytes.watch(
      [this]() { trigger_config_update_notification(); });
    setup_metrics();
}

ss::future<> group_manager::start() {
    co_await _heartbeats.start();
    co_await _recovery_scheduler.start();
    _metrics_timer.arm_periodic(_metric_collection_interval);
}

ss::future<> group_manager::stop() {
    _metrics.clear();
    _public_metrics.clear();
    _metrics_timer.cancel();
    auto f = _gate.close();

    f = f.then([this] { return _recovery_scheduler.stop(); });

    if (!_heartbeats.is_stopped()) {
        // In normal redpanda process shutdown, heartbeats would
        // have been stopped earlier.  Do it here if that didn't happen,
        // e.g. in a unit test.
        f = f.then([this] { return _heartbeats.stop(); });
    }

    return f
      .then([this] {
          return ss::parallel_for_each(
            _groups, [](ss::lw_shared_ptr<consensus> raft) {
                return raft->stop().discard_result();
            });
      })
      .then([this] { return _buffered_protocol->stop(); });
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
  const std::vector<raft::vnode>& nodes,
  ss::shared_ptr<storage::log> log,
  with_learner_recovery_throttle enable_learner_recovery_throttle,
  keep_snapshotted_log keep_snapshotted_log) {
    auto revision = log->config().get_revision();

    auto raft = ss::make_lw_shared<raft::consensus>(
      _self,
      id,
      raft::group_configuration(nodes, revision),
      raft::timeout_jitter(_configuration.election_timeout_ms),
      log,
      scheduling_config(_raft_sg, raft_priority()),
      _configuration.raft_io_timeout_ms,
      _configuration.enable_longest_log_detection,
      consensus_client_protocol(_buffered_protocol),
      [this](raft::leadership_status st) {
          trigger_leadership_notification(std::move(st));
      },
      _storage,
      enable_learner_recovery_throttle
        ? std::make_optional<
            std::reference_wrapper<coordinated_recovery_throttle>>(
            _recovery_throttle)
        : std::nullopt,
      _recovery_mem_quota,
      _recovery_scheduler,
      _feature_table,
      _is_ready ? std::nullopt : std::make_optional(min_voter_priority),
      keep_snapshotted_log);

    return ss::with_gate(_gate, [this, raft] {
        return _heartbeats.register_group(raft).then([this, raft] {
            if (_is_ready) {
                // Check _is_ready flag again to guard against the case when
                // set_ready() was called after we created this consensus
                // instance but before we insert it into the _groups
                // collection.
                raft->reset_node_priority();
            }
            _groups.push_back(raft);
            return raft;
        });
    });
}

ss::future<> group_manager::remove(ss::lw_shared_ptr<raft::consensus> c) {
    return do_shutdown(std::move(c), true).discard_result();
}

ss::future<xshard_transfer_state>
group_manager::shutdown(ss::lw_shared_ptr<raft::consensus> c) {
    return do_shutdown(std::move(c), false);
}

ss::future<xshard_transfer_state> group_manager::do_shutdown(
  ss::lw_shared_ptr<raft::consensus> c, bool remove_persistent_state) {
    const auto group_id = c->group();
    auto transfer_state = co_await c->stop();
    if (remove_persistent_state) {
        co_await c->remove_persistent_state();
    }
    co_await _heartbeats.deregister_group(group_id);
    auto it = std::find(_groups.begin(), _groups.end(), c);
    vassert(
      it != _groups.end(),
      "A consensus instance with group id: {} that is requested to be removed "
      "must be managed by the manager",
      group_id);
    _groups.erase(it);
    co_return transfer_state;
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
         sm::description("Number of raft groups")),
       sm::make_gauge(
         "learners_gap_bytes",
         [this] { return _learners_gap_bytes; },
         sm::description(
           "Total numbers of bytes that must be delivered to learners"))});

    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("raft"),
      {sm::make_gauge(
        "learners_gap_bytes",
        [this] { return _learners_gap_bytes; },
        sm::description(
          "Total numbers of bytes that must be delivered to learners"))});
}

void group_manager::trigger_config_update_notification() {
    if (_gate.is_closed()) {
        return;
    }
    for (auto& group : _groups) {
        group->notify_config_update();
    }
}

void group_manager::collect_learner_metrics() {
    // we can use a synchronous loop here as the number of raft groups per core
    // is limited.
    _learners_gap_bytes = 0;
    for (const auto& group : _groups) {
        _learners_gap_bytes += group->bytes_to_deliver_to_learners();
    }
}

} // namespace raft
