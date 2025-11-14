// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/compaction_coordinator.h"

#include "config/configuration.h"
#include "config/property.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "raft/follower_states.h"
#include "raft/fundamental.h"
#include "raft/logger.h"
#include "raft/types.h"
#include "ssx/future-util.h"

using namespace std::chrono_literals;

namespace raft {

compaction_coordinator::compaction_coordinator(
  features::feature_table& features,
  follower_states& fstates,
  ss::shared_ptr<storage::log> log,
  vnode self,
  ctx_log& logger,
  group_id group,
  consensus_client_protocol& client_protocol,
  ss::abort_source& as,
  ss::gate& bg)
  : _log(std::move(log))
  , _logger(logger)
  , _timer{[this] { collect_mcco_from_all_members(); }}
  , _jitter{base_interval()}
  , _retry_interval{retry_interval(base_interval())}
  , _fstates(fstates)
  , _self(self)
  , _group(group)
  , _client_protocol(client_protocol)
  , _raft_as(as)
  , _raft_bg(bg)
  , _tombstone_retention_ms_binding(
      config::shard_local_cfg().tombstone_retention_ms) {
    ssx::spawn_with_gate(bg, [this, &as, &features] {
        return features
          .await_feature(features::feature::coordinated_compaction, as)
          .then([this, &as] {
              arm_timer_if_needed(true);
              _as_sub = ssx::subscribe_or_trigger(
                as, [this] noexcept { cancel_timer(); });
              _tombstone_retention_ms_binding.watch(
                [this] { on_ntp_config_change(); });
              _started = true;
              vlog(_logger.info, "compaction coordinator started");
          });
    });
}

void compaction_coordinator::on_leadership_change(
  std::optional<vnode> new_leader_id) {
    bool new_is_leader = (new_leader_id && new_leader_id.value() == _self);
    if (new_is_leader != _is_leader) {
        _is_leader = new_is_leader;
        if (_is_leader) {
            arm_timer_if_needed(true);
        } else {
            cancel_timer();
        }
    }
}

void compaction_coordinator::on_group_configuration_change() {
    if (_started && _is_leader) {
        recalculate_mtro();
    }
}

get_compaction_mcco_reply compaction_coordinator::do_get_compaction_mcco(
  get_compaction_mcco_request req) {
    if (!_started) {
        return get_compaction_mcco_reply{};
    }
    if (unlikely(req.target_node() != _self)) {
        vlog(
          _logger.warn,
          "received get_compaction_mcco_request addressed to node with "
          "different revision: {}, current node: {}, source: {}",
          req.target_node(),
          _self,
          req.source_node());
        return get_compaction_mcco_reply{};
    }
    update_local_mcco();
    return get_compaction_mcco_reply{
      .mcco = get_local_max_cleanly_compacted_offset()};
}

distribute_compaction_mtro_reply
compaction_coordinator::do_distribute_compaction_mtro(
  distribute_compaction_mtro_request req) {
    if (!_started) {
        return distribute_compaction_mtro_reply{
          .success = distribute_compaction_mtro_reply::is_success::no};
    }
    if (unlikely(req.target_node() != _self)) {
        vlog(
          _logger.warn,
          "received distribute_compaction_mtro_request addressed to node "
          "with different revision: {}, current node: {}, source: {}",
          req.target_node(),
          _self,
          req.source_node());
        return distribute_compaction_mtro_reply{
          .success = distribute_compaction_mtro_reply::is_success::no};
    }

    update_mtro(req.mtro);
    // we may be a follower lagging behind, so our MCCO or even max offset
    // may be below MTRO; fix that: any log below MTRO, even not replicated
    // yet, is cleanly compacted
    on_local_mcco_update(req.mtro);

    return distribute_compaction_mtro_reply{
      .success = distribute_compaction_mtro_reply::is_success::yes};
}

model::offset compaction_coordinator::get_max_tombstone_remove_offset() const {
    return _mtro;
}

model::offset
compaction_coordinator::get_local_max_cleanly_compacted_offset() const {
    return _local_mcco;
}

compaction_coordinator::clock_t::duration
compaction_coordinator::base_interval() const {
    auto retention = _log->config().tombstone_retention_ms();
    // Typically retention is 24h, set update interval to 1h
    // (up to 1.5h due to jitter).
    // Using half-max for disabled to avoid overflow in jitter.
    clock_t::duration base_interval = retention ? clock_t::duration(
                                                    retention.value() / 24)
                                                : clock_t::duration::max() / 2;
    vlog(_logger.debug, "calculated base_interval as {}", base_interval);
    return base_interval;
}

compaction_coordinator::clock_t::duration
compaction_coordinator::retry_interval(
  compaction_coordinator::clock_t::duration base) {
    // typically coordinator_base_interval is ~1h, retry RPCs every 6min
    return base / 10;
}

void compaction_coordinator::on_ntp_config_change() {
    auto base = base_interval();
    _jitter = jitter_t{base};
    _retry_interval = retry_interval(base);
    // rearm
    if (_timer.armed()) {
        cancel_timer();
        arm_timer_if_needed(true);
    }
}

void compaction_coordinator::update_local_mcco() {
    on_local_mcco_update(_log->cleanly_compacted_prefix_offset());
}

void compaction_coordinator::collect_mcco_from_all_members() {
    if (!_is_leader || _raft_as.abort_requested() || _raft_bg.is_closed()) {
        return;
    }
    update_local_mcco();
    for (auto& [node_id, fstate] : _fstates) {
        ssx::background = fstate.mcco_getter->submit(
          [this, holder = _raft_bg.hold(), node_id](
            ss::abort_source& op_as) mutable {
              return get_and_process_compaction_mcco(node_id, op_as)
                .finally([holder = std::move(holder)] {});
          });
    }
    arm_timer_if_needed(false);
}

ss::future<> compaction_coordinator::get_and_process_compaction_mcco(
  vnode node_id, ss::abort_source& op_as) {
    auto maybe_mcco = co_await repeat(
      [this, node_id]() mutable { return get_compaction_mcco(node_id); },
      op_as);
    if (!maybe_mcco) {
        co_return;
    }
    vlog(
      _logger.debug,
      "received max cleanly compacted offset from node {}: {}",
      node_id,
      *maybe_mcco);
    if (op_as.abort_requested()) {
        co_return;
    }
    auto fs_it = _fstates.find(node_id);
    if (fs_it == _fstates.end()) {
        vlog(
          _logger.warn,
          "received max cleanly compacted offset from unknown node {}: {}, "
          "ignoring",
          node_id,
          *maybe_mcco);
        co_return;
    }
    fs_it->second.max_cleanly_compacted_offset = maybe_mcco.value();
    if (_is_leader) [[likely]] {
        recalculate_mtro();
    }
}

ss::future<std::optional<model::offset>>
compaction_coordinator::get_compaction_mcco(vnode node_id) {
    auto rpc_future = co_await ss::coroutine::as_future(
      _client_protocol.get_compaction_mcco(
        node_id.id(),
        get_compaction_mcco_request{
          .node_id = _self, .target_node_id = node_id, .group = _group},
        rpc::client_opts{timeout}));
    if (rpc_future.failed()) {
        vlog(
          _logger.debug,
          "failed to get max cleanly compacted offset from node {}: {}",
          node_id,
          rpc_future.get_exception());
        co_return std::nullopt;
    }
    auto reply = rpc_future.get();
    if (!reply || !reply.value().mcco) {
        vlog(
          _logger.debug,
          "failed to get max cleanly compacted offset from node {}: {}",
          node_id,
          reply);
        co_return std::nullopt;
    }
    co_return reply.value().mcco;
}

void compaction_coordinator::on_local_mcco_update(model::offset new_mcco) {
    vlog(
      _logger.debug,
      "updating local max cleanly compacted offset from {} to {}",
      _local_mcco,
      new_mcco);
    if (new_mcco == _local_mcco) {
        if (_need_force_update) {
            _need_force_update = false;
        } else {
            return;
        }
    }
    if (new_mcco < _local_mcco) {
        // This may happen when a cleanly compacted segment is merged with the
        // following one which is not cleanly compacted. Log only tracks clean
        // compaction per segment, but compaction coordinator will retain
        // earlier info about it gathered when segment were more granular.
        vlog(
          _logger.debug,
          "attempted to move local max cleanly compacted offset backwards from "
          "{} to {}",
          _local_mcco,
          new_mcco);
        return;
    }
    _local_mcco = new_mcco;

    if (_is_leader) {
        recalculate_mtro();
    }
}

void compaction_coordinator::send_mtro_to_followers() {
    for (const auto& [node_id, fstate] : _fstates) {
        ssx::background = fstate.mtro_sender->submit(
          [this, holder = _raft_bg.hold(), node_id](
            ss::abort_source& op_as) mutable -> ss::future<> {
              // MTRO may get recalculated a few times triggered by MCCO
              // arriving from multiple nodes. However, we cannot wait for all
              // MCCOs to arrive as some of them may come very late e.g. due to
              // a node outage. Sleep to avoid flooding the follower with RPCs.
              // Earlier runs will be superseded by the later ones during by
              // executor, so typically only the last RPC will be sent.
              return ss::sleep_abortable(mtro_send_delay, op_as)
                .then([this, node_id, &op_as]() {
                    return repeat(
                      [this, node_id]() {
                          return send_mtro_to_follower(node_id);
                      },
                      op_as);
                })
                .finally([holder = std::move(holder)] {})
                .discard_result();
          });
    }
}

ss::future<ss::stop_iteration>
compaction_coordinator::send_mtro_to_follower(vnode node_id) {
    vlog(
      _logger.trace,
      "sending max tombstone remove offset to node {}: {}",
      node_id,
      _mtro);
    auto rpc_future = co_await ss::coroutine::as_future(
      _client_protocol.distribute_compaction_mtro(
        node_id.id(),
        distribute_compaction_mtro_request{
          .node_id = _self,
          .target_node_id = node_id,
          .group = _group,
          .mtro = _mtro},
        rpc::client_opts{timeout}));
    if (rpc_future.failed()) {
        vlog(
          _logger.debug,
          "failed to send max tombstone remove offset to node {}: {}",
          node_id,
          rpc_future.get_exception());
        co_return ss::stop_iteration::no;
    }
    auto reply = rpc_future.get();
    if (!reply || !reply.value().success) {
        vlog(
          _logger.debug,
          "failed to send max tombstone remove offset to node {}: {}",
          node_id,
          reply);
        co_return ss::stop_iteration::no;
    }
    vlog(
      _logger.trace,
      "successfully sent max tombstone remove offset to node {}: {}",
      node_id,
      reply);
    co_return ss::stop_iteration::yes;
}

void compaction_coordinator::recalculate_mtro() {
    vassert(
      _is_leader, "only leader can recalculate max tombstone remove offset");
    model::offset new_mtro = _local_mcco;
    for (const auto& [node_id, fstate] : _fstates) {
        if (fstate.max_cleanly_compacted_offset == model::offset{}) {
            vlog(
              _logger.debug,
              "vnode: {} hasn't reported its max cleanly compacted offset, "
              "cannot calculate max tombstone remove offset",
              node_id);
            return;
        }
        new_mtro = std::min(new_mtro, fstate.max_cleanly_compacted_offset);
        vlog(
          _logger.trace,
          "on vnode: {} max cleanly compacted offset: {}, new max tombstone "
          "remove offset: {}",
          node_id,
          fstate.max_cleanly_compacted_offset,
          new_mtro);
    }
    update_mtro(new_mtro);
}

void compaction_coordinator::update_mtro(model::offset new_mtro) {
    vlog(
      _logger.debug,
      "updating max tombstone remove offset from {} to {}",
      _mtro,
      new_mtro);
    if (new_mtro == _mtro) {
        if (_need_force_update) {
            _need_force_update = false;
        } else {
            return;
        }
    }
    if (new_mtro == model::offset{}) {
        // uncalculated MTRO, keep previous value as it can never go
        // down
        return;
    }
    if (new_mtro < _mtro) [[unlikely]] {
        // A follower with an empty log has MCCO of 0. Similarly, a follower
        // with a log much shorter than the leader may have MCCO less than the
        // group's MTRO. Low MCCO reported by such followers earlier may
        // contribute to lowering the group's MTRO below existing value.
        //
        // It is intentionally ignored, as semantically MTRO remains the same:
        // when such a follower catches up, its log will be filled with already
        // cleanly compacted data, as, by definition of MTRO, all replicas have
        // their logs cleanly compacted up to it.
        //
        // Later, when the follower receives MTRO from the leader, the follower
        // will update its MCCO to at least match the group's MTRO.
        vlog(
          _logger.debug,
          "attempted to move max tombstone remove offset backwards from {} to "
          "{}",
          _mtro,
          new_mtro);
    }
    if (new_mtro > _mtro) {
        _mtro = new_mtro;
        on_mtro_update();
    }
}

void compaction_coordinator::on_mtro_update() {
    if (_is_leader) {
        send_mtro_to_followers();
    }
    _log->stm_manager()->set_max_tombstone_remove_offset(
      model::prev_offset(_mtro));
}

void compaction_coordinator::cancel_timer() { _timer.cancel(); }

void compaction_coordinator::arm_timer_if_needed(bool jitter_only) {
    if (
      !_started || !_is_leader || _raft_as.abort_requested()
      || _raft_bg.is_closed()) {
        return;
    }
    auto duration = jitter_only ? _jitter.next_duration()
                                : _jitter.next_jitter_duration();
    vlog(_logger.debug, "arming compaction coordinator timer for {}", duration);
    _timer.arm(duration);
}

compaction_coordinator::clock_t::duration
compaction_coordinator::test_accessor::mcco_getting_delay(
  const compaction_coordinator& coco) {
    return coco._jitter.base_duration() + coco._jitter.jitter_duration();
}

compaction_coordinator::clock_t::duration
compaction_coordinator::test_accessor::mtro_distribution_delay() {
    return compaction_coordinator::mtro_send_delay;
}

} // namespace raft
