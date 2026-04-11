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
  , _timer{[this] { collect_all_replica_offsets(); }}
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
              _started = true;
              arm_timer_if_needed(true);
              _as_sub = ssx::subscribe_or_trigger(
                as, [this] noexcept { cancel_timer(); });
              _tombstone_retention_ms_binding.watch(
                [this] { on_ntp_config_change(); });
              vlog(_logger.info, "compaction coordinator started");
          });
    });
}

void compaction_coordinator::on_leadership_change(
  std::optional<vnode> new_leader_id, model::term_id new_term) {
    cancel_timer();
    bool new_is_leader = (new_leader_id && *new_leader_id == _self);
    _need_force_update = new_is_leader
                         && (!_leader_term_id || new_term > *_leader_term_id);
    if (new_is_leader) {
        _leader_term_id = {new_term};
        arm_timer_if_needed(true);
    } else {
        _leader_term_id = std::nullopt;
    }
}

void compaction_coordinator::on_group_configuration_change() {
    if (_started && _leader_term_id) {
        recalculate_group_offsets();
    }
}

get_compaction_mcco_reply compaction_coordinator::do_get_local_replica_offsets(
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
    update_local_replica_offsets();
    return get_compaction_mcco_reply{
      .mcco = get_local_max_cleanly_compacted_offset(),
      .mxfo = get_local_max_transaction_free_offset()};
}

distribute_compaction_mtro_reply
compaction_coordinator::do_distribute_group_offsets(
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

    update_group_offsets(req.mtro, req.mxro);
    // We may be a follower lagging behind, so our MCCO or even max offset
    // may be below MTRO. Fix that: any log below MTRO, even not replicated
    // yet, is cleanly compacted. Same for MXFO/MXRO.
    record_updated_local_replica_offsets(req.mtro, req.mxro);

    return distribute_compaction_mtro_reply{
      .success = distribute_compaction_mtro_reply::is_success::yes};
}

model::offset compaction_coordinator::get_max_tombstone_remove_offset() const {
    return _mtro;
}

model::offset
compaction_coordinator::get_max_transaction_remove_offset() const {
    return _mxro;
}

model::offset
compaction_coordinator::get_local_max_cleanly_compacted_offset() const {
    return _local_mcco;
}

model::offset
compaction_coordinator::get_local_max_transaction_free_offset() const {
    return _local_mxfo;
}

compaction_coordinator::clock_t::duration
compaction_coordinator::base_interval() const {
    auto retention = _log->config().delete_retention_ms();
    // Typically retention is 24h, set update interval to 1h
    // (up to 1.5h due to jitter).
    // Using half-max for disabled to avoid overflow in jitter.
    auto base_interval = retention ? clock_t::duration(*retention / 24)
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

void compaction_coordinator::update_local_replica_offsets() {
    bool updated = record_updated_local_replica_offsets(
      _log->cleanly_compacted_prefix_offset(),
      _log->transaction_free_prefix_offset());
    if (_leader_term_id && updated) {
        recalculate_group_offsets();
    }
}

void compaction_coordinator::collect_all_replica_offsets() {
    if (
      !_leader_term_id || _raft_as.abort_requested() || _raft_bg.is_closed()) {
        return;
    }
    update_local_replica_offsets();
    for (auto& [node_id, fstate] : _fstates) {
        ssx::background = fstate.coordinated_compaction_offsets_getter->submit(
          [this, holder = _raft_bg.hold(), node_id](
            ss::abort_source& op_as) mutable {
              return get_and_process_replica_offsets(node_id, op_as)
                .finally([holder = std::move(holder)] {});
          });
    }
    arm_timer_if_needed(false);
}

bool compaction_coordinator::bump_offset_value(
  model::offset compaction_coordinator::* member,
  model::offset new_value,
  std::string_view var_name) {
    vlog(
      _logger.debug,
      "updating {} from {} to {}",
      var_name,
      this->*member,
      new_value);
    if (new_value == this->*member) {
        return false;
    }
    if (new_value < this->*member) {
        // We ignore attempts to move offsets backwards for the following
        // reasons:
        //
        // A. MCCO/MXFO.
        //   For MCCO this may happen when a cleanly compacted segment is merged
        // with the following one which is not cleanly compacted. Log only
        // tracks clean compaction per segment, but compaction coordinator will
        // retain earlier info about it gathered when segment were more
        // granular.
        //   It also may be a late RPC from leader imposing the group's MTRO as
        // local MCCO. Same for MXFO/MXRO.
        //
        // B. For MTRO/MXRO.
        //   A follower with an empty log may have MCCO=0. Similarly, a follower
        // with a log much shorter than the leader may have MCCO less than the
        // group's MTRO. Low MCCO reported by such followers earlier may
        // contribute to lowering the group's MTRO below existing value.
        //   It is intentionally ignored, as semantically MTRO remains the same:
        // when such a follower catches up, its log will be filled with already
        // cleanly compacted data, as, by definition of MTRO, all replicas have
        // their logs cleanly compacted up to it.
        //   Later, when the follower receives MTRO from the leader, the
        // follower will update its MCCO to at least match the group's MTRO.
        //   Same for MXFO/MXRO.
        vlog(
          _logger.debug,
          "attempted to move local {} backwards from "
          "{} to {}",
          var_name,
          this->*member,
          new_value);
        return false;
    }
    this->*member = new_value;
    return true;
}

ss::future<> compaction_coordinator::get_and_process_replica_offsets(
  vnode node_id, ss::abort_source& op_as) {
    auto maybe_remote_replica_offsets = co_await repeat(
      [this, node_id]() mutable { return get_remote_replica_offsets(node_id); },
      op_as);
    if (!maybe_remote_replica_offsets) {
        co_return;
    }
    vlog(
      _logger.debug,
      "received remote replica offsets from node {}: mcco={}, mxfo={}",
      node_id,
      *maybe_remote_replica_offsets->mcco,
      *maybe_remote_replica_offsets->mxfo);
    if (op_as.abort_requested()) {
        co_return;
    }
    auto fs_it = _fstates.find(node_id);
    if (fs_it == _fstates.end()) {
        vlog(
          _logger.warn,
          "received remote replica offsets from unknown node {}, ignoring",
          node_id);
        co_return;
    }
    fs_it->second.max_cleanly_compacted_offset
      = *maybe_remote_replica_offsets->mcco;
    fs_it->second.max_transaction_free_offset
      = *maybe_remote_replica_offsets->mxfo;
    if (_leader_term_id) [[likely]] {
        recalculate_group_offsets();
    }
}

ss::future<std::optional<get_compaction_mcco_reply>>
compaction_coordinator::get_remote_replica_offsets(vnode node_id) {
    auto rpc_future = co_await ss::coroutine::as_future(
      _client_protocol.get_compaction_mcco(
        node_id.id(),
        get_compaction_mcco_request{
          .node_id = _self, .target_node_id = node_id, .group = _group},
        rpc::client_opts{timeout}));
    if (rpc_future.failed()) {
        auto ex = rpc_future.get_exception();
        vlog(
          _logger.debug,
          "failed to get max cleanly compacted offset from node {}: {}",
          node_id,
          ex);
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
    co_return reply.value();
}

// returns whether recorded offsets were changed
bool compaction_coordinator::record_updated_local_replica_offsets(
  model::offset new_mcco, model::offset new_mxfo) {
    bool mcco_updated = bump_offset_value(
      &compaction_coordinator::_local_mcco,
      new_mcco,
      "local max cleanly compacted offset");
    bool mxfo_updated = bump_offset_value(
      &compaction_coordinator::_local_mxfo,
      new_mxfo,
      "local max transaction free offset");
    return mcco_updated || mxfo_updated;
}

void compaction_coordinator::send_group_offsets_to_followers() {
    vlog(
      _logger.debug,
      "compaction coordinator planning to distribute group offsets in {}",
      group_offsets_send_delay);
    for (const auto& [node_id, fstate] : _fstates) {
        ssx::background = fstate.coordinated_compaction_offsets_sender->submit(
          [this, holder = _raft_bg.hold(), node_id](
            ss::abort_source& op_as) mutable -> ss::future<> {
              // Group offsets may get recalculated a few times triggered by
              // replica offsets arriving from multiple nodes. However, we
              // cannot wait for all replica offsets to arrive, as some of them
              // may come very late e.g. due to a node outage. Sleep to avoid
              // flooding the follower with RPCs. Earlier runs will be
              // superseded by the later ones during by executor, so typically
              // only the last RPC will be sent.
              return ss::sleep_abortable(group_offsets_send_delay, op_as)
                .then([this, node_id, &op_as]() {
                    return repeat(
                      [this, node_id]() {
                          return send_group_offsets_to_follower(node_id);
                      },
                      op_as);
                })
                .finally([holder = std::move(holder)] {})
                .discard_result();
          });
    }
}

ss::future<ss::stop_iteration>
compaction_coordinator::send_group_offsets_to_follower(vnode node_id) {
    vlog(
      _logger.trace,
      "sending group offsets to node {}: mtro={}, mxro={}",
      node_id,
      _mtro,
      _mxro);
    auto rpc_future = co_await ss::coroutine::as_future(
      _client_protocol.distribute_compaction_mtro(
        node_id.id(),
        distribute_compaction_mtro_request{
          .node_id = _self,
          .target_node_id = node_id,
          .group = _group,
          .mtro = _mtro,
          .mxro = _mxro},
        rpc::client_opts{timeout}));
    if (rpc_future.failed()) {
        auto ex = rpc_future.get_exception();
        vlog(
          _logger.debug,
          "failed to send group offsets to node {}: {}",
          node_id,
          ex);
        co_return ss::stop_iteration::no;
    }
    auto reply = rpc_future.get();
    if (!reply || !reply.value().success) {
        vlog(
          _logger.debug,
          "failed to send group offsets to node {}: {}",
          node_id,
          reply);
        co_return ss::stop_iteration::no;
    }
    vlog(
      _logger.trace,
      "successfully sent group offsets to node {}: {}",
      node_id,
      reply);
    co_return ss::stop_iteration::yes;
}

void compaction_coordinator::recalculate_group_offsets() {
    vassert(_leader_term_id, "only leader can recalculate group offsets");
    model::offset new_mtro = _local_mcco;
    model::offset new_mxro = _local_mxfo;
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
        new_mxro = std::min(new_mxro, fstate.max_transaction_free_offset);
        vlog(
          _logger.trace,
          "on vnode: {} max cleanly compacted offset: {}, new max "
          "transaction-free offset: {}, new max tombstone "
          "remove offset: {}, new max transaction remove offset: {}",
          node_id,
          fstate.max_cleanly_compacted_offset,
          fstate.max_transaction_free_offset,
          new_mtro,
          new_mxro);
    }
    update_group_offsets(new_mtro, new_mxro);
}

void compaction_coordinator::update_group_offsets(
  model::offset new_mtro, model::offset new_mxro) {
    bool mtro_updated = bump_offset_value(
      &compaction_coordinator::_mtro, new_mtro, "max tombstone remove offset");
    bool mxro_updated = bump_offset_value(
      &compaction_coordinator::_mxro,
      new_mxro,
      "max transaction remove offset");
    if (
      _leader_term_id && (mtro_updated || mxro_updated || _need_force_update)) {
        // Reset the flag now. If sending updated offsets to followers fails,
        // we will retry later anyway. Retries may be cancelled
        // a) on leadership change, where the flag will be reset back to true;
        // or
        // b) on a further call to `update_group_offsets` due to
        // MTRO/MXRO change.
        _need_force_update = false;
        send_group_offsets_to_followers();
    }
    _log->stm_hookset()->set_max_tombstone_remove_offset(
      model::prev_offset(_mtro));
    _log->stm_hookset()->set_max_tx_end_remove_offset(
      model::prev_offset(_mxro));
}

void compaction_coordinator::cancel_timer() {
    vlog(_logger.debug, "canceling compaction coordinator timer");
    _timer.cancel();
}

void compaction_coordinator::arm_timer_if_needed(bool jitter_only) {
    if (
      !_started || !_leader_term_id || _raft_as.abort_requested()
      || _raft_bg.is_closed()) {
        return;
    }
    auto duration = jitter_only ? _jitter.next_duration()
                                : _jitter.next_jitter_duration();
    vlog(_logger.debug, "arming compaction coordinator timer for {}", duration);
    _timer.arm(duration);
}

compaction_coordinator::clock_t::duration
compaction_coordinator::test_accessor::local_offsets_getting_delay(
  const compaction_coordinator& coco) {
    return coco._jitter.base_duration() + coco._jitter.jitter_duration();
}

compaction_coordinator::clock_t::duration
compaction_coordinator::test_accessor::group_offsets_distribution_delay() {
    return compaction_coordinator::group_offsets_send_delay;
}

} // namespace raft
