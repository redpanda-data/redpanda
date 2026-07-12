/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/partition_properties_stm.h"

#include "base/vassert.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/snapshot.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "raft/persisted_stm.h"
#include "storage/log.h"
#include "utils/prefix_logger.h"

#include <fmt/format.h>

#include <algorithm>
#include <iterator>

namespace cluster {

partition_properties_stm::partition_properties_stm(
  raft::consensus* raft,
  ss::logger& logger,
  storage::kvstore& kvstore,
  config::binding<std::chrono::milliseconds> sync_timeout)
  : raft::persisted_stm<raft::kvstore_backed_stm_snapshot>(
      partition_properties_stm_snapshot, logger, raft, kvstore)
  , _sync_timeout(std::move(sync_timeout))
  , _state_snapshots({state_snapshot{
      .writes_disabled = writes_disabled::no,
      .update_offset = model::offset{},
      .writes_revision_id{},
      .partition_mode = model::redpanda_storage_mode::unset}}) {}

ss::future<iobuf>
partition_properties_stm::take_raft_snapshot(model::offset o) {
    if (o < _raft->start_offset()) {
        throw std::invalid_argument(
          fmt::format(
            "can not take raft snapshot at offset {} which is smaller than "
            "raft "
            "start offset",
            o,
            _raft->start_offset()));
    }
    vlog(
      _log.trace,
      "taking snapshot at offset: {}, state snapshots: {}",
      o,
      fmt::join(_state_snapshots, ", "));

    auto it = std::upper_bound(
      _state_snapshots.begin(),
      _state_snapshots.end(),
      o,
      [](model::offset o, const state_snapshot& current_update) {
          return o < current_update.update_offset;
      });

    vassert(
      it != _state_snapshots.begin(),
      "Partition properties state machine holds an invariant that first "
      "snapshot in state snapshots field is smaller than raft start offset "
      "therefore the situation in which first element of the "
      "_state_snapshots is returned is a critical failure. Requested "
      "offset {}, state snapshots: {}",
      o,
      fmt::join(_state_snapshots, ", "));

    // move iterator one step back to obtain cached snapshot with smaller
    // offset
    --it;
    vlog(
      _log.trace,
      "returning indexed snapshot with offset: {}, requested offset: {}",
      it->update_offset,
      o);
    co_return serde::to_iobuf(
      raft_snapshot{
        .writes_disabled = it->writes_disabled,
        .writes_revision_id = it->writes_revision_id,
        .partition_mode = it->partition_mode});
}

ss::future<raft::local_snapshot_applied>
partition_properties_stm::apply_local_snapshot(
  raft::stm_snapshot_header header, iobuf&& buffer) {
    vlog(_log.debug, "Applying local snapshot with offset {}", header.offset);
    auto snapshot = serde::from_iobuf<local_snapshot>(std::move(buffer));
    _state_snapshots = std::move(snapshot.state_updates);
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot> partition_properties_stm::take_local_snapshot(
  ssx::semaphore_units apply_units) {
    vlog(_log.debug, "Taking local snapshot");

    auto last_applied = last_applied_offset();
    // cleanup snapshot index
    // delete all obsolete snapshot indicies
    auto it = std::lower_bound(
      _state_snapshots.begin(),
      _state_snapshots.end(),
      _raft->start_offset(),
      [](const state_snapshot& current_update, model::offset o) {
          return current_update.update_offset < o;
      });
    vassert(
      it != _state_snapshots.begin(),
      "State snapshot vector must always contain element that is strictly "
      "smaller than the underlying log start offset. Snapshot vector {}",
      fmt::join(_state_snapshots, ", "));
    // decrease iterator to preseve entry smaller than the start offset
    --it;
    // cleanup only if there are elements to remove
    if (it != _state_snapshots.begin()) {
        chunked_vector<state_snapshot> state_updates;
        state_updates.reserve(std::distance(it, _state_snapshots.end()));
        // keep last snapshot with offset smaller than or equal to raft log
        // start offset to be able to preserve the state from before the
        // snapshot was taken
        std::copy(
          it, _state_snapshots.end(), std::back_inserter(state_updates));
        _state_snapshots = std::move(state_updates);
    }

    auto state = _state_snapshots.copy();
    apply_units.return_all();
    co_return raft::stm_snapshot::create(
      0,
      last_applied,
      serde::to_iobuf(local_snapshot{.state_updates = std::move(state)}));
}

ss::future<> partition_properties_stm::seed_partition_mode(
  storage::kvstore& kvstore,
  ss::logger& logger,
  const model::ntp& ntp,
  model::offset snapshot_offset,
  model::redpanda_storage_mode mode) {
    // A single base state entry (update_offset {}, strictly below any log start
    // offset) carrying the mode, matching the STM's initial in-memory state
    // plus partition_mode.
    local_snapshot snap;
    snap.state_updates.push_back(
      state_snapshot{
        .writes_disabled = writes_disabled::no,
        .update_offset = model::offset{},
        .writes_revision_id = {},
        .partition_mode = mode});
    prefix_logger log(logger, fmt::format("[{}]", ntp));
    raft::kvstore_backed_stm_snapshot backend(
      partition_properties_stm_snapshot, log, ntp, kvstore);
    co_await backend.persist_local_snapshot(
      raft::stm_snapshot::create(
        0, snapshot_offset, serde::to_iobuf(std::move(snap))));
}

ss::future<> partition_properties_stm::do_apply(const model::record_batch& b) {
    if (
      b.header().type
      != model::record_batch_type::partition_properties_update) {
        co_return;
    }
    b.for_each_record([this, bo = b.header().base_offset](model::record r) {
        apply_record(std::move(r), bo);
    });
}

void partition_properties_stm::apply_record(
  model::record r, model::offset batch_begin_offset) {
    auto key = r.release_key();
    auto value = r.release_value();
    auto ot = serde::from_iobuf<operation_type>(std::move(key));
    const auto update_offset = model::offset(
      r.offset_delta() + batch_begin_offset);
    const auto current_state = _state_snapshots.back();
    switch (ot) {
    case operation_type::update_writes_disabled: {
        auto update = serde::from_iobuf<update_writes_disabled_cmd>(
          std::move(value));
        vlog(
          _log.trace,
          "Applying update {} at offset: {}",
          update,
          update_offset);
        bool is_legacy_command = update.writes_revision_id
                                 == model::revision_id{};
        bool is_newer_revision = update.writes_revision_id
                                 > current_state.writes_revision_id;
        if (!is_newer_revision && !is_legacy_command) {
            vlog(
              _log.error,
              "Ignoring out-of-order update: {}, current state: {}",
              update,
              current_state);
            return;
        }
        bool differs = update.writes_disabled != current_state.writes_disabled
                       || update.writes_revision_id
                            != current_state.writes_revision_id;
        if (differs) {
            // copy-modify preserves the other properties (e.g. partition_mode)
            auto next = current_state;
            next.writes_disabled = update.writes_disabled;
            next.writes_revision_id = update.writes_revision_id;
            next.update_offset = update_offset;
            _state_snapshots.push_back(next);
        }
        return;
    }
    case operation_type::update_partition_mode: {
        auto update = serde::from_iobuf<update_partition_mode_cmd>(
          std::move(value));
        vlog(
          _log.trace,
          "Applying partition_mode update {} at offset: {}",
          update,
          update_offset);
        // idempotent by value, ordered by log offset (no revision needed).
        if (update.partition_mode != current_state.partition_mode) {
            auto next = current_state;
            next.partition_mode = update.partition_mode;
            next.update_offset = update_offset;
            _state_snapshots.push_back(next);
            notify_partition_mode_change();
        }
        return;
    }
    }
    vlog(
      _log.warn,
      "ignored unknown operation type with value: {}",
      static_cast<int>(ot));
}

ss::future<>
partition_properties_stm::apply_raft_snapshot(const iobuf& buffer) {
    if (buffer.empty()) {
        vlog(_log.debug, "Applying empty raft snapshot");
        co_return;
    }

    auto snapshot = serde::from_iobuf<raft_snapshot>(buffer.copy());

    vlog(_log.debug, "Applying raft snapshot {}", snapshot);
    _state_snapshots.clear();
    _state_snapshots.push_back(
      state_snapshot{
        .writes_disabled = snapshot.writes_disabled,
        .update_offset = model::prev_offset(_raft->start_offset()),
        .writes_revision_id = snapshot.writes_revision_id,
        .partition_mode = snapshot.partition_mode});
    notify_partition_mode_change();
    co_return;
}

ss::future<result<model::offset>>
partition_properties_stm::replicate_properties_update(
  model::timeout_clock::duration timeout, update_writes_disabled_cmd cmd) {
    auto holder = _gate.hold();
    auto units = co_await _writes_mutex.get_units();
    if (!co_await sync(timeout)) {
        co_return errc::not_leader;
    }

    // sync'ed and holding mutex, check revision
    vassert(
      !_state_snapshots.empty(),
      "The invariant of state snapshot containing at least one element is "
      "broken");
    auto current_state = _state_snapshots.back();
    if (
      cmd.writes_revision_id == current_state.writes_revision_id
      && cmd.writes_disabled == current_state.writes_disabled) [[unlikely]] {
        // no-op
        co_return current_state.update_offset;
    }
    if (
      cmd.writes_revision_id != model::revision_id{}
      && cmd.writes_revision_id <= current_state.writes_revision_id)
      [[unlikely]] {
        vlog(
          _log.warn,
          "Skipping replicate_properties_update with out-of-order cmd: {}, "
          "current state: {}",
          cmd,
          current_state);
        co_return errc::invalid_data_migration_state;
    }

    // replicate the command
    auto b = make_update_partitions_batch(cmd);
    vlog(
      _log.debug, "replicating update partition properties command: {}", cmd);
    raft::replicate_options r_opts(
      raft::consistency_level::quorum_ack,
      _insync_term,
      std::chrono::milliseconds(timeout / 1ms));
    r_opts.set_force_flush();
    auto r = co_await _raft->replicate(std::move(b), r_opts);

    if (r.has_error()) {
        vlog(
          _log.warn,
          "error replicating update partition properties command: {} - {}",
          cmd,
          r.error().message());
        // stepdown to force sync and state update on the next request
        co_await _raft->step_down("partition_properties_stm/replication_error");
        co_return r.error();
    }

    auto message_offset = r.value().last_offset;
    // we cannot return errc::timeout, as we must get to a consistent state by
    // the time we unlock the mutex
    if (!co_await wait_no_throw(
          message_offset, model::timeout_clock::time_point::max())) {
        co_await _raft->step_down("partition_properties_stm/replication_error");
        co_return errc::shutting_down;
    }
    co_return message_offset;
}

partition_properties_stm::writes_disabled
partition_properties_stm::are_writes_disabled() const {
    vassert(
      !_state_snapshots.empty(),
      "The invariant of state snapshot containing at least one element is "
      "broken");
    return _state_snapshots.back().writes_disabled;
}

model::record_batch partition_properties_stm::make_update_partitions_batch(
  update_writes_disabled_cmd cmd) {
    storage::record_batch_builder builder(
      model::record_batch_type::partition_properties_update, model::offset{});
    builder.add_raw_kv(
      serde::to_iobuf(operation_type::update_writes_disabled),
      serde::to_iobuf(std::move(cmd)));
    return std::move(builder).build();
}

model::record_batch partition_properties_stm::make_update_partition_mode_batch(
  update_partition_mode_cmd cmd) {
    storage::record_batch_builder builder(
      model::record_batch_type::partition_properties_update, model::offset{});
    builder.add_raw_kv(
      serde::to_iobuf(operation_type::update_partition_mode),
      serde::to_iobuf(std::move(cmd)));
    return std::move(builder).build();
}

ss::future<result<model::offset>>
partition_properties_stm::replicate_partition_mode_update(
  model::timeout_clock::duration timeout, update_partition_mode_cmd cmd) {
    auto holder = _gate.hold();
    auto units = co_await _writes_mutex.get_units();
    if (!co_await sync(timeout)) {
        co_return errc::not_leader;
    }

    vassert(
      !_state_snapshots.empty(),
      "The invariant of state snapshot containing at least one element is "
      "broken");
    auto current_state = _state_snapshots.back();
    if (cmd.partition_mode == current_state.partition_mode) [[unlikely]] {
        // no-op
        co_return current_state.update_offset;
    }

    auto b = make_update_partition_mode_batch(cmd);
    vlog(_log.debug, "replicating update partition mode command: {}", cmd);
    raft::replicate_options r_opts(
      raft::consistency_level::quorum_ack,
      _insync_term,
      std::chrono::milliseconds(timeout / 1ms));
    r_opts.set_force_flush();
    auto r = co_await _raft->replicate(std::move(b), r_opts);

    if (r.has_error()) {
        vlog(
          _log.warn,
          "error replicating update partition mode command: {} - {}",
          cmd,
          r.error().message());
        co_await _raft->step_down("partition_properties_stm/replication_error");
        co_return r.error();
    }

    auto message_offset = r.value().last_offset;
    if (!co_await wait_no_throw(
          message_offset, model::timeout_clock::time_point::max())) {
        co_await _raft->step_down("partition_properties_stm/replication_error");
        co_return errc::shutting_down;
    }
    co_return message_offset;
}

ss::future<result<model::offset>> partition_properties_stm::set_partition_mode(
  model::redpanda_storage_mode mode) {
    vlog(_log.info, "setting partition mode to {}", mode);
    return replicate_partition_mode_update(
      _sync_timeout(), update_partition_mode_cmd{.partition_mode = mode});
}

model::redpanda_storage_mode partition_properties_stm::partition_mode() const {
    vassert(
      !_state_snapshots.empty(),
      "The invariant of state snapshot containing at least one element is "
      "broken");
    return _state_snapshots.back().partition_mode;
}

void partition_properties_stm::notify_partition_mode_change() {
    if (_partition_mode_change_cb) {
        _partition_mode_change_cb();
    }
}

ss::future<result<model::redpanda_storage_mode>>
partition_properties_stm::sync_partition_mode() {
    auto holder = _gate.hold();
    if (!co_await sync(_sync_timeout())) {
        co_return errc::not_leader;
    }
    co_return partition_mode();
}

ss::future<result<model::offset>>
partition_properties_stm::disable_writes(model::revision_id revision_id) {
    vlog(_log.info, "disabling partition writes");
    return replicate_properties_update(
      _sync_timeout(),
      update_writes_disabled_cmd{
        .writes_disabled = writes_disabled::yes,
        .writes_revision_id = revision_id});
}

ss::future<result<model::offset>>
partition_properties_stm::enable_writes(model::revision_id revision_id) {
    vlog(_log.info, "enabling partition writes");
    return replicate_properties_update(
      _sync_timeout(),
      update_writes_disabled_cmd{
        .writes_disabled = writes_disabled::no,
        .writes_revision_id = revision_id});
}

ss::future<result<partition_properties_stm::writes_disabled>>
partition_properties_stm::sync_writes_disabled() {
    auto holder = _gate.hold();
    if (!co_await sync(_sync_timeout())) {
        co_return errc::not_leader;
    }
    co_return are_writes_disabled();
}

partition_properties_stm_factory::partition_properties_stm_factory(
  storage::kvstore& kvstore,
  config::binding<std::chrono::milliseconds> sync_timeout)
  : _kvstore(kvstore)
  , _sync_timeout(std::move(sync_timeout)) {}

bool partition_properties_stm_factory::is_applicable_for(
  const storage::ntp_config& cfg) const {
    // we create partition properties stm only for the Kafka data topics.
    return model::is_user_topic(cfg.ntp());
}

void partition_properties_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto stm = builder.create_stm<partition_properties_stm>(
      raft, clusterlog, _kvstore, _sync_timeout);
    raft->log()->stm_hookset()->add_stm(stm);
}

fmt::iterator
partition_properties_stm::raft_snapshot::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{writes_disabled: {}, writes_revision_id: {}, partition_mode: {}}}",
      writes_disabled,
      writes_revision_id,
      model::redpanda_storage_mode_to_string(partition_mode));
}

fmt::iterator partition_properties_stm::update_partition_mode_cmd::format_to(
  fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{partition_mode: {}}}",
      model::redpanda_storage_mode_to_string(partition_mode));
}

fmt::iterator partition_properties_stm::update_writes_disabled_cmd::format_to(
  fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{writes_disabled: {}, writes_revision_id: {}}}",
      writes_disabled,
      writes_revision_id);
}

fmt::iterator
partition_properties_stm::state_snapshot::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{update_offset: {}, writes_disabled: {}, writes_revision_id: {}, "
      "partition_mode: {}}}",
      update_offset,
      writes_disabled,
      writes_revision_id,
      model::redpanda_storage_mode_to_string(partition_mode));
}

fmt::iterator
partition_properties_stm::local_snapshot::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{state_updates: {}}}", fmt::join(state_updates, ", "));
}

} // namespace cluster
