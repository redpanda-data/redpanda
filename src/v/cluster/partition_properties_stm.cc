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
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "raft/persisted_stm.h"
#include "snapshot.h"
#include "storage/log.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/defer.hh>

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
      .update_offset = model::offset{}}}) {}

ss::future<iobuf> partition_properties_stm::take_snapshot(model::offset o) {
    if (o < _raft->start_offset()) {
        throw std::invalid_argument(fmt::format(
          "can not take raft snapshot at offset {} which is smaller than raft "
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
      raft_snapshot{.writes_disabled = it->writes_disabled});
}

ss::future<> partition_properties_stm::apply_local_snapshot(
  raft::stm_snapshot_header header, iobuf&& buffer) {
    vlog(_log.debug, "Applying local snapshot with offset {}", header.offset);
    auto snapshot = serde::from_iobuf<local_snapshot>(std::move(buffer));
    _state_snapshots = std::move(snapshot.state_updates);
    co_return;
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
    if (ot != operation_type::update_writes_disabled) [[unlikely]] {
        vlog(
          _log.warn,
          "ignored unknown operation type with value: {}",
          static_cast<int>(ot));
        return;
    }
    auto update = serde::from_iobuf<update_writes_disabled_cmd>(
      std::move(value));
    vlog(
      _log.trace,
      "Applying update {} at offset: {}",
      update,
      r.offset_delta() + batch_begin_offset);
    bool differs = are_writes_disabled() != update.writes_disabled;
    if (differs) {
        _state_snapshots.push_back(state_snapshot{
          .writes_disabled = update.writes_disabled,
          .update_offset = model::offset(r.offset_delta() + batch_begin_offset),
        });
    }
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
    _state_snapshots.push_back(state_snapshot{
      .writes_disabled = snapshot.writes_disabled,
      .update_offset = model::prev_offset(_raft->start_offset()),
    });
    co_return;
}

ss::future<std::error_code>
partition_properties_stm::replicate_properties_update(
  model::timeout_clock::duration timeout, update_writes_disabled_cmd cmd) {
    if (!co_await sync(timeout)) {
        co_return errc::not_leader;
    }

    auto b = make_update_partitions_batch(cmd);
    auto deadline = timeout + model::timeout_clock::now();
    vlog(
      _log.debug, "replicating update partition properties command: {}", cmd);
    raft::replicate_options r_opts(
      raft::consistency_level::quorum_ack,
      std::chrono::milliseconds(timeout / 1ms));
    r_opts.set_force_flush();
    auto r = co_await _raft->replicate(
      _insync_term,
      model::make_memory_record_batch_reader(std::move(b)),
      r_opts);

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

    auto applied = co_await wait_no_throw(r.value().last_offset, deadline);
    if (!applied) {
        co_return errc::timeout;
    }
    co_return errc::success;
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

ss::future<std::error_code> partition_properties_stm::disable_writes() {
    vlog(_log.info, "disabling partition writes");
    return replicate_properties_update(
      _sync_timeout(),
      update_writes_disabled_cmd{.writes_disabled = writes_disabled::yes});
}

ss::future<std::error_code> partition_properties_stm::enable_writes() {
    vlog(_log.info, "enabling partition writes");
    return replicate_properties_update(
      _sync_timeout(),
      update_writes_disabled_cmd{.writes_disabled = writes_disabled::no});
}

ss::future<result<partition_properties_stm::writes_disabled>>
partition_properties_stm::sync_writes_disabled() {
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
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto stm = builder.create_stm<partition_properties_stm>(
      raft, clusterlog, _kvstore, _sync_timeout);
    raft->log()->stm_manager()->add_stm(stm);
}

std::ostream& operator<<(
  std::ostream& o, const partition_properties_stm::raft_snapshot& snap) {
    fmt::print(o, "{{writes_disabled: {}}}", snap.writes_disabled);
    return o;
}

std::ostream& operator<<(
  std::ostream& o,
  const partition_properties_stm::update_writes_disabled_cmd& update) {
    fmt::print(o, "{{writes_disabled: {}}}", update.writes_disabled);
    return o;
}

std::ostream& operator<<(
  std::ostream& o, const partition_properties_stm::local_snapshot& snap) {
    fmt::print(o, "{{state_updates: {}}}", fmt::join(snap.state_updates, ", "));
    return o;
}

std::ostream& operator<<(
  std::ostream& o, const partition_properties_stm::state_snapshot& update) {
    fmt::print(
      o,
      "{{update_offset: {}, writes_disabled: {}}}",
      update.update_offset,
      update.writes_disabled);
    return o;
}

} // namespace cluster
