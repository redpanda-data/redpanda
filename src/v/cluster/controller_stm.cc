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

#include "cluster/controller_stm.h"

#include "bytes/iostream.h"
#include "cluster/controller_snapshot.h"
#include "cluster/logger.h"
#include "cluster/members_manager.h"
#include "vlog.h"

namespace cluster {

ss::future<> controller_stm::on_batch_applied() {
    if (!_feature_table.is_active(features::feature::controller_snapshots)) {
        co_return;
    }
    if (_gate.is_closed()) {
        co_return;
    }

    if (
      get_last_applied_offset() > _raft->last_snapshot_index()
      && !_snapshot_debounce_timer.armed()) {
        _snapshot_debounce_timer.arm(_snapshot_max_age());
    };
}

ss::future<> controller_stm::stop() {
    _snapshot_debounce_timer.cancel();
    return base_t::stop();
}

void controller_stm::snapshot_timer_callback() {
    ssx::background
      = ssx::spawn_with_gate_then(_gate, [this] {
            return maybe_write_snapshot().then([](bool written) {
                if (!written) {
                    vlog(clusterlog.info, "skipped writing snapshot");
                }
            });
        }).handle_exception([](const std::exception_ptr& e) {
            vlog(clusterlog.warn, "failed to write snapshot: {}", e);
        });
}

ss::future<std::optional<iobuf>>
controller_stm::maybe_make_snapshot(ssx::semaphore_units apply_mtx_holder) {
    auto started_at = ss::steady_clock_type::now();

    if (!_feature_table.is_active(features::feature::controller_snapshots)) {
        vlog(clusterlog.warn, "skipping snapshotting, feature not enabled");
        co_return std::nullopt;
    }

    controller_snapshot data;
    ss::future<> fill_fut = ss::now();
    auto call_stm_fill = [&fill_fut, &data](auto& stm) {
        fill_fut = fill_fut.then(
          [&data, &stm] { return stm.fill_snapshot(data); });
    };
    std::apply(
      [call_stm_fill](auto&&... stms) { (call_stm_fill(stms), ...); }, _state);
    co_await std::move(fill_fut);

    vlog(
      clusterlog.info,
      "created snapshot at offset {} in {} ms",
      get_last_applied_offset(),
      (ss::steady_clock_type::now() - started_at) / 1ms);

    // release apply_mtx and let the stm continue operation while we are
    // serializing.
    apply_mtx_holder.return_all();
    co_await ss::yield();

    iobuf snapshot_buf;
    co_await serde::write_async(snapshot_buf, std::move(data));
    co_return snapshot_buf;
}

ss::future<> controller_stm::apply_snapshot(
  model::offset offset, storage::snapshot_reader& reader) {
    const size_t size = co_await reader.get_snapshot_size();
    vlog(
      clusterlog.info,
      "loading snapshot at offset: {}, size: {}, previous last_applied: {}",
      offset,
      size,
      get_last_applied_offset());

    auto snap_buf_parser = iobuf_parser{
      co_await read_iobuf_exactly(reader.input(), size)};
    auto snapshot = co_await serde::read_async<controller_snapshot>(
      snap_buf_parser);

    try {
        co_await std::get<bootstrap_backend&>(_state).apply_snapshot(
          offset, snapshot);
        // apply features early so that we have a fresh feature table when
        // applying the rest of the snapshot.
        co_await std::get<feature_backend&>(_state).apply_snapshot(
          offset, snapshot);
        // apply members early so that we have rpc clients to all cluster nodes.
        co_await std::get<members_manager&>(_state).apply_snapshot(
          offset, snapshot);

        // apply everything else in no particular order.
        co_await ss::when_all(
          std::get<config_manager&>(_state).apply_snapshot(offset, snapshot),
          std::get<topic_updates_dispatcher&>(_state).apply_snapshot(
            offset, snapshot),
          std::get<security_manager&>(_state).apply_snapshot(offset, snapshot));
    } catch (...) {
        vassert(
          false,
          "Failed to apply snapshot: {}. State inconsistency possible, "
          "aborting. Snapshot path: {}",
          std::current_exception(),
          _raft->get_snapshot_path());
    }
}

} // namespace cluster
