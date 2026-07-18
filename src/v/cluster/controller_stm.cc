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

#include "base/vlog.h"
#include "bytes/iostream.h"
#include "cluster/controller_snapshot.h"
#include "cluster/data_migration_table.h"
#include "cluster/logger.h"
#include "cluster/members_manager.h"
#include "config/configuration.h"

#include <seastar/core/abort_source.hh>

namespace cluster {

ss::future<> controller_stm::on_batch_applied() {
    maybe_arm_snapshot_timer();
    co_return;
}

void controller_stm::maybe_arm_snapshot_timer() {
    if (!_feature_table.local().is_active(
          features::feature::controller_snapshots)) {
        return;
    }
    if (_gate.is_closed()) {
        return;
    }

    auto current_offset = model::next_offset(last_applied_offset());
    if (
      current_offset > _raft->last_snapshot_index()
      && !_snapshot_debounce_timer.armed()) {
        _snapshot_debounce_timer.arm(_snapshot_max_age());
    };
    // The periodic poll below keeps the uploaded controller snapshot fresh
    // enough for whole-cluster restore -- which only matters when cloud
    // storage is enabled. A pure-local cluster keeps exact upstream behavior
    // (snapshots driven solely by on_batch_applied).
    if (!config::shard_local_cfg().cloud_storage_enabled()) {
        return;
    }
    // Re-arm the poll so a snapshot that becomes stale between controller
    // batches (e.g. a topic created shortly before the controller goes
    // idle, while only produce traffic continues) is still refreshed
    // within max_age. Without this the debounce timer only fires off the
    // next controller batch, which on an idle controller may never come —
    // and whole-cluster restore would miss that topic's metadata.
    if (!_snapshot_poll_timer.armed() && !_gate.is_closed()) {
        _snapshot_poll_timer.arm(_snapshot_max_age());
    }
}
void controller_stm::shutdown_apply_loop() { _as.request_abort(); }

ss::future<> controller_stm::shutdown() {
    _snapshot_debounce_timer.cancel();
    _snapshot_poll_timer.cancel();
    return base_t::stop();
}

ss::future<> controller_stm::stop() { co_return; }

void controller_stm::snapshot_poll_timer_callback() {
    maybe_arm_snapshot_timer();
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

bool controller_stm::ready_to_snapshot() const {
    // cluster info is initialized manually from the first 2 batches of
    // controller log so we have to wait until it is initialized by
    // metrics_reporter before making a snapshot.
    return _metrics_reporter_cluster_info.is_initialized();
}

ss::future<std::optional<iobuf>>
controller_stm::maybe_make_snapshot(ssx::semaphore_units apply_mtx_holder) {
    auto started_at = ss::steady_clock_type::now();

    if (!_feature_table.local().is_active(
          features::feature::controller_snapshots)) {
        vlog(clusterlog.warn, "skipping snapshotting, feature not enabled");
        co_return std::nullopt;
    }

    controller_snapshot data;

    if (!ready_to_snapshot()) {
        vlog(clusterlog.debug, "skipping snapshotting, not ready");
        co_return std::nullopt;
    }

    data.metrics_reporter.cluster_info = _metrics_reporter_cluster_info;

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
        // apply config_manager next so that downstream backends see a
        // fresh shard_local_cfg during their own apply_snapshot work.
        co_await std::get<config_manager&>(_state).apply_snapshot(
          offset, snapshot);
        // apply members early so that we have rpc clients to all cluster nodes.
        co_await std::get<members_manager&>(_state).apply_snapshot(
          offset, snapshot);
        // lots of Redpanda downstream components rely on the topic metadata to
        // be up to date. Therefore apply the topic table state first.
        co_await std::get<topic_updates_dispatcher&>(_state).apply_snapshot(
          offset, snapshot);

        // apply everything else in no particular order.
        co_await ss::when_all(
          std::get<plugin_backend&>(_state).apply_snapshot(offset, snapshot),
          std::get<cluster_recovery_manager&>(_state).apply_snapshot(
            offset, snapshot),
          std::get<security_manager&>(_state).apply_snapshot(offset, snapshot),
          std::get<client_quota::backend&>(_state).apply_snapshot(
            offset, snapshot),
          std::get<data_migrations::migrations_table&>(_state).apply_snapshot(
            offset, snapshot),
          std::get<cluster_link::table&>(_state).apply_snapshot(
            offset, snapshot));

    } catch (const seastar::abort_requested_exception&) {
    } catch (const seastar::gate_closed_exception&) {
    } catch (const seastar::broken_semaphore&) {
    } catch (const seastar::broken_promise&) {
    } catch (const seastar::broken_condition_variable&) {
    } catch (...) {
        vassert(
          false,
          "Failed to apply snapshot: {}. State inconsistency possible, "
          "aborting. Snapshot path: {}",
          std::current_exception(),
          _raft->get_snapshot_path());
    }

    _metrics_reporter_cluster_info = snapshot.metrics_reporter.cluster_info;
    co_await _feature_table.invoke_on_all([&](features::feature_table& ft) {
        ft.set_builtin_trial_license(
          _metrics_reporter_cluster_info.creation_timestamp);
    });
}

ss::future<ssx::semaphore_units> controller_stm::lock_apply() {
    return _apply_mtx.get_units();
}

ss::future<result<raft::replicate_result>> controller_stm::replicate(
  model::record_batch&& b, std::optional<model::term_id> term) {
    return ss::with_scheduling_group(
      _scheduling_group, [this, b = std::move(b), term]() mutable {
          return base_t::replicate(std::move(b), term);
      });
}

/// Replicates record batch and waits until state will be applied to the
/// state machine
ss::future<std::error_code> controller_stm::replicate_and_wait(
  model::record_batch&& b,
  model::timeout_clock::time_point timeout,
  ss::abort_source& as,
  std::optional<model::term_id> term) {
    return ss::with_scheduling_group(
      _scheduling_group,
      [this, b = std::move(b), term, timeout, &as]() mutable {
          return base_t::replicate_and_wait(std::move(b), timeout, as, term);
      });
}

} // namespace cluster
