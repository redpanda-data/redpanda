/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/bootstrap_backend.h"
#include "cluster/client_quota_backend.h"
#include "cluster/cluster_link/table.h"
#include "cluster/cluster_recovery_manager.h"
#include "cluster/config_manager.h"
#include "cluster/controller_log_limiter.h"
#include "cluster/data_migration_table.h"
#include "cluster/feature_backend.h"
#include "cluster/plugin_backend.h"
#include "cluster/security_manager.h"
#include "cluster/topic_updates_dispatcher.h"
#include "raft/mux_state_machine.h"

#include <utility>

namespace cluster {

class members_manager;

// single instance
class controller_stm final
  : public raft::mux_state_machine<
      topic_updates_dispatcher,
      security_manager,
      members_manager,
      config_manager,
      feature_backend,
      bootstrap_backend,
      plugin_backend,
      cluster_recovery_manager,
      client_quota::backend,
      data_migrations::migrations_table,
      cluster_link::table> {
public:
    template<typename... Args>
    controller_stm(
      limiter_configuration limiter_conf,
      ss::sharded<features::feature_table>& feature_table,
      config::binding<std::chrono::seconds>&& snapshot_max_age,
      ss::scheduling_group scheduling_group,
      Args&&... stm_args)
      : mux_state_machine(std::forward<Args>(stm_args)...)
      , _limiter(std::move(limiter_conf))
      , _feature_table(feature_table)
      , _snapshot_max_age(std::move(snapshot_max_age))
      , _snapshot_debounce_timer([this] { snapshot_timer_callback(); })
      , _snapshot_poll_timer([this] { snapshot_poll_timer_callback(); })
      , _scheduling_group(scheduling_group) {}

    controller_stm(controller_stm&&) = delete;
    controller_stm(const controller_stm&) = delete;
    controller_stm& operator=(controller_stm&&) = delete;
    controller_stm& operator=(const controller_stm&) = delete;
    ~controller_stm() = default;

    metrics_reporter_cluster_info& get_metrics_reporter_cluster_info() {
        return _metrics_reporter_cluster_info;
    }

    template<typename Cmd>
    requires ControllerCommand<Cmd>
    bool throttle() {
        return _limiter.throttle<Cmd>();
    }

    /// Replicates record batch
    ss::future<result<raft::replicate_result>>
    replicate(model::record_batch&&, std::optional<model::term_id>);

    /// Replicates record batch and waits until state will be applied to the
    /// state machine
    ss::future<std::error_code> replicate_and_wait(
      model::record_batch&& b,
      model::timeout_clock::time_point timeout,
      ss::abort_source& as,
      std::optional<model::term_id> term = std::nullopt);

    void shutdown_apply_loop();
    ss::future<> shutdown();

    virtual ss::future<> stop() final;

    bool ready_to_snapshot() const;

    /// Compose a mini-snapshot for joining or restarting nodes: this is
    /// a specialized peer of the more general maybe_make_snapshot.
    ///
    /// The Backends template pack selects which controller_stm sub-stms
    /// contribute to the snapshot via fill_snapshot. The unselected
    /// sub-stms' corresponding fields on controller_join_snapshot are
    /// left default-constructed (the receiver applies whatever's
    /// present and ignores the rest).
    ///
    /// Typical specializations:
    ///   - <bootstrap_backend, feature_backend, config_manager> for the
    ///     join_node response (newly-registering nodes need the
    ///     cluster_uuid, the feature table, and the cluster config).
    ///   - <feature_backend, config_manager> for the
    ///     fetch_controller_snapshot RPC, used by restarting nodes to
    ///     refresh shard_local_cfg and the feature table from the
    ///     controller leader before downstream services come up.
    template<typename... Backends>
    ss::future<std::optional<iobuf>> maybe_make_join_snapshot();

    /**
     * By calling this function caller may take a lock and prevent applying any
     * new updates to the controller state machine even if they are available.
     */
    ss::future<ssx::semaphore_units> lock_apply();

private:
    ss::future<> on_batch_applied() final;
    void snapshot_timer_callback();
    void snapshot_poll_timer_callback();
    void maybe_arm_snapshot_timer();

    ss::future<std::optional<iobuf>>
    maybe_make_snapshot(ssx::semaphore_units apply_mtx_holder) final;
    ss::future<> apply_snapshot(model::offset, storage::snapshot_reader&) final;

private:
    controller_log_limiter _limiter;
    ss::sharded<features::feature_table>& _feature_table;
    config::binding<std::chrono::seconds> _snapshot_max_age;

    metrics_reporter_cluster_info _metrics_reporter_cluster_info;

    ss::timer<ss::lowres_clock> _snapshot_debounce_timer;
    // Periodic re-check so a stale snapshot is refreshed even when no new
    // controller batches arrive to drive on_batch_applied.
    ss::timer<ss::lowres_clock> _snapshot_poll_timer;
    ss::scheduling_group _scheduling_group;
};

inline constexpr ss::shard_id controller_stm_shard = 0;

template<typename... Backends>
ss::future<std::optional<iobuf>> controller_stm::maybe_make_join_snapshot() {
    // We do **not** check the controller_snapshots feature flag here:
    // even if snapshotting in general is turned off, it is still safe
    // to send snapshots to joining nodes: they will just ignore
    // it if they don't want it.
    //
    // Hold the gate + write_snapshot_mtx for the same reason
    // mux_state_stm::maybe_write_snapshot does — guarantee a coherent
    // point-in-time view across backends.
    auto gate_holder = _gate.hold();
    auto write_snapshot_mtx_holder = co_await _write_snapshot_mtx.get_units();

    if (!ready_to_snapshot()) {
        vlog(clusterlog.debug, "skipping join snapshotting, not ready");
        co_return std::nullopt;
    }

    controller_snapshot snapshot;

    auto apply_mtx_holder = co_await _apply_mtx.get_units();
    model::offset last_applied = get_last_applied_offset();

    // Build a sequential future chain calling fill_snapshot on each
    // selected backend. Sequential because fill_snapshot mutations on
    // the shared controller_snapshot must not race.
    ss::future<> fill_fut = ss::now();
    auto call_fill = [&snapshot, &fill_fut](auto& backend) {
        fill_fut = fill_fut.then(
          [&backend, &snapshot] { return backend.fill_snapshot(snapshot); });
    };
    (call_fill(std::get<Backends&>(_state)), ...);
    co_await std::move(fill_fut);

    apply_mtx_holder.return_all();

    co_return serde::to_iobuf(
      controller_join_snapshot{
        .last_applied = last_applied,
        .bootstrap = std::move(snapshot.bootstrap),
        .features = std::move(snapshot.features),
        .config = std::move(snapshot.config)});
}

} // namespace cluster
