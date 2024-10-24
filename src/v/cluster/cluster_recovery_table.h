/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cluster_recovery_state.h"
#include "cluster/commands.h"
#include "cluster/controller_snapshot.h"
#include "cluster/types.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

namespace cluster {

// Returns true if the given status implies there may be more state to be
// recovered from a remote controller snapshot.
inline bool
may_require_controller_recovery(std::optional<recovery_stage> cur_status) {
    if (!cur_status.has_value()) {
        return true;
    }
    switch (cur_status.value()) {
    case recovery_stage::recovered_controller_snapshot:
    case recovery_stage::recovered_offsets_topic:
    case recovery_stage::recovered_tx_coordinator:
    case recovery_stage::complete:
    case recovery_stage::failed:
        return false;
    default:
        return true;
    }
};

inline bool
may_require_producer_id_recovery(std::optional<recovery_stage> cur_stage) {
    if (!cur_stage.has_value()) {
        return true;
    }
    switch (cur_stage.value()) {
    case recovery_stage::recovered_tx_coordinator:
    case recovery_stage::complete:
    case recovery_stage::failed:
        return false;
    default:
        return true;
    }
}

inline bool
may_require_offsets_recovery(std::optional<recovery_stage> cur_stage) {
    if (!cur_stage.has_value()) {
        return true;
    }
    switch (cur_stage.value()) {
    case recovery_stage::recovered_offsets_topic:
    case recovery_stage::recovered_tx_coordinator:
    case recovery_stage::complete:
    case recovery_stage::failed:
        return false;
    default:
        return true;
    }
}

// Tracks the state of recovery attempts performed on the cluster.
//
// It is expected this will live on every shard on every node, making it easy
// to determine from any shard what the current status of recovery is (e.g. for
// reporting, or to install a guardrail while recovery is running).
class cluster_recovery_table {
public:
    ss::future<> stop() {
        stop_waiters();
        return ss::make_ready_future();
    }

    void stop_waiters() { _has_active_recovery.broken(); }

    bool is_recovery_active() const {
        if (_states.empty()) {
            return false;
        }
        return _states.back().is_active();
    }

    std::optional<recovery_stage> current_status() const {
        if (_states.empty()) {
            return std::nullopt;
        }
        return _states.back().stage;
    }

    // If no recovery is active, returns immediately. Otherwise, waits for a
    // recovery to be initialized.
    ss::future<> wait_for_active_recovery();

    std::optional<std::reference_wrapper<const cluster_recovery_state>>
    current_recovery() const {
        if (_states.empty()) {
            return std::nullopt;
        }
        return std::reference_wrapper(_states.back());
    }

    std::error_code apply(
      model::offset offset,
      cloud_metadata::cluster_metadata_manifest,
      cloud_storage_clients::bucket_name,
      wait_for_nodes wait = wait_for_nodes::no);
    std::error_code apply(model::offset offset, cluster_recovery_init_cmd);
    std::error_code apply(model::offset offset, cluster_recovery_update_cmd);

    void fill_snapshot(controller_snapshot& snap) {
        snap.cluster_recovery.recovery_states = _states;
    }
    void set_recovery_states(std::vector<cluster_recovery_state> states) {
        _states = std::move(states);
    }

private:
    bool validate_state_update(recovery_stage next_stage);

    ss::condition_variable _has_active_recovery;
    std::vector<cluster_recovery_state> _states;
};

} // namespace cluster
