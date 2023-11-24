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
#include "cluster/cluster_recovery_table.h"

#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cluster_recovery_state.h"
#include "cluster/logger.h"
#include "cluster/types.h"

namespace cluster {

bool cluster_recovery_table::validate_state_update(recovery_stage next_stage) {
    if (_states.empty()) {
        // Must have an existing recovery.
        return false;
    }
    const auto cur_recovery = _states.back();
    if (
      cur_recovery.stage == recovery_stage::complete
      || cur_recovery.stage == recovery_stage::failed) {
        // Can't update stage after a terminal state.
        return false;
    }
    switch (next_stage) {
    case recovery_stage::initialized:
        // Can't revert to initialized state.
        return false;

    case recovery_stage::starting:
        // Must enter the starting stage from initialized.
        return cur_recovery.stage == recovery_stage::initialized;

    // Can reach terminal states from any non-terminal stage.
    case recovery_stage::complete:
    case recovery_stage::failed:
    default:
        // TODO: be more precise. We expect to progress linearly through
        // stages, but it doesn't seem too problematic to allow the recovery
        // to jump states (in the case of races).
        return true;
    }
}

ss::future<> cluster_recovery_table::wait_for_active_recovery() {
    return _has_active_recovery.wait([this] { return is_recovery_active(); });
}

std::error_code cluster_recovery_table::apply(
  model::offset offset,
  cloud_metadata::cluster_metadata_manifest manifest,
  cloud_storage_clients::bucket_name bucket,
  wait_for_nodes wait_for_nodes) {
    if (!_states.empty() && _states.back().is_active()) {
        return errc::update_in_progress;
    }
    // Either there has never been a recovery, or the latest recovery attempt
    // is complete, in which case it is okay to proceed with another.
    vlog(
      clusterlog.info,
      "Initializing cluster recovery at offset {} with manifest {} from bucket "
      "{}, waiting for nodes: {}",
      offset,
      manifest,
      bucket,
      wait_for_nodes);
    _states.emplace_back(
      std::move(manifest), std::move(bucket), wait_for_nodes);
    _has_active_recovery.signal();
    return errc::success;
}

std::error_code cluster_recovery_table::apply(
  model::offset offset, cluster_recovery_init_cmd cmd) {
    return apply(
      offset,
      std::move(cmd.value.state.manifest),
      std::move(cmd.value.state.bucket));
}

std::error_code
cluster_recovery_table::apply(model::offset, cluster_recovery_update_cmd cmd) {
    if (_states.empty()) {
        vlog(
          clusterlog.error,
          "Invalid recovery state update to {}: no recovery ongoing",
          cmd.value.stage);
        return errc::invalid_request;
    }
    if (!validate_state_update(cmd.value.stage)) {
        vlog(
          clusterlog.error,
          "Invalid recovery state update to {} from {}",
          cmd.value.stage,
          _states.back().stage);
        return errc::invalid_request;
    }
    vlog(clusterlog.info, "Updating recovery state: {}", cmd.value.stage);
    _states.back().stage = cmd.value.stage;
    if (cmd.value.error_msg.has_value()) {
        _states.back().error_msg = std::move(cmd.value.error_msg);
    }
    return errc::success;
}

} // namespace cluster
