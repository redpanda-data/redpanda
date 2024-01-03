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

#include "base/seastarx.h"
#include "cloud_storage/fwd.h"
#include "cluster/cluster_recovery_table.h"
#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "storage/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

class cluster_recovery_manager {
public:
    static constexpr ss::shard_id shard = 0;
    cluster_recovery_manager(
      ss::sharded<ss::abort_source>&,
      ss::sharded<controller_stm>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<cloud_storage::remote>& remote,
      ss::sharded<cluster_recovery_table>&,
      ss::sharded<storage::api>&,
      consensus_ptr raft0);

    // Asserts that this controller replica is leader, and waits until all ops
    // are applied to the controller stm.
    //
    // Should be called before making decisions on actions to take, as it
    // ensures that not only is this replica leader, but it is also caught up.
    ss::future<std::optional<model::term_id>> sync_leader(ss::abort_source&);

    // Starts a recovery if one isn't already in progress.
    ss::future<result<cluster::errc, cloud_metadata::error_outcome>>
    initialize_recovery(cloud_storage_clients::bucket_name bucket);

    // Returns true if the update was successfuly replicated and applied.
    // Otherwise, logs a warning and returns false.
    ss::future<cluster::errc> replicate_update(
      model::term_id term,
      recovery_stage next_stage,
      std::optional<ss::sstring> error_msg = std::nullopt);

    // Interface for mux_state_machine.
    static constexpr auto commands = make_commands_list<
      cluster_recovery_init_cmd,
      cluster_recovery_update_cmd>();

    bool is_batch_applicable(const model::record_batch& b) const;
    ss::future<std::error_code> apply_update(model::record_batch b);
    ss::future<> fill_snapshot(controller_snapshot& snap) const {
        _recovery_table.local().fill_snapshot(snap);
        return ss::now();
    }
    ss::future<>
    apply_snapshot(model::offset, const controller_snapshot& snap) {
        co_await _recovery_table.invoke_on_all([&snap](auto& table) mutable {
            table.set_recovery_states(snap.cluster_recovery.recovery_states);
        });
    }

private:
    // Sends updates to each shard of the underlying recovery table.
    template<typename Cmd>
    ss::future<std::error_code>
    dispatch_updates_to_cores(model::offset, Cmd cmd);

    ss::future<std::error_code>
      apply_to_table(model::offset, cluster_recovery_init_cmd);
    ss::future<std::error_code>
      apply_to_table(model::offset, cluster_recovery_update_cmd);

    // NOTE: only sharded to comply with the signature of replicate_and_wait().
    // Still only used on shard-0.
    ss::sharded<ss::abort_source>& _sharded_as;
    ss::sharded<controller_stm>& _controller_stm;
    ss::sharded<features::feature_table>& _feature_table;

    ss::sharded<cloud_storage::remote>& _remote;

    // State that backs the recoveries managed by this manager. Sharded so that
    // the status of the controller recovery is propagated across cores.
    ss::sharded<cluster_recovery_table>& _recovery_table;

    // Storage interface, used to ensure initial state has been initialized
    // (e.g. cluster UUID).
    ss::sharded<storage::api>& _storage;

    consensus_ptr _raft0;
};

} // namespace cluster
