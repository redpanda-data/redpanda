/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/fwd.h"
#include "cluster/topic_table.h"
#include "coproc/fwd.h"
#include "coproc/ntp_context.h"
#include "coproc/types.h"
#include "storage/fwd.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

namespace coproc {

class reconciliation_backend
  : public ss::peering_sharded_service<reconciliation_backend> {
public:
    explicit reconciliation_backend(
      ss::sharded<wasm::script_database>&,
      ss::sharded<storage::api>&,
      ss::sharded<cluster::topic_table>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<pacemaker>&) noexcept;

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> process_shutdown(
      model::ntp,
      model::revision_id,
      std::vector<model::broker_shard>,
      std::vector<storage::ntp_config>);
    ss::future<> process_restart(model::ntp, model::revision_id);
    ss::future<> process_update(cluster::topic_table::delta);
    ss::future<> process_updates(std::vector<cluster::topic_table::delta>);

private:
    cluster::notification_id_type _id_cb;
    model::node_id _self;

    ss::gate _gate;
    ss::abort_source _as;
    ss::semaphore _sem{1};

    ss::sharded<wasm::script_database>& _sdb;
    ss::sharded<storage::api>& _storage;
    ss::sharded<cluster::topic_table>& _topics;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<pacemaker>& _pacemaker;

    /// If xcore moves occur, also move in-memory state
    struct state_revision {
        ntp_context::offset_tracker offsets;
        std::vector<storage::ntp_config> configs;
        model::revision_id r_id;
    };
    absl::node_hash_map<model::ntp, state_revision> _saved_ctxs;
};

} // namespace coproc
