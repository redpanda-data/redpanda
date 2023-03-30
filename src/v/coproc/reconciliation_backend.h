/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "coproc/script_context_router.h"
#include "coproc/types.h"
#include "storage/fwd.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/node_hash_map.h>

#include <chrono>

namespace coproc {

class reconciliation_backend {
public:
    explicit reconciliation_backend(
      ss::sharded<cluster::topic_table>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<partition_manager>&,
      ss::sharded<pacemaker>&,
      ss::sharded<wasm::script_database>&) noexcept;

    /// Starts the reconciliation loop
    ///
    /// Listens for events emitted from the topics table
    ss::future<> start();

    /// Stops the reconciliation loop
    ss::future<> stop();

private:
    using update_t = cluster::topic_table::delta;
    using events_cache_t
      = absl::node_hash_map<model::ntp, std::vector<update_t>>;

    ss::future<> fetch_and_reconcile(events_cache_t);
    ss::future<> process_updates(model::ntp, std::vector<update_t>);

    ss::future<std::error_code> process_shutdown(
      model::ntp,
      model::ntp,
      model::revision_id,
      std::vector<model::broker_shard>);
    ss::future<std::error_code> process_restart(
      model::ntp,
      model::ntp,
      model::revision_id,
      std::vector<model::broker_shard>);

    ss::future<std::error_code> process_update(model::ntp, update_t);

    ss::future<std::error_code>
    delete_non_replicable_partition(model::ntp ntp, model::revision_id rev);
    ss::future<std::error_code> create_non_replicable_partition(
      model::ntp ntp, model::revision_id rev, std::vector<model::broker_shard>);
    ss::future<> add_to_shard_table(
      model::ntp ntp, ss::shard_id shard, model::revision_id revision);

    void enqueue_events(cluster::topic_table::delta_range_t);
    ss::future<> process_loop();

    bool stale_create_non_replicable_partition_request(
      const model::ntp& parent_ntp,
      const model::ntp& ntp,
      const std::vector<model::broker_shard>&);

private:
    cluster::notification_id_type _id_cb;
    model::node_id _self;
    ss::sstring _data_directory;
    events_cache_t _topic_deltas;

    ss::gate _gate;
    ss::abort_source _as;

    ss::sharded<cluster::topic_table>& _topics;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::partition_manager>& _cluster_pm;
    ss::sharded<partition_manager>& _coproc_pm;
    ss::sharded<pacemaker>& _pacemaker;
    ss::sharded<wasm::script_database>& _sdb;
};

} // namespace coproc
