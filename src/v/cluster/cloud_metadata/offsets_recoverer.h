/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_storage/fwd.h"
#include "cluster/cloud_metadata/offsets_recovery_rpc_types.h"
#include "cluster/cloud_metadata/offsets_snapshot.h"
#include "cluster/fwd.h"
#include "model/metadata.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace rpc {
class connection_cache;
} // namespace rpc

namespace kafka {
class group_manager;
} // namespace kafka

namespace cluster::cloud_metadata {

class offsets_lookup;

class offsets_recoverer {
public:
    offsets_recoverer(
      model::node_id node_id,
      ss::sharded<cloud_storage::remote>& remote,
      ss::sharded<cloud_storage::cache>& cache,
      ss::sharded<offsets_lookup>& local_lookup,
      ss::sharded<cluster::partition_leaders_table>& leaders_table,
      ss::sharded<rpc::connection_cache>& connection_cache,
      ss::sharded<kafka::group_manager>& gm);
    ss::future<> start() { return ss::make_ready_future<>(); }
    ss::future<> stop();

    // Recovers the offsets of a given partition of the consumer offsets topic,
    // as indicated by the given recovery request.
    ss::future<offsets_recovery_reply> recover(
      offsets_recovery_request,
      size_t groups_per_batch = default_groups_per_batch);

private:
    static constexpr auto default_groups_per_batch = 1000;
    // Recovers a given set of groups on the given partition of the consumer
    // offsets topic.
    //
    // Returns the following error codes
    // - errc::success: if the operation succeeded
    // - errc::timeout: past the deadline
    // - errc::replication_error: the local replica is not the leader
    // - errc::waiting_for_recovery: for other errors
    ss::future<cluster::errc> recover_groups(
      model::partition_id,
      group_offsets_snapshot,
      retry_chain_node& retry_parent);

    ss::gate _gate;
    ss::abort_source _as;

    model::node_id _node_id;
    ss::sharded<cloud_storage::remote>& _remote;
    ss::sharded<cloud_storage::cache>& _cache;
    ss::sharded<offsets_lookup>& _offsets_lookup;
    ss::sharded<cluster::partition_leaders_table>& _leaders_table;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<kafka::group_manager>& _group_manager;
};

} // namespace cluster::cloud_metadata
