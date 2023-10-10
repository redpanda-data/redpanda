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
#include "cluster/cloud_metadata/offsets_lookup_batcher.h"
#include "cluster/cloud_metadata/offsets_recovery_rpc_types.h"
#include "model/metadata.h"

#include <seastar/core/sharded.hh>

namespace kafka {
class group_manager;
} // namespace kafka

namespace cluster::cloud_metadata {

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
    ss::future<> start() { co_return; }
    ss::future<> stop() { co_return; }

    ss::future<offsets_recovery_reply> recover(offsets_recovery_request);

private:
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
