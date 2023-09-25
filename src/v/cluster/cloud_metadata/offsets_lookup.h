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

#include "cluster/cloud_metadata/offsets_lookup_rpc_types.h"
#include "cluster/fwd.h"
#include "model/metadata.h"

#include <seastar/core/sharded.hh>

namespace cluster::cloud_metadata {

class offsets_lookup {
public:
    offsets_lookup(
      model::node_id node_id,
      ss::sharded<cluster::partition_manager>& pm,
      ss::sharded<cluster::shard_table>& st);
    ss::future<> start() { co_return; }
    ss::future<> stop() { co_return; }

    ss::future<offsets_lookup_reply> lookup(offsets_lookup_request);

private:
    model::node_id _node_id;
    ss::sharded<cluster::partition_manager>& _partitions;
    ss::sharded<cluster::shard_table>& _shards;
};

} // namespace cluster::cloud_metadata
