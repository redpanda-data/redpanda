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
#include "cluster/cloud_metadata/offsets_lookup.h"
#include "cluster/cloud_metadata/offsets_lookup_rpc_types.h"
#include "cluster/partition_leaders_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "rpc/connection_cache.h"
#include "utils/retry_chain_node.h"

#include <absl/container/btree_map.h>

namespace cluster::cloud_metadata {

class offsets_lookup_batcher {
public:
    typedef absl::btree_map<model::ntp, kafka::offset> map_t;
    explicit offsets_lookup_batcher(
      model::node_id node_id,
      offsets_lookup& local_lookup,
      cluster::partition_leaders_table& leaders_table,
      rpc::connection_cache& connection_cache,
      size_t batch_size = 100)
      : _node_id(node_id)
      , _local_lookup(local_lookup)
      , _leaders_table(leaders_table)
      , _connection_cache(connection_cache)
      , _batch_size(batch_size) {}

    // Splits the given list of NTPs into per-node requests and dispatches
    // them, retrying on transient failures.
    //
    // Stops early if the given retry node is no longer permitted to proceed
    // (e.g. timeout, aborted, etc).
    ss::future<> run_lookups(
      absl::btree_set<model::ntp> ntps_to_lookup,
      retry_chain_node& parent_node);

    // Should not be used while `run_lookups()` is called.
    const map_t& offsets_by_ntp() const { return _offset_by_ntp; }

    void clear() { _offset_by_ntp.clear(); }

private:
    ss::future<offsets_lookup_reply> send_request(offsets_lookup_request req);

    const model::node_id _node_id;
    offsets_lookup& _local_lookup;
    cluster::partition_leaders_table& _leaders_table;
    rpc::connection_cache& _connection_cache;
    size_t _batch_size;

    map_t _offset_by_ntp;
};

} // namespace cluster::cloud_metadata
