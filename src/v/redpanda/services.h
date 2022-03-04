/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "cloud_storage/fwd.h"
#include "cluster/fwd.h"
#include "kafka/server/fwd.h"
#include "raft/fwd.h"
#include "seastarx.h"
#include "storage/fwd.h"

#include <seastar/core/sharded.hh>

struct services {
    ss::sharded<cluster::metadata_cache> metadata_cache;
    ss::sharded<kafka::group_router> group_router;
    ss::sharded<cluster::shard_table> shard_table;
    ss::sharded<storage::api> storage;
    ss::sharded<raft::group_manager> raft_group_manager;
    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    ss::sharded<cloud_storage::partition_recovery_manager>
      partition_recovery_manager;
    ss::sharded<cloud_storage::remote> cloud_storage_api;
    ss::sharded<cloud_storage::cache> shadow_index_cache;
};
