/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/controller.h"
#include "cluster/metadata_cache.h"
#include "cluster/rm_group_proxy.h"
#include "cluster/tm_stm.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "seastarx.h"

namespace cluster {

class tx_gateway_frontend {
public:
    tx_gateway_frontend(
      ss::smp_service_group,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      cluster::controller*,
      ss::sharded<cluster::id_allocator_frontend>&,
      rm_group_proxy*,
      ss::sharded<cluster::rm_partition_frontend>&);

private:
    [[maybe_unused]] ss::smp_service_group _ssg;
    [[maybe_unused]] ss::sharded<cluster::partition_manager>&
      _partition_manager;
    [[maybe_unused]] ss::sharded<cluster::shard_table>& _shard_table;
    [[maybe_unused]] ss::sharded<cluster::metadata_cache>& _metadata_cache;
    [[maybe_unused]] ss::sharded<rpc::connection_cache>& _connection_cache;
    [[maybe_unused]] ss::sharded<partition_leaders_table>& _leaders;
    [[maybe_unused]] cluster::controller* _controller;
    [[maybe_unused]] ss::sharded<cluster::id_allocator_frontend>&
      _id_allocator_frontend;
    [[maybe_unused]] rm_group_proxy* _rm_group_proxy;
    [[maybe_unused]] ss::sharded<cluster::rm_partition_frontend>&
      _rm_partition_frontend;
};
} // namespace cluster
