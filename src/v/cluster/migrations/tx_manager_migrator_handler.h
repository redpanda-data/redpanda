// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cluster/migrations/tx_manager_migrator.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/tx_manager_migrator_service.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
namespace cluster {

/**
 * Since transaction manager topic may be hosted on multiple nodes we add
 * here a single entry point for reading and replicating data into
 * transactional manager topic
 */

class tx_manager_migrator_handler : public tx_manager_migrator_service {
public:
    explicit tx_manager_migrator_handler(
      ss::scheduling_group sg,
      ss::smp_service_group ssg,
      ss::sharded<partition_manager>& pm,
      ss::sharded<shard_table>& st,
      ss::sharded<cluster::metadata_cache>& metadata_cache,
      ss::sharded<rpc::connection_cache>& connection_cache,
      ss::sharded<partition_leaders_table>& leaders,
      const model::node_id self)
      : tx_manager_migrator_service(sg, ssg)
      , _read_router(pm, st, metadata_cache, connection_cache, leaders, self)
      , _replicate_router(
          pm, st, metadata_cache, connection_cache, leaders, self) {}

    ss::future<tx_manager_replicate_reply> tx_manager_replicate(
      tx_manager_replicate_request request, ::rpc::streaming_context&) final {
        auto ntp = request.ntp;
        return _replicate_router.find_shard_and_process(
          std::move(request), ntp, tx_manager_migrator::default_timeout);
    }

    ss::future<tx_manager_read_reply> tx_manager_read(
      tx_manager_read_request request, ::rpc::streaming_context&) final {
        auto ntp = request.ntp;
        return _read_router.find_shard_and_process(
          std::move(request), ntp, tx_manager_migrator::default_timeout);
    }

private:
    tx_manager_read_router _read_router;
    tx_manager_replicate_router _replicate_router;
};

}; // namespace cluster
