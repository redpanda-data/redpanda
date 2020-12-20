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

#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "kafka/fetch_session_cache.h"
#include "kafka/groups/group_router.h"
#include "kafka/quota_manager.h"
#include "rpc/server.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace cluster {
class metadata_cache;
}

namespace kafka {

class connection_context;

class protocol final : public rpc::server::protocol {
public:
    protocol(
      ss::smp_service_group,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<cluster::topics_frontend>&,
      ss::sharded<quota_manager>&,
      ss::sharded<kafka::group_router_type>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<coordinator_ntp_mapper>& coordinator_mapper,
      ss::sharded<fetch_session_cache>&) noexcept;

    ~protocol() noexcept override = default;
    protocol(const protocol&) = delete;
    protocol& operator=(const protocol&&) = delete;
    protocol(protocol&&) noexcept = default;
    protocol& operator=(protocol&&) noexcept = delete;

    const char* name() const final { return "kafka rpc protocol"; }
    // the lifetime of all references here are guaranteed to live
    // until the end of the server (container/parent)
    ss::future<> apply(rpc::server::resources) final;

private:
    friend class connection_context;

    ss::smp_service_group _smp_group;

    // services needed by kafka proto
    ss::sharded<cluster::topics_frontend>& _topics_frontend;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<quota_manager>& _quota_mgr;
    ss::sharded<kafka::group_router_type>& _group_router;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<kafka::coordinator_ntp_mapper>& _coordinator_mapper;
    ss::sharded<kafka::fetch_session_cache>& _fetch_session_cache;
};

} // namespace kafka
