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

#include "cluster/fwd.h"
#include "config/configuration.h"
#include "kafka/server/fwd.h"
#include "rpc/server.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

namespace kafka {

class protocol final : public rpc::server::protocol {
public:
    protocol(
      ss::smp_service_group,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<cluster::topics_frontend>&,
      ss::sharded<quota_manager>&,
      ss::sharded<kafka::group_router>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<coordinator_ntp_mapper>& coordinator_mapper,
      ss::sharded<fetch_session_cache>&,
      ss::sharded<cluster::id_allocator_frontend>&) noexcept;

    ~protocol() noexcept override = default;
    protocol(const protocol&) = delete;
    protocol& operator=(const protocol&&) = delete;
    protocol(protocol&&) noexcept = default;
    protocol& operator=(protocol&&) noexcept = delete;

    const char* name() const final { return "kafka rpc protocol"; }
    // the lifetime of all references here are guaranteed to live
    // until the end of the server (container/parent)
    ss::future<> apply(rpc::server::resources) final;

    ss::smp_service_group smp_group() const { return _smp_group; }
    cluster::topics_frontend& topics_frontend() {
        return _topics_frontend.local();
    }
    cluster::metadata_cache& metadata_cache() {
        return _metadata_cache.local();
    }
    cluster::id_allocator_frontend& id_allocator_frontend() {
        return _id_allocator_frontend.local();
    }
    kafka::group_router& group_router() { return _group_router.local(); }
    cluster::shard_table& shard_table() { return _shard_table.local(); }
    ss::sharded<cluster::partition_manager>& partition_manager() {
        return _partition_manager;
    }
    coordinator_ntp_mapper& coordinator_mapper() {
        return _coordinator_mapper.local();
    }
    fetch_session_cache& fetch_sessions_cache() {
        return _fetch_session_cache.local();
    }
    quota_manager& quota_mgr() { return _quota_mgr.local(); }
    bool is_idempotence_enabled() { return _is_idempotence_enabled; }

private:
    ss::smp_service_group _smp_group;
    ss::sharded<cluster::topics_frontend>& _topics_frontend;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<quota_manager>& _quota_mgr;
    ss::sharded<kafka::group_router>& _group_router;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<kafka::coordinator_ntp_mapper>& _coordinator_mapper;
    ss::sharded<kafka::fetch_session_cache>& _fetch_session_cache;
    ss::sharded<cluster::id_allocator_frontend>& _id_allocator_frontend;
    bool _is_idempotence_enabled{false};
};

} // namespace kafka
