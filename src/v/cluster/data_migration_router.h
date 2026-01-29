/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/outcome.h"
#include "cluster/data_migration_group_proxy.h"
#include "cluster/data_migration_rpc_service.h"
#include "cluster/fwd.h"
#include "cluster/leader_router.h"
#include "cluster/offsets_snapshot.h"
#include "errc.h"
#include "model/fundamental.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

namespace cluster::data_migrations {

/*
 * This service routes calls from inside to outside of data migrations subsystem
 * based on partitions leader locations
 */
class router : public ss::peering_sharded_service<router> {
public:
    router(
      model::node_id,
      group_proxy&,
      ss::sharded<shard_table>&,
      ss::sharded<metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      ss::abort_source&);

    ss::future<> stop();

    ss::future<get_group_offsets_reply>
    get_group_offsets(get_group_offsets_request&& req);

    ss::future<set_group_offsets_reply>
    set_group_offsets(set_group_offsets_request&& req);

private:
    group_proxy& _group_proxy;

    struct get_group_offsets_handler {
        using proto_t = data_migrations_client_protocol;
        static ss::sstring process_name() { return "get group offsets"; }
        static get_group_offsets_reply error_resp(cluster::errc e);

        static ss::future<result<rpc::client_context<get_group_offsets_reply>>>
        dispatch(
          proto_t proto,
          get_group_offsets_request req,
          model::timeout_clock::duration timeout);

        ss::future<get_group_offsets_reply>
        process(ss::shard_id shard, get_group_offsets_request req);

        router& parent;
    };
    get_group_offsets_handler _get_group_offsets_handler;

    leader_router<
      get_group_offsets_request,
      get_group_offsets_reply,
      get_group_offsets_handler>
      _get_group_offsets_router;
    struct set_group_offsets_handler {
        using proto_t = data_migrations_client_protocol;
        static ss::sstring process_name() { return "set group offsets"; }
        static set_group_offsets_reply error_resp(cluster::errc e);

        static ss::future<result<rpc::client_context<set_group_offsets_reply>>>
        dispatch(
          proto_t proto,
          set_group_offsets_request req,
          model::timeout_clock::duration timeout);

        ss::future<set_group_offsets_reply>
        process(ss::shard_id shard, set_group_offsets_request req);

        router& parent;
    };
    set_group_offsets_handler _set_group_offsets_handler;

    leader_router<
      set_group_offsets_request,
      set_group_offsets_reply,
      set_group_offsets_handler>
      _set_group_offsets_router;

    ss::optimized_optional<ss::abort_source::subscription> _as_sub;
};

} // namespace cluster::data_migrations
