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
#include "cluster/data_migration_types.h"
#include "cluster/fwd.h"
#include "features/fwd.h"
#include "rpc/fwd.h"
#include "ssx/single_sharded.h"

#include <seastar/core/sharded.hh>

namespace cluster::data_migrations {

class frontend : public ss::peering_sharded_service<frontend> {
public:
    using can_dispatch_to_leader = ss::bool_class<struct allow_redirect_tag>;

    frontend(
      model::node_id,
      bool,
      ssx::single_sharded<migrations_table>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<controller_stm>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<ss::abort_source>&);

    ss::future<result<id>> create_migration(
      data_migration migration,
      can_dispatch_to_leader dispatch = can_dispatch_to_leader::yes);

    ss::future<std::error_code> update_migration_state(
      id, state, can_dispatch_to_leader dispatch = can_dispatch_to_leader::yes);

    ss::future<std::error_code> remove_migration(
      id, can_dispatch_to_leader dispatch = can_dispatch_to_leader::yes);

    ss::future<check_ntp_states_reply> check_ntp_states_on_foreign_node(
      model::node_id node, check_ntp_states_request&& req);

    ss::future<result<migration_metadata>> get_migration(id);
    ss::future<chunked_vector<migration_metadata>> list_migrations();

private:
    /**
     * Must be executed on data migrations shard
     */
    ss::future<result<id>> do_create_migration(data_migration);
    ss::future<std::error_code> do_update_migration_state(id, state);
    ss::future<std::error_code> do_remove_migration(id);

    ss::future<std::error_code> insert_barrier();

    template<
      typename Request,
      typename Reply,
      typename DispatchFunc,
      typename ProcessFunc,
      typename ReplyMapperFunc>
    ss::future<std::invoke_result_t<ReplyMapperFunc, result<Reply>>>
      process_or_dispatch(
        Request,
        can_dispatch_to_leader,
        DispatchFunc,
        ProcessFunc,
        ReplyMapperFunc);

    inline void validate_migration_shard() const {
        vassert(
          ss::this_shard_id() == data_migrations_shard,
          "This method can only be called on data migration shard");
    }

    bool data_migrations_active() const;

private:
    model::node_id _self;
    bool _cloud_storage_api_initialized;
    ssx::single_sharded<migrations_table>& _table;
    ss::sharded<features::feature_table>& _features;
    ss::sharded<controller_stm>& _controller;
    ss::sharded<partition_leaders_table>& _leaders_table;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<ss::abort_source>& _as;
    std::chrono::milliseconds _operation_timeout;
};

} // namespace cluster::data_migrations
