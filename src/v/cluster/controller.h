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

#include "cluster/controller_backend.h"
#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/members_manager.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/topic_updates_dispatcher.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "rpc/connection_cache.h"
#include "storage/api.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <vector>

namespace cluster {
class controller {
public:
    controller(
      ss::sharded<rpc::connection_cache>& ccache,
      ss::sharded<partition_manager>& pm,
      ss::sharded<shard_table>& st,
      ss::sharded<storage::api>& storage);

    model::node_id self() { return _raft0->self().id(); }
    ss::sharded<topics_frontend>& get_topics_frontend() { return _tp_frontend; }
    ss::sharded<members_manager>& get_members_manager() {
        return _members_manager;
    }

    ss::sharded<members_table>& get_members_table() { return _members_table; }
    ss::sharded<topic_table>& get_topics_state() { return _tp_state; }
    ss::sharded<partition_leaders_table>& get_partition_leaders() {
        return _partition_leaders;
    }

    ss::future<> wire_up();

    ss::future<> start();
    ss::future<> stop();

private:
    ss::sharded<ss::abort_source> _as;                     // instance per core
    ss::sharded<partition_allocator> _partition_allocator; // single instance
    ss::sharded<topic_table> _tp_state;                    // instance per core
    ss::sharded<members_table> _members_table;             // instance per core
    ss::sharded<partition_leaders_table>
      _partition_leaders;                          // instance per core
    ss::sharded<members_manager> _members_manager; // single instance
    ss::sharded<topics_frontend> _tp_frontend;     // instance per core
    ss::sharded<controller_backend> _backend;      // instance per core
    ss::sharded<controller_stm> _stm;              // single instance
    ss::sharded<controller_service> _service;      // instance per core
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_manager>& _partition_manager;
    ss::sharded<shard_table>& _shard_table;
    ss::sharded<storage::api>& _storage;
    topic_updates_dispatcher _tp_updates_dispatcher;
    consensus_ptr _raft0;
};

} // namespace cluster
