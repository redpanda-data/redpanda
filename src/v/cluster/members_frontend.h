/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/cluster_utils.h"
#include "cluster/controller_stm.h"
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"

namespace cluster {

/**
 * Members frontend is an entry point for all operations related with cluster
 * management like, nodes decommissioning, nodes additions etc.
 *
 * NOTE:
 *
 * currently some of the nodes operation are handeld in members manager however
 * we are going to migrate those to members frontend to be consistent with the
 * way how we manage other cluster wide resources
 */
class members_frontend {
public:
    members_frontend(
      ss::sharded<controller_stm>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<ss::abort_source>&);

    ss::future<std::error_code> decommission_node(model::node_id);
    ss::future<std::error_code> recommission_node(model::node_id);
    ss::future<std::error_code> finish_node_reallocations(model::node_id);
    ss::future<std::error_code> remove_node(model::node_id);

    ss::future<std::error_code>
    set_maintenance_mode(model::node_id, bool enabled);

private:
    template<typename T>
    ss::future<std::error_code> do_replicate_node_command(model::node_id id) {
        return replicate_and_wait(
          _stm,
          _feature_table,
          _as,
          T(id, 0),
          _node_op_timeout + model::timeout_clock::now());
    }

    model::node_id _self;
    std::chrono::milliseconds _node_op_timeout;
    ss::sharded<controller_stm>& _stm;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<ss::abort_source>& _as;
};
} // namespace cluster
