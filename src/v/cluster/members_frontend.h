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

#include "cluster/controller_stm.h"
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
      ss::sharded<ss::abort_source>&);

    ss::future<> start();
    ss::future<> stop();

    ss::future<std::error_code> decommission_node(model::node_id);
    ss::future<std::error_code> recommission_node(model::node_id);
    ss::future<std::error_code> finish_node_reallocations(model::node_id);

private:
    template<typename T>
    ss::future<std::error_code> do_replicate_node_command(model::node_id id) {
        return _stm.invoke_on(
          controller_stm_shard, [id, this](controller_stm& stm) {
              T cmd(id, 0);
              return serialize_cmd(cmd).then(
                [this, &stm](model::record_batch b) {
                    return stm.replicate_and_wait(
                      std::move(b),
                      _node_op_timeout + model::timeout_clock::now(),
                      _as.local());
                });
          });
    }
    model::node_id _self;
    std::chrono::milliseconds _node_op_timeout;
    ss::sharded<controller_stm>& _stm;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<ss::abort_source>& _as;
};
} // namespace cluster
