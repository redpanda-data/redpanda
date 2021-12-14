/*
 * Copyright 2021 Vectorized, Inc.
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

namespace cluster {

class config_frontend final {
public:
    // Shard ID that will track the next available version and serialize
    // writes to hand out sequential versions.
    // Setting this to the same as controller_stm shard simplfies implementation
    // by letting all state machine appliers assume they are running
    // on the same shard that should keep its version state up to date.
    static constexpr ss::shard_id version_shard = cluster::controller_stm_shard;

    config_frontend(
      ss::sharded<controller_stm>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<ss::abort_source>&);

    ss::future<std::error_code>
    patch(config_update_request&&, model::timeout_clock::time_point);

    ss::future<std::error_code>
    do_patch(config_update_request&&, model::timeout_clock::time_point);

    ss::future<std::error_code>
    set_status(config_status&, model::timeout_clock::time_point);

    ss::future<> start();
    ss::future<> stop();

    void set_next_version(config_version v);

private:
    ss::sharded<controller_stm>& _stm;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<ss::abort_source>& _as;

    // Initially unset, frontend is not writeable until backend finishes
    // init and calls set_next_version.
    config_version _next_version{
      config_version_unset}; // Only maintained on `version_shard`

    // Serialize writes to generate version numbers.
    mutex _write_lock;
};
} // namespace cluster
