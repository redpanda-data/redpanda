// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cluster/client_quota_serde.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "rpc/fwd.h"

#include <seastar/core/sharded.hh>

namespace cluster::client_quota {

class frontend final {
public:
    frontend(
      model::node_id self,
      ss::sharded<controller_stm>& stm,
      ss::sharded<rpc::connection_cache>& connections,
      ss::sharded<partition_leaders_table>& leaders,
      ss::sharded<ss::abort_source>& as)
      : _self{self}
      , _stm{stm}
      , _connections{connections}
      , _leaders{leaders}
      , _as{as} {}

    /// Applies the given delta to the client quota store
    /// Should be called ONLY on the controller leader node, but may be called
    /// from any shard
    /// Returns errc::success on success or cluster errors on failure
    /// There are no client quota store-specific errors, because the upsert and
    /// remove operations always succeed regardless of the previous state of the
    /// given quota (eg. there is no error for removing non-existent quotas).
    ss::future<cluster::errc>
      alter_quotas(alter_delta_cmd_data, model::timeout_clock::time_point);

private:
    // Dispatch the request to `node_id`, the controller leader.
    ss::future<cluster::errc> dispatch_alter_to_remote(
      model::node_id, alter_delta_cmd_data, model::timeout_clock::duration);

    // Perform the alter locally (assumes the node is the controller leader)
    ss::future<cluster::errc>
      do_alter_quotas(alter_delta_cmd_data, model::timeout_clock::time_point);

    model::node_id _self;
    ss::sharded<controller_stm>& _stm;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<ss::abort_source>& _as;
};

} // namespace cluster::client_quota
