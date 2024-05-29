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

#include "cluster/controller_backend.h"
#include "cluster/shard_placement_table.h"
#include "container/chunked_hash_map.h"
#include "ssx/event.h"

namespace cluster {

/// shard_balancer runs on shard 0 of each node and manages assignments of
/// partitions hosted on this node to shards. It does this by modifying
/// shard_placement_table and notifying controller_backend of these modification
/// so that it can perform necessary reconciling actions.
///
/// Currently shard_balancer simply uses assignments from topic_table, but in
/// the future it will calculate them on its own.
class shard_balancer {
public:
    // single instance
    static constexpr ss::shard_id shard_id = 0;

    shard_balancer(
      ss::sharded<shard_placement_table>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<topic_table>&,
      ss::sharded<controller_backend>&);

    ss::future<> start();
    ss::future<> stop();

    /// Persist current shard_placement_table contents to kvstore. Executed once
    /// when enabling the node_local_core_assignment feature (assumes that it is
    /// in the "preparing" state).
    ss::future<> enable_persistence();

private:
    void process_delta(const topic_table::delta&);

    ss::future<> assign_fiber();
    ss::future<> do_assign_ntps();
    ss::future<> assign_ntp(const model::ntp&);

private:
    shard_placement_table& _shard_placement;
    features::feature_table& _features;
    ss::sharded<topic_table>& _topics;
    ss::sharded<controller_backend>& _controller_backend;
    model::node_id _self;

    cluster::notification_id_type _topic_table_notify_handle;

    chunked_hash_set<model::ntp> _to_assign;
    ssx::event _wakeup_event{"shard_balancer"};
    ss::gate _gate;
};

} // namespace cluster
