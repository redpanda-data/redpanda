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
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

namespace cluster {

using version_map = std::map<model::node_id, cluster_version>;

/**
 * Versions and features:
 *  - Feature flags are useful for internal and external consumers
 *    that only care about whether a particular piece of functionality
 *    is active, and don't want to reason about which versions have it.
 *  - The version is useful for protocol changes that don't necessarily
 *    correspond to a feature, but are needed to safely read or write
 *    persistent structures in the controller log.
 *
 *  The persistence of the cluster version is also useful for marking
 *  in the controller log where the upgrade was finalized: in a disaster
 *  if we need to snip the controller log back to something an old
 *  version can read, that would be the offset at which to snip.
 *
 *  For the detail of how individual features are enabled, consult
 *  the definition of `feature_state`.
 */

/**
 * The feature manager keeps a persistent record of the logical
 * versions of nodes, and thereby which features are available
 * for use on the cluster.
 *
 * This is needed because in general new features can't be safely
 * used until all nodes in the cluster are up to date.
 */
class feature_manager {
public:
    static constexpr ss::shard_id backend_shard = controller_stm_shard;

    feature_manager(
      ss::sharded<controller_stm>& stm,
      ss::sharded<ss::abort_source>& as,
      ss::sharded<members_table>& members,
      ss::sharded<raft::group_manager>& group_manager,
      ss::sharded<health_monitor_frontend>& hm_frontend,
      ss::sharded<health_monitor_backend>& hm_backend,
      ss::sharded<feature_table>& table,
      raft::group_id raft0_group)
      : _stm(stm)
      , _as(as)
      , _members(members)
      , _group_manager(group_manager)
      , _hm_frontend(hm_frontend)
      , _hm_backend(hm_backend)
      , _feature_table(table)
      , _raft0_group(raft0_group) {}

    ss::future<> start();
    ss::future<> stop();

    ss::future<std::error_code> write_action(cluster::feature_update_action);

private:
    void update_node_version(model::node_id, cluster_version v);

    ss::future<> do_maybe_update_active_version();
    ss::future<> maybe_update_active_version();

    ss::sharded<controller_stm>& _stm;
    ss::sharded<ss::abort_source>& _as;
    ss::gate _gate;
    ss::sharded<members_table>& _members;
    ss::sharded<raft::group_manager>& _group_manager;
    ss::sharded<health_monitor_frontend>& _hm_frontend;
    ss::sharded<health_monitor_backend>& _hm_backend;
    ss::sharded<feature_table>& _feature_table;
    raft::group_id _raft0_group;

    std::vector<std::pair<model::node_id, cluster_version>> _updates;
    ss::condition_variable _update_wait;

    cluster::notification_id_type _leader_notify_handle{
      notification_id_type_invalid};
    cluster::notification_id_type _health_notify_handle{
      notification_id_type_invalid};

    // The individually reported versions of nodes, which
    // are usually all the same but will differ during upgrades.
    // This is not persistent: populated from health reports on
    // the controller leader.
    version_map _node_versions;

    // Keep track of whether this node is the controller leader
    // via leadership notifications
    bool _am_controller_leader{false};
};

} // namespace cluster