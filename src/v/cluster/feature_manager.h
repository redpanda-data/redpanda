/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "cluster/feature_barrier.h"
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "security/fwd.h"

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
      ss::sharded<features::feature_table>& table,
      ss::sharded<rpc::connection_cache>& connection_cache,
      ss::sharded<security::role_store>& role_store,
      raft::group_id raft0_group);

    /**
     * \param cluster_founder_nodes When a new cluster is about to bootstrap,
     *    this should list all cluster founder nodes. It is assumed that all
     *    cluster founder nodes are verified to have the same logical version
     *    as the local node. In all other cases this list will be empty.
     */
    ss::future<> start(std::vector<model::node_id>&& cluster_founder_nodes);
    ss::future<> stop();

    ss::future<std::error_code> write_action(cluster::feature_update_action);

    /**
     * If you know a barrier must be over, e.g. because feature has already
     * proceeded to active state, then register that here so that any peers
     * trying to enter the barrier will skip it too.
     */
    void exit_barrier(feature_barrier_tag tag) {
        _barrier_state.exit_barrier(tag);
    }

    feature_barrier_response
    update_barrier(feature_barrier_tag tag, model::node_id peer, bool entered) {
        return _barrier_state.update_barrier(tag, peer, entered);
    }

    ss::future<> barrier(feature_barrier_tag tag) {
        return _barrier_state.barrier(tag);
    }

    ss::future<std::error_code> update_license(security::license&& license);

private:
    void update_node_version(model::node_id, cluster_version v);

    /**
     * When cluster discovery has verified seed server latest versions are
     * the same as local latest version, this function is used to initialize
     * seed servers versions in feature manager.
     */
    void set_node_to_latest_version(model::node_id);

    /// Background loop body: calls the functions that may update the
    /// active version and/or activate features.
    ss::future<> maybe_update_feature_table();

    /// Consume _updates and evaluate whether the cluster version may advance
    ss::future<> do_maybe_update_active_version();

    /// Check _feature_table for any features that are elegible to auto
    /// activate but not yet active.
    ss::future<> do_maybe_activate_features();

    /// Whether there is work to do in maybe_update_feature_table
    bool updates_pending() {
        return (!_updates.empty()
                || !auto_activate_features(
                      _feature_table.local().get_original_version(),
                      _feature_table.local().get_active_version())
                      .empty())
               && _am_controller_leader;
    }

    ss::future<> maybe_log_license_check_info();

    // Compose a command struct, replicate it via raft and wait for apply.
    // Silently swallow not_leader errors, raise on other errors;
    ss::future<> replicate_feature_update_cmd(feature_update_cmd_data data);

    // Discover features that may now be auto-activated: usually this happens
    // when we activate a new logical version, but it may also happen if we
    // upgraded Redpanda to a binary in the same logical version with a
    // different activation policy for the feature.
    std::vector<std::reference_wrapper<const features::feature_spec>>
      auto_activate_features(cluster_version, cluster_version);

    // This method returns true if there are any feature(s) enabled that require
    // the enterprise license.  Currently the following features require a
    // license:
    // +-------------+---------------------------------+---------------+
    // | Config Type | Config Name                     | Value(s)      |
    // +-------------+---------------------------------+---------------+
    // | Cluster     | `audit_enabled`                 | `true`        |
    // | Cluster     | `cloud_storage_enabled`         | `true`        |
    // | Cluster     | `partition_auto_balancing_mode` | `continuous`  |
    // | Cluster     | `core_balancing_continous`      | `true`        |
    // | Cluster     | `sasl_mechanisms`               | `GSSAPI`      |
    // | Cluster     | `sasl_mechanisms`               | `OAUTHBEARER` |
    // | Cluster     | `http_authentication`           | `OIDC`        |
    // | Cluster     | `enable_schema_id_validation`   | `redpanda`    |
    // | Cluster     | `enable_schema_id_validation`   | `compat`      |
    // +-------------+---------------------------------+---------------+
    //
    // Also if there are any non default roles in the role store.
    bool license_required_feature_enabled() const;

    ss::sharded<controller_stm>& _stm;
    ss::sharded<ss::abort_source>& _as;
    ss::gate _gate;
    ss::sharded<members_table>& _members;
    ss::sharded<raft::group_manager>& _group_manager;
    ss::sharded<health_monitor_frontend>& _hm_frontend;
    ss::sharded<health_monitor_backend>& _hm_backend;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<security::role_store>& _role_store;
    raft::group_id _raft0_group;

    version_map _updates;
    ss::condition_variable _update_wait;

    cluster::notification_id_type _leader_notify_handle{
      notification_id_type_invalid};
    cluster::notification_id_type _health_notify_handle{
      notification_id_type_invalid};

    // Barriers are only populated on shard 0
    feature_barrier_state<ss::lowres_clock> _barrier_state;

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
