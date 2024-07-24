/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/seastarx.h"
#include "cloud_storage/fwd.h"
#include "cluster/cluster_recovery_reconciler.h"
#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "features/fwd.h"
#include "model/fundamental.h"
#include "raft/fwd.h"
#include "raft/notification.h"
#include "security/fwd.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>

namespace cluster::cloud_metadata {

class offsets_recovery_requestor;

class cluster_recovery_backend {
public:
    cluster_recovery_backend(
      cluster::cluster_recovery_manager&,
      raft::group_manager&,
      cloud_storage::remote&,
      cloud_storage::cache&,
      cluster::members_table&,
      features::feature_table&,
      security::credential_store&,
      cluster::topic_table&,
      cluster::controller_api&,
      cluster::feature_manager&,
      cluster::config_frontend&,
      cluster::security_frontend&,
      cluster::topics_frontend&,
      ss::shared_ptr<producer_id_recovery_manager> producer_id_recovery,
      ss::shared_ptr<offsets_recovery_requestor> offsets_recovery,
      ss::sharded<cluster_recovery_table>&,
      consensus_ptr raft0);

    void start();
    ss::future<> stop_and_wait();

    // Repeatedly attempts to wait for and perform recovery as leader.
    ss::future<> recover_until_abort();

    // Runs through recovery for as long as this node is still leader.
    ss::future<> recover_until_term_change();

private:
    // Syncs the leader in the given term, ensuring it is still leader.
    // Returns false if not, or if no recovery is active.
    ss::future<bool> sync_in_term(ss::abort_source& term_as, model::term_id);

    ss::future<cluster::errc> apply_controller_actions_in_term(
      ss::abort_source& term_as,
      model::term_id,
      cloud_metadata::controller_snapshot_reconciler::controller_actions);

    // Runs the action to get to the given stage.
    ss::future<cluster::errc> do_action(
      ss::abort_source& term_as,
      recovery_stage next_stage,
      controller_snapshot_reconciler::controller_actions& actions);

    // Looks into the bucket for the latest cluster metadata that refers to a
    // controller snapshot, parsing it if one exists. Returns std::nullopt if
    // none exists or if there was an error along the way.
    ss::future<std::optional<cluster::controller_snapshot>>
    find_controller_snapshot_in_bucket(
      ss::abort_source& term_as, cloud_storage_clients::bucket_name);

    ss::abort_source _as;
    ss::gate _gate;

    ss::condition_variable _leader_cond;
    raft::group_manager_notification_id _leader_cb_id{
      notification_id_type_invalid};

    // Abort source to stop waiting if there is a term change.
    std::optional<std::reference_wrapper<ss::abort_source>> _term_as;

    cluster::cluster_recovery_manager& _recovery_manager;
    raft::group_manager& _raft_group_manager;

    // Remote with which to download recovery materials.
    cloud_storage::remote& _remote;
    cloud_storage::cache& _cache;

    // Controller state for the current cluster.
    cluster::members_table& _members_table;
    features::feature_table& _features;
    security::credential_store& _creds;
    cluster::topic_table& _topics;
    cluster::controller_api& _controller_api;

    // Abstractions that drive replicated changes to controller state.
    cluster::feature_manager& _feature_manager;
    cluster::config_frontend& _config_frontend;
    cluster::security_frontend& _security_frontend;
    cluster::topics_frontend& _topics_frontend;

    ss::shared_ptr<producer_id_recovery_manager> _producer_id_recovery;
    ss::shared_ptr<offsets_recovery_requestor> _offsets_recovery;

    // State that backs the recoveries managed by this manager. Sharded so that
    // the status of the controller recovery is propagated across cores.
    ss::sharded<cluster_recovery_table>& _recovery_table;

    consensus_ptr _raft0;
};

} // namespace cluster::cloud_metadata
