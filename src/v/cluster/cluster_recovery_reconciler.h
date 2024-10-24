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

#include "cluster/cluster_recovery_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "features/feature_table.h"
#include "security/credential_store.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {
struct user_credential {
    security::credential_user user;
    security::scram_credential cred;
};
} // namespace cluster

namespace cluster::cloud_metadata {

// Determines the additive difference between the given snapshot and the
// current cluster.
//
// Does not take into account leadership when computing actions. Callers should
// be mindful of this and ensure externally that the controller subsystems are
// up-to-date when computing reconciliation actions.
class controller_snapshot_reconciler {
public:
    // Actions that may be performed without additional context.
    struct controller_actions {
        std::optional<security::license> license;
        config_update_request config;
        fragmented_vector<cluster::user_credential> users;
        fragmented_vector<security::acl_binding> acls;
        fragmented_vector<topic_configuration> remote_topics;
        fragmented_vector<topic_configuration> local_topics;
        // TODO: restore wasm plugins/transforms

        bool empty() const {
            return !license.has_value() && config.upsert.empty()
                   && users.empty() && acls.empty() && remote_topics.empty()
                   && local_topics.empty();
        }

        // The set of recovery stages that should be walked through to
        // accomplish the actions.
        std::vector<recovery_stage> stages;
    };

    controller_snapshot_reconciler(
      cluster::cluster_recovery_table& recovery,
      features::feature_table& features,
      security::credential_store& creds,
      cluster::topic_table& topics)
      : _recovery_table(recovery)
      , _feature_table(features)
      , _creds(creds)
      , _topic_table(topics) {}

    // Returns the set of actions to perform to get to the state of 'snap'.
    //
    // Does not compute updates to existing states, only whether new state
    // should be added (e.g. if table "foo" exists but is different from that
    // in the snapshot, it isn't reported as an action).
    controller_actions get_actions(cluster::controller_snapshot& snap) const;

private:
    cluster::cluster_recovery_table& _recovery_table;
    features::feature_table& _feature_table;
    security::credential_store& _creds;
    cluster::topic_table& _topic_table;
};

} // namespace cluster::cloud_metadata
