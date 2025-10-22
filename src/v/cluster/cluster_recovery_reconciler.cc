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

#include "cluster/cluster_recovery_reconciler.h"

#include "cluster/cluster_recovery_table.h"
#include "cluster/topic_table.h"
#include "config/config_store.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "security/acl_store.h"
#include "security/credential_store.h"

namespace cluster::cloud_metadata {

chunked_hash_set<ss::sstring>
controller_snapshot_reconciler::properties_ignore_list() {
    chunked_hash_set<ss::sstring> ignore_list;
    config::shard_local_cfg().for_each(
      [&ignore_list](const config::base_property& prop) {
          if (!prop.gets_restored()) {
              ignore_list.emplace(prop.name());
              for (const auto& alias : prop.aliases()) {
                  ignore_list.emplace(alias);
              }
          }
      });
    return ignore_list;
}

controller_snapshot_reconciler::controller_actions
controller_snapshot_reconciler::get_actions(
  cluster::controller_snapshot& snap) const {
    // If recovery isn't in progress, return empty.
    auto cur_stage = _recovery_table.current_status().value_or(
      recovery_stage::initialized);
    if (!may_require_controller_recovery(cur_stage)) {
        return {};
    }

    controller_actions actions;
    const auto& snap_license = snap.features.snap.license;
    auto existing_license = _feature_table.get_configured_license();
    if (
      cur_stage < recovery_stage::recovered_license && !existing_license
      && snap_license.has_value()) {
        // If there is already a license, it's presumably more up-to-date than
        // whatever is in a snapshot. Otherwise, try using the license.
        actions.license = snap.features.snap.license.value();
        actions.stages.emplace_back(recovery_stage::recovered_license);
    }

    const auto& snap_config = snap.config.values;
    auto ignore_list = properties_ignore_list();
    if (cur_stage < recovery_stage::recovered_cluster_config) {
        for (const auto& [snap_k, snap_v] : snap_config) {
            if (ignore_list.contains(snap_k)) {
                continue;
            }
            if (!config::shard_local_cfg().contains(snap_k)) {
                continue;
            }
            actions.config.upsert.emplace_back(
              cluster_property_kv{snap_k, snap_v});
        }
        if (!actions.config.upsert.empty()) {
            actions.stages.emplace_back(
              recovery_stage::recovered_cluster_config);
        }
    }

    if (cur_stage < recovery_stage::recovered_users) {
        const auto& snap_user_creds = snap.security.user_credentials;
        for (const auto& user : snap_user_creds) {
            if (!_creds.contains(user.username)) {
                actions.users.emplace_back(user.username, user.credential);
            }
        }
        if (!actions.users.empty()) {
            actions.stages.emplace_back(recovery_stage::recovered_users);
        }
    }

    if (cur_stage < recovery_stage::recovered_acls) {
        const auto& snap_acls = snap.security.acls;
        for (const auto& binding : snap_acls) {
            // TODO: filter those that exist. For now, just pass in everything
            // since this is idempotent.
            actions.acls.emplace_back(binding);
        }

        // Role recovery is bundled within ACL recovery stage in order to
        // avoid adding a new recovery stage enum that isn't backportable.
        for (const auto& snap_role : snap.security.roles) {
            if (!_roles.contains(snap_role.name)) {
                vlog(
                  clusterlog.debug,
                  "Role {} does not exist, creating from snapshot",
                  snap_role.name);
                actions.roles.emplace_back(snap_role.name, snap_role.role);
            }
        }

        if (!actions.acls.empty() || !actions.roles.empty()) {
            actions.stages.emplace_back(recovery_stage::recovered_acls);
        }
    }

    if (cur_stage < recovery_stage::recovered_topic_data) {
        const auto& snap_tables = snap.topics.topics;
        for (const auto& [tp_ns, meta] : snap_tables) {
            const auto& tp_config = meta.metadata.configuration;
            auto& si_props = tp_config.properties.shadow_indexing;
            if (_topic_table.contains(tp_ns)) {
                continue;
            }
            if (tp_config.is_internal()) {
                vlog(
                  clusterlog.debug,
                  "Skipping topic recovery for internal topic {}",
                  tp_ns);
                continue;
            }
            if (
              si_props.has_value()
              && model::is_archival_enabled(si_props.value())) {
                // We expect to create the topic with tiered storage data.
                auto new_config = tp_config;
                if (!new_config.properties.remote_topic_properties
                       .has_value()) {
                    auto& remote_props
                      = new_config.properties.remote_topic_properties.emplace();
                    remote_props.remote_revision = model::initial_revision_id{
                      meta.metadata.revision};
                    remote_props.remote_partition_count
                      = tp_config.partition_count;
                }
                new_config.properties.recovery = true;
                actions.remote_topics.emplace_back(std::move(new_config));
                continue;
            };
            // Either this is a read replica or no metadata is expected to exist
            // in tiered storage. Just create the topic.
            actions.local_topics.emplace_back(tp_config);
        }
        if (!actions.remote_topics.empty()) {
            actions.stages.emplace_back(
              recovery_stage::recovered_remote_topic_data);
        }
        if (!actions.local_topics.empty()) {
            actions.stages.emplace_back(recovery_stage::recovered_topic_data);
        }
    }

    // Always include this final stage, indicating the end of the recovering of
    // the controller snapshot. That way, if we start reconciling while at or
    // past this state, we don't need to redownload the controller snapshot.
    actions.stages.emplace_back(recovery_stage::recovered_controller_snapshot);
    return actions;
}

} // namespace cluster::cloud_metadata
