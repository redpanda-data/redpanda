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

// Defines the order in which the recovery stages should be evaluated. This
// should match the order of generated actions in get_actions().
//
// NOTE: unlike the enum values themselves, this is not persisted, and can be
// used to add new enum values (which must be added to the end) that are
// executed out of enum value order.
int order(recovery_stage s) {
    using enum recovery_stage;
    switch (s) {
    case initialized:
        return 0;
    case starting:
        return 1;
    case recovered_license:
        return 2;
    case recovered_cluster_config:
        return 3;
    case recovered_users:
        return 4;
    case recovered_acls:
        return 5;
    case recovered_cloud_topics_metastore:
        return 6;
    case recovered_cloud_topic_data:
        return 7;
    case recovered_remote_topic_data:
        return 8;
    case recovered_topic_data:
        return 9;
    case recovered_controller_snapshot:
        return 10;
    case recovered_offsets_topic:
        return 11;
    case recovered_tx_coordinator:
        return 12;
    case complete:
        return 100;
    case failed:
        return 101;
    }
}

// Returns whether, if currently in the given cur_stage, whether we still need
// actions for the given target stage.
bool needs_actions(recovery_stage cur_stage, recovery_stage target) {
    return order(cur_stage) < order(target);
}

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
      needs_actions(cur_stage, recovery_stage::recovered_license)
      && !existing_license && snap_license.has_value()) {
        // If there is already a license, it's presumably more up-to-date than
        // whatever is in a snapshot. Otherwise, try using the license.
        actions.license = snap.features.snap.license.value();
        actions.stages.emplace_back(recovery_stage::recovered_license);
    }

    const auto& snap_config = snap.config.values;
    auto ignore_list = properties_ignore_list();
    if (needs_actions(cur_stage, recovery_stage::recovered_cluster_config)) {
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

    if (needs_actions(cur_stage, recovery_stage::recovered_users)) {
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

    if (needs_actions(cur_stage, recovery_stage::recovered_acls)) {
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

    if (
      needs_actions(
        cur_stage, recovery_stage::recovered_cloud_topics_metastore)) {
        const auto& snap_tables = snap.topics.topics;
        auto snap_it = snap_tables.find(model::l1_metastore_nt);
        if (snap_it != snap_tables.end()) {
            auto& snap_conf = snap_it->second.metadata.configuration;
            auto cur_conf = _topic_table.get_topic_cfg(model::l1_metastore_nt);
            // If the topic exists but with a different remote label, it's
            // possible we can reset it (e.g. if it's empty).
            if (
              !cur_conf
              || cur_conf->properties.remote_label
                   != snap_conf.properties.remote_label) {
                actions.ct_metastore_topic = snap_conf;
            }
        }
        if (actions.ct_metastore_topic.has_value()) {
            actions.stages.emplace_back(
              recovery_stage::recovered_cloud_topics_metastore);
        }
    }

    if (needs_actions(cur_stage, recovery_stage::recovered_topic_data)) {
        const auto& snap_tables = snap.topics.topics;
        for (const auto& [tp_ns, meta] : snap_tables) {
            const auto& tp_config = meta.metadata.configuration;
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
            if (tp_config.is_cloud_topic()) {
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
                actions.cloud_topics.emplace_back(std::move(new_config));
                continue;
            }

            if (tp_config.properties.is_archival_enabled()) {
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
        if (!actions.cloud_topics.empty()) {
            actions.stages.emplace_back(
              recovery_stage::recovered_cloud_topic_data);
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
