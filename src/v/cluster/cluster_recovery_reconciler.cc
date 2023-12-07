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
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "security/acl_store.h"
#include "security/credential_store.h"

#include <absl/container/flat_hash_map.h>

namespace {
// List of properties that are explicitly not recovered, since they would
// affect the cluster's ability to recover, or they are strongly coupled with
// the cluster's configuration.
//
// TODO: consider making this some annotation in the underlying properties so
// we can tell if the new cluster cares about the config.
const absl::flat_hash_set<ss::sstring> properties_ignore_list = {
  "cloud_storage_cache_size",
  "cluster_id",
  "cloud_storage_access_key",
  "cloud_storage_secret_key",
  "cloud_storage_region",
  "cloud_storage_bucket",
  "cloud_storage_api_endpoint",
  "cloud_storage_credentials_source",
  "cloud_storage_trust_file",
  "cloud_storage_backend",
  "cloud_storage_credentials_host",
  "cloud_storage_azure_storage_account",
  "cloud_storage_azure_container",
  "cloud_storage_azure_shared_key",
  "cloud_storage_azure_adls_endpoint",
  "cloud_storage_azure_adls_port",
};
} // anonymous namespace

namespace cluster::cloud_metadata {

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
    auto existing_license = _feature_table.get_license();
    if (
      cur_stage < recovery_stage::recovered_license && !existing_license
      && snap_license.has_value()) {
        // If there is already a license, it's presumably more up-to-date than
        // whatever is in a snapshot. Otherwise, try using the license.
        actions.license = snap.features.snap.license.value();
        actions.stages.emplace_back(recovery_stage::recovered_license);
    }

    const auto& snap_config = snap.config.values;
    if (cur_stage < recovery_stage::recovered_cluster_config) {
        for (const auto& [snap_k, snap_v] : snap_config) {
            if (properties_ignore_list.contains(snap_k)) {
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
        if (!actions.acls.empty()) {
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
