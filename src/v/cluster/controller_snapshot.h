/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/client_quota_serde.h"
#include "cluster/cluster_recovery_state.h"
#include "cluster/data_migration_types.h"
#include "cluster/types.h"
#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"
#include "features/feature_table_snapshot.h"
#include "security/role.h"
#include "security/types.h"
#include "serde/envelope.h"

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>

namespace cluster {

namespace controller_snapshot_parts {

struct bootstrap_t
  : public serde::
      envelope<bootstrap_t, serde::version<0>, serde::compat_version<0>> {
    std::optional<model::cluster_uuid> cluster_uuid;

    friend bool operator==(const bootstrap_t&, const bootstrap_t&) = default;

    auto serde_fields() { return std::tie(cluster_uuid); }
};

struct features_t
  : public serde::
      envelope<features_t, serde::version<0>, serde::compat_version<0>> {
    features::feature_table_snapshot snap;

    friend bool operator==(const features_t&, const features_t&) = default;

    auto serde_fields() { return std::tie(snap); }
};

struct members_t
  : public serde::
      envelope<members_t, serde::version<2>, serde::compat_version<0>> {
    struct node_t
      : serde::envelope<node_t, serde::version<0>, serde::compat_version<0>> {
        model::broker broker;
        broker_state state;

        friend bool operator==(const node_t&, const node_t&) = default;

        auto serde_fields() { return std::tie(broker, state); }
    };

    struct update_t
      : serde::envelope<update_t, serde::version<0>, serde::compat_version<0>> {
        node_update_type type;
        model::offset offset;
        std::optional<model::revision_id> decommission_update_revision;

        friend bool operator==(const update_t&, const update_t&) = default;

        auto serde_fields() {
            return std::tie(type, offset, decommission_update_revision);
        }
    };

    absl::flat_hash_map<model::node_uuid, model::node_id> node_ids_by_uuid;
    model::node_id next_assigned_id;

    absl::node_hash_map<model::node_id, node_t> nodes;
    absl::node_hash_map<model::node_id, node_t> removed_nodes;
    absl::flat_hash_set<model::node_id> removed_nodes_still_in_raft0;
    absl::node_hash_map<model::node_id, update_t> in_progress_updates;

    model::offset first_node_operation_command_offset;
    // revision of metadata table (offset of last applied cluster member
    // command)
    model::revision_id version{};

    friend bool operator==(const members_t&, const members_t&) = default;

    auto serde_fields() {
        return std::tie(
          node_ids_by_uuid,
          next_assigned_id,
          nodes,
          removed_nodes,
          removed_nodes_still_in_raft0,
          in_progress_updates,
          first_node_operation_command_offset,
          version);
    }
};

struct config_t
  : public serde::
      envelope<config_t, serde::version<0>, serde::compat_version<0>> {
    config_version version;
    absl::btree_map<ss::sstring, ss::sstring> values;
    fragmented_vector<config_status> nodes_status;

    friend bool operator==(const config_t&, const config_t&) = default;

    auto serde_fields() { return std::tie(version, values, nodes_status); }
};

struct topics_t
  : public serde::
      envelope<topics_t, serde::version<1>, serde::compat_version<0>> {
    // NOTE: layout here is a bit different than in the topic table because it
    // allows more compact storage and more convenient generation of controller
    // backend deltas when applying the snapshot.
    struct partition_t
      : public serde::
          envelope<partition_t, serde::version<0>, serde::compat_version<0>> {
        raft::group_id group;
        /// NOTE: in contrast to topic_table does NOT reflect the result of
        /// current in-progress update.
        std::vector<model::broker_shard> replicas;
        /// Also does not reflect the current in-progress update.
        replicas_revision_map replicas_revisions;
        model::revision_id last_update_finished_revision;

        friend bool operator==(const partition_t&, const partition_t&)
          = default;

        auto serde_fields() {
            return std::tie(
              group,
              replicas,
              replicas_revisions,
              last_update_finished_revision);
        }
    };

    struct update_t
      : public serde::
          envelope<update_t, serde::version<1>, serde::compat_version<0>> {
        /// NOTE: In the event of cancellation this remains the original target.
        std::vector<model::broker_shard> target_assignment;
        reconfiguration_state state;
        /// Revision of the command initiating this update
        model::revision_id revision;
        /// Revision of the last command in this update (cancellation etc.)
        model::revision_id last_cmd_revision;
        /// Reconfiguration policy used with this update
        reconfiguration_policy policy;

        friend bool operator==(const update_t&, const update_t&) = default;

        auto serde_fields() {
            return std::tie(
              target_assignment, state, revision, last_cmd_revision, policy);
        }
    };

    struct topic_t
      : public serde::
          envelope<topic_t, serde::version<1>, serde::compat_version<0>> {
        topic_metadata_fields metadata;
        chunked_hash_map<model::partition_id, partition_t> partitions;
        chunked_hash_map<model::partition_id, update_t> updates;
        std::optional<topic_disabled_partitions_set> disabled_set;

        friend bool operator==(const topic_t&, const topic_t&) = default;

        ss::future<> serde_async_write(iobuf&);
        ss::future<> serde_async_read(iobuf_parser&, const serde::header);
    };

    chunked_hash_map<model::topic_namespace, topic_t> topics;
    raft::group_id highest_group_id;

    absl::node_hash_map<
      nt_revision,
      nt_lifecycle_marker,
      nt_revision_hash,
      nt_revision_eq>
      lifecycle_markers;

    force_recoverable_partitions_t partitions_to_force_recover;

    friend bool operator==(const topics_t&, const topics_t&) = default;

    ss::future<> serde_async_write(iobuf&);
    ss::future<> serde_async_read(iobuf_parser&, const serde::header);
};

struct named_role_t
  : public serde::
      envelope<named_role_t, serde::version<0>, serde::compat_version<0>> {
    named_role_t() = default;
    named_role_t(security::role_name name, security::role role)
      : name(std::move(name))
      , role(std::move(role)) {}
    security::role_name name;
    security::role role;

    friend bool operator==(const named_role_t&, const named_role_t&) = default;

    auto serde_fields() { return std::tie(name, role); }
};

struct security_t
  : public serde::
      envelope<security_t, serde::version<1>, serde::compat_version<0>> {
    fragmented_vector<user_and_credential> user_credentials;
    fragmented_vector<security::acl_binding> acls;
    chunked_vector<named_role_t> roles;

    friend bool operator==(const security_t&, const security_t&) = default;

    ss::future<> serde_async_write(iobuf&);
    ss::future<> serde_async_read(iobuf_parser&, const serde::header);
};

struct metrics_reporter_t
  : public serde::envelope<
      metrics_reporter_t,
      serde::version<0>,
      serde::compat_version<0>> {
    metrics_reporter_cluster_info cluster_info;

    friend bool operator==(const metrics_reporter_t&, const metrics_reporter_t&)
      = default;

    auto serde_fields() { return std::tie(cluster_info); }
};

struct plugins_t
  : public serde::
      envelope<plugins_t, serde::version<0>, serde::compat_version<0>> {
    absl::btree_map<model::transform_id, model::transform_metadata> transforms;

    friend bool operator==(const plugins_t&, const plugins_t&) = default;

    auto serde_fields() { return std::tie(transforms); }
};

struct cluster_recovery_t
  : public serde::envelope<
      cluster_recovery_t,
      serde::version<0>,
      serde::compat_version<0>> {
    std::vector<cluster_recovery_state> recovery_states;

    friend bool operator==(const cluster_recovery_t&, const cluster_recovery_t&)
      = default;

    auto serde_fields() { return std::tie(recovery_states); }
};

struct client_quotas_t
  : public serde::
      envelope<client_quotas_t, serde::version<0>, serde::compat_version<0>> {
    absl::node_hash_map<client_quota::entity_key, client_quota::entity_value>
      quotas;

    friend bool operator==(const client_quotas_t&, const client_quotas_t&)
      = default;

    auto serde_fields() { return std::tie(quotas); }
};

struct data_migrations_t
  : public serde::
      envelope<data_migrations_t, serde::version<0>, serde::compat_version<0>> {
    data_migrations::id next_id;
    absl::
      node_hash_map<data_migrations::id, data_migrations::migration_metadata>
        migrations;

    friend bool operator==(const data_migrations_t&, const data_migrations_t&)
      = default;

    auto serde_fields() { return std::tie(next_id, migrations); }
};

} // namespace controller_snapshot_parts

struct controller_snapshot
  : public serde::checksum_envelope<
      controller_snapshot,
      serde::version<4>,
      serde::compat_version<0>> {
    controller_snapshot_parts::bootstrap_t bootstrap;
    controller_snapshot_parts::features_t features;
    controller_snapshot_parts::members_t members;
    controller_snapshot_parts::config_t config;
    controller_snapshot_parts::topics_t topics;
    controller_snapshot_parts::security_t security;
    controller_snapshot_parts::metrics_reporter_t metrics_reporter;
    controller_snapshot_parts::plugins_t plugins;
    controller_snapshot_parts::cluster_recovery_t cluster_recovery;
    controller_snapshot_parts::client_quotas_t client_quotas;
    controller_snapshot_parts::data_migrations_t data_migrations;

    friend bool
    operator==(const controller_snapshot&, const controller_snapshot&)
      = default;

    ss::future<> serde_async_write(iobuf&);
    ss::future<> serde_async_read(iobuf_parser&, const serde::header);
};

/// A subset of the controller snapshot used to initialize nodes joining
/// the cluster.  This does not include any of the large per-partition
/// structures.
struct controller_join_snapshot
  : public serde::envelope<
      controller_join_snapshot,
      serde::version<0>,
      serde::compat_version<0>> {
    // Joining node should use this offset as the controllers
    // 'bootstrap_last_applied' so that the joining node will not start Kafka
    // API until it has replayed all the known controller history.
    model::offset last_applied;

    // Joining nodes should apply this state before starting controller for
    // the first time.
    controller_snapshot_parts::bootstrap_t bootstrap;
    controller_snapshot_parts::features_t features;
    controller_snapshot_parts::config_t config;
    controller_snapshot_parts::metrics_reporter_t metrics_reporter;

    auto serde_fields() {
        return std::tie(
          last_applied, bootstrap, features, config, metrics_reporter);
    }
};

} // namespace cluster
