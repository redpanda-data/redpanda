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

#include "cluster/config_manager.h"
#include "cluster/types.h"
#include "features/feature_table_snapshot.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "serde/envelope.h"
#include "serde/serde.h"
#include "utils/fragmented_vector.h"

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
      envelope<members_t, serde::version<0>, serde::compat_version<0>> {
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

    friend bool operator==(const members_t&, const members_t&) = default;

    auto serde_fields() {
        return std::tie(
          node_ids_by_uuid,
          next_assigned_id,
          nodes,
          removed_nodes,
          removed_nodes_still_in_raft0,
          in_progress_updates,
          first_node_operation_command_offset);
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
      envelope<topics_t, serde::version<0>, serde::compat_version<0>> {
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
          envelope<update_t, serde::version<0>, serde::compat_version<0>> {
        /// NOTE: In the event of cancellation this remains the original target.
        std::vector<model::broker_shard> target_assignment;
        reconfiguration_state state;
        /// Revision of the command initiating this update
        model::revision_id revision;
        /// Revision of the last command in this update (cancellation etc.)
        model::revision_id last_cmd_revision;

        friend bool operator==(const update_t&, const update_t&) = default;

        auto serde_fields() {
            return std::tie(
              target_assignment, state, revision, last_cmd_revision);
        }
    };

    struct topic_t
      : public serde::
          envelope<topic_t, serde::version<0>, serde::compat_version<0>> {
        topic_metadata_fields metadata;
        absl::node_hash_map<model::partition_id, partition_t> partitions;
        absl::node_hash_map<model::partition_id, update_t> updates;

        friend bool operator==(const topic_t&, const topic_t&) = default;

        ss::future<> serde_async_write(iobuf&);
        ss::future<> serde_async_read(iobuf_parser&, serde::header const);
    };

    absl::node_hash_map<model::topic_namespace, topic_t> topics;
    raft::group_id highest_group_id;

    friend bool operator==(const topics_t&, const topics_t&) = default;

    ss::future<> serde_async_write(iobuf&);
    ss::future<> serde_async_read(iobuf_parser&, serde::header const);
};

struct security_t
  : public serde::
      envelope<security_t, serde::version<0>, serde::compat_version<0>> {
    fragmented_vector<user_and_credential> user_credentials;
    fragmented_vector<security::acl_binding> acls;

    friend bool operator==(const security_t&, const security_t&) = default;

    ss::future<> serde_async_write(iobuf&);
    ss::future<> serde_async_read(iobuf_parser&, serde::header const);
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

} // namespace controller_snapshot_parts

struct controller_snapshot
  : public serde::checksum_envelope<
      controller_snapshot,
      serde::version<0>,
      serde::compat_version<0>> {
    controller_snapshot_parts::bootstrap_t bootstrap;
    controller_snapshot_parts::features_t features;
    controller_snapshot_parts::members_t members;
    controller_snapshot_parts::config_t config;
    controller_snapshot_parts::topics_t topics;
    controller_snapshot_parts::security_t security;
    controller_snapshot_parts::metrics_reporter_t metrics_reporter;

    friend bool
    operator==(const controller_snapshot&, const controller_snapshot&)
      = default;

    ss::future<> serde_async_write(iobuf&);
    ss::future<> serde_async_read(iobuf_parser&, serde::header const);
};

} // namespace cluster
