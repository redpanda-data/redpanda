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

#include "cluster/types.h"
#include "features/feature_table_snapshot.h"
#include "serde/envelope.h"
#include "serde/serde.h"

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

} // namespace controller_snapshot_parts

struct controller_snapshot
  : public serde::checksum_envelope<
      controller_snapshot,
      serde::version<0>,
      serde::compat_version<0>> {
    controller_snapshot_parts::bootstrap_t bootstrap;
    controller_snapshot_parts::features_t features;
    controller_snapshot_parts::members_t members;

    friend bool
    operator==(const controller_snapshot&, const controller_snapshot&)
      = default;

    ss::future<> serde_async_write(iobuf&);
    ss::future<> serde_async_read(iobuf_parser&, serde::header const);
};

} // namespace cluster
