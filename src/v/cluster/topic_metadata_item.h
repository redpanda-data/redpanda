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

#include "cluster/types.h"
#include "model/fundamental.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

/**
 * Replicas revision map is used to track revision of brokers in a replica
 * set. When a node is added into replica set its gets the revision assigned
 */
using replicas_revision_map
  = absl::flat_hash_map<model::node_id, model::revision_id>;

struct topic_metadata_item {
    topic_metadata metadata;
    // replicas revisions for each partition
    absl::node_hash_map<model::partition_id, replicas_revision_map>
      replica_revisions;

    bool is_topic_replicable() const;

    assignments_set& get_assignments();
    const assignments_set& get_assignments() const;

    model::revision_id get_revision() const;

    std::optional<model::initial_revision_id> get_remote_revision() const;

    const model::topic& get_source_topic() const;

    const topic_configuration& get_configuration() const;
    topic_configuration& get_configuration();
};

} // namespace cluster
