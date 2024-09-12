/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "cluster/data_migration_types.h"
#include "cluster/fwd.h"
#include "container/chunked_hash_map.h"
#include "model/metadata.h"

#include <absl/container/flat_hash_set.h>

namespace cluster::data_migrations {
/**
 * Helper stucture that represents resources being actively migrated. This
 * structure is intended to be used as a look up table for migrated resources to
 * prevent their updates while being migrated.
 *
 * Data migration resources is updated by data_migrations_table.
 */
class migrated_resources {
public:
    struct resource_metadata {
        id migration_id;
        migrated_resource_state state;
    };

public:
    /**
     * Returns current state of the topic that is being migrated.
     * It the topic is not being migrated the method returns a
     * migrated_resource_state::non_restricted.
     */
    migrated_resource_state get_topic_state(model::topic_namespace_view) const;
    /**
     * Returns current state of the consumer group that is being migrated.
     * It the group is not being migrated the method returns a
     * migrated_resource_state::non_restricted.
     */
    migrated_resource_state get_group_state(const consumer_group&) const;

    /// Checks if topic is already migrated
    bool is_already_migrated(const model::topic_namespace& topic) const {
        return _topics.contains(topic);
    }
    /// Checks if consumer group  is already migrated
    bool is_already_migrated(const consumer_group& cg) const {
        return _groups.contains(cg);
    }

private:
    void apply_snapshot(
      const std::vector<migration_metadata>& deleted,
      const std::vector<std::reference_wrapper<migration_metadata>>& updated);

    void apply_update(const migration_metadata&);
    void apply_update(id, const inbound_migration&, state);
    void apply_update(id, const outbound_migration&, state);

    void remove_migration(const migration_metadata&);
    void remove_migration(id, const inbound_migration&);
    void remove_migration(id, const outbound_migration&);

    friend migrations_table;

    chunked_hash_map<
      model::topic_namespace,
      resource_metadata,
      model::topic_namespace_hash,
      model::topic_namespace_eq>
      _topics;
    chunked_hash_map<consumer_group, resource_metadata> _groups;
};
} // namespace cluster::data_migrations
