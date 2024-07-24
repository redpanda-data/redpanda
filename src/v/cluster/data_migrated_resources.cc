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
#include "cluster/data_migrated_resources.h"

#include "container/chunked_hash_map.h"
#include "data_migration_types.h"

#include <seastar/util/variant_utils.hh>

#include <absl/container/flat_hash_set.h>

namespace cluster::data_migrations {

namespace {
template<typename T, typename H, typename E>
void remove_from_resources(
  id id,
  const T& to_remove,
  chunked_hash_map<T, migrated_resources::resource_metadata, H, E>& resources) {
    auto it = resources.find(to_remove);
    vassert(
      it != resources.end(),
      "migrated {} resource must exists in migrated resources");
    vassert(
      it->second.migration_id == id,
      "removed migration must match migrated resource migration id");
    resources.erase(it);
}

migrated_resource_state get_outbound_migration_resource_state(state state) {
    switch (state) {
    case state::planned:
    case state::preparing:
    case state::prepared:
    case state::canceling:
    case state::cancelled:
        return migrated_resource_state::restricted;
    case state::executing:
    case state::executed:
    case state::cut_over:
        return migrated_resource_state::blocked;
    case state::finished:
        return migrated_resource_state::non_restricted;
    }
}

} // namespace

migrated_resource_state
migrated_resources::get_topic_state(model::topic_namespace_view tp_ns) const {
    auto it = _topics.find(tp_ns);
    if (it != _topics.end()) {
        return it->second.state;
    }
    return migrated_resource_state::non_restricted;
}

migrated_resource_state
migrated_resources::get_group_state(const consumer_group& cg) const {
    auto it = _groups.find(cg);
    if (it != _groups.end()) {
        return it->second.state;
    }
    return migrated_resource_state::non_restricted;
}

void migrated_resources::apply_update(
  const migration_metadata& migration_meta) {
    ss::visit(
      migration_meta.migration,
      [this, id = migration_meta.id, state = migration_meta.state](
        auto& migration) { apply_update(id, migration, state); });
}

void migrated_resources::apply_update(
  id id, const outbound_migration& migration, state state) {
    auto target_state = get_outbound_migration_resource_state(state);
    for (const auto& t : migration.topics) {
        _topics[t] = {
          .migration_id = id,
          .state = target_state,
        };
    }
    for (const auto& gr : migration.groups) {
        _groups[gr] = {
          .migration_id = id,
          .state = target_state,
        };
    }
}

void migrated_resources::apply_update(
  id id, const inbound_migration& migration, state state) {
    /**
     * When inbound migration is active all the resources are blocked. Only when
     * migration is finished the restrictions are lifted off.
     */
    if (state == state::finished) {
        remove_migration(id, migration);
        return;
    }
    if (state == state::planned) {
        for (const auto& t : migration.topics) {
            auto [_, inserted] = _topics.try_emplace(
              t.effective_topic_name(),
              resource_metadata{
                .migration_id = id,
                .state = migrated_resource_state::blocked,
              });
            vassert(
              inserted,
              "The topic {} has already been added to migrated resources of "
              "data migration: {}",
              t.effective_topic_name(),
              id);
        }
        for (const auto& gr : migration.groups) {
            auto [_, inserted] = _groups.try_emplace(
              gr,
              resource_metadata{
                .migration_id = id,
                .state = migrated_resource_state::blocked,
              });
            vassert(
              inserted,
              "The group {} has already been added to migrated resources of "
              "data migration: {}",
              gr,
              id);
        }
    }
}

void migrated_resources::remove_migration(
  const migration_metadata& migration_meta) {
    ss::visit(
      migration_meta.migration,
      [this, id = migration_meta.id](auto& migration) {
          for (const auto& topic : migration.topic_nts()) {
              remove_from_resources(id, topic, _topics);
          }

          for (const auto& cg : migration.groups) {
              remove_from_resources(id, cg, _groups);
          }
      });
}

} // namespace cluster::data_migrations
