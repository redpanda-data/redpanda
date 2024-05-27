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

namespace cluster {

namespace {
template<typename T>
void remove_from_resources(
  data_migration_id id,
  const T& to_remove,
  chunked_hash_map<T, data_migrated_resources::resource_metadata>& resources) {
    auto it = resources.find(to_remove);
    vassert(
      it != resources.end(),
      "migrated {} resource must exists in migrated resources");
    vassert(
      it->second.migration_id == id,
      "removed migration must match migrated resource migration id");
    resources.erase(it);
}

migrated_resource_state
get_outbound_migration_resource_state(data_migration_state state) {
    switch (state) {
    case data_migration_state::planned:
    case cluster::data_migration_state::preparing:
    case cluster::data_migration_state::prepared:
    case cluster::data_migration_state::canceling:
    case cluster::data_migration_state::cancelled:
        return migrated_resource_state::restricted;
    case cluster::data_migration_state::executing:
    case cluster::data_migration_state::executed:
        return migrated_resource_state::blocked;
    case cluster::data_migration_state::finished:
        return migrated_resource_state::non_restricted;
    }
}

} // namespace

migrated_resource_state data_migrated_resources::get_topic_state(
  const model::topic_namespace& tp_ns) const {
    auto it = _topics.find(tp_ns);
    if (it != _topics.end()) {
        return it->second.state;
    }
    return migrated_resource_state::non_restricted;
}

migrated_resource_state
data_migrated_resources::get_group_state(const consumer_group& cg) const {
    auto it = _groups.find(cg);
    if (it != _groups.end()) {
        return it->second.state;
    }
    return migrated_resource_state::non_restricted;
}

void data_migrated_resources::apply_update(
  const data_migration_metadata& migration_meta) {
    ss::visit(
      migration_meta.migration,
      [this, id = migration_meta.id, state = migration_meta.state](
        auto& migration) { apply_update(id, migration, state); });
}

void data_migrated_resources::apply_update(
  data_migration_id id,
  const outbound_data_migration& migration,
  data_migration_state state) {
    auto target_state = get_outbound_migration_resource_state(state);
    for (const auto& t : migration.topics) {
        auto [it, _] = _topics.try_emplace(
          t,
          resource_metadata{
            .migration_id = id,
            .state = target_state,
          });
        it->second.state = target_state;
    }
    for (const auto& gr : migration.groups) {
        auto [it, _] = _groups.try_emplace(
          gr,
          resource_metadata{
            .migration_id = id,
            .state = target_state,
          });
        it->second.state = target_state;
    }
}

void data_migrated_resources::apply_update(
  data_migration_id id,
  const inbound_data_migration& migration,
  data_migration_state state) {
    /**
     * When inbound migration is active all the resources are blocked. Only when
     * migration is finished the restrictions are lifted off.
     */
    if (state == data_migration_state::finished) {
        remove_migration(id, migration);
        return;
    }

    for (const auto& t : migration.topics) {
        _topics.try_emplace(
          t.effective_topic_name(),
          resource_metadata{
            .migration_id = id,
            .state = migrated_resource_state::blocked,
          });
    }
    for (const auto& gr : migration.groups) {
        _groups.try_emplace(
          gr,
          resource_metadata{
            .migration_id = id,
            .state = migrated_resource_state::blocked,
          });
    }
}

void data_migrated_resources::remove_migration(
  const data_migration_metadata& migration_meta) {
    ss::visit(
      migration_meta.migration,
      [this, id = migration_meta.id](auto& migration) {
          remove_migration(id, migration);
      });
}

void data_migrated_resources::remove_migration(
  data_migration_id id, const inbound_data_migration& idm) {
    for (auto& inbound_topic : idm.topics) {
        remove_from_resources(
          id, inbound_topic.effective_topic_name(), _topics);
    }

    for (auto& cg : idm.groups) {
        remove_from_resources(id, cg, _groups);
    }
}

void data_migrated_resources::remove_migration(
  data_migration_id id, const outbound_data_migration& odm) {
    for (const auto& topic : odm.topics) {
        remove_from_resources(id, topic, _topics);
    }

    for (const auto& cg : odm.groups) {
        remove_from_resources(id, cg, _groups);
    }
}

} // namespace cluster
