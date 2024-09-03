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

#include <utility>

namespace cluster::data_migrations {

namespace {
template<typename T, typename H, typename E>
void remove_from_resources(
  id id,
  const T& to_remove,
  chunked_hash_map<T, migrated_resources::resource_metadata, H, E>& resources) {
    auto it = std::as_const(resources).find(to_remove);
    if (it != resources.cend()) {
        vassert(
          it->second.migration_id == id,
          "removed migration must match migrated resource migration id");
        resources.erase(it);
    }
}

template<class M>
migrated_resource_state get_resource_state(state state);

template<>
migrated_resource_state get_resource_state<inbound_migration>(state state) {
    switch (state) {
    case state::planned:
        return migrated_resource_state::metadata_locked;
    case state::preparing:
    case state::prepared:
    case state::canceling:
    case state::executing:
    case state::executed:
    case state::cut_over:
        return migrated_resource_state::fully_blocked;
    case state::finished:
    case state::cancelled:
        return migrated_resource_state::non_restricted;
    }
}

template<>
migrated_resource_state get_resource_state<outbound_migration>(state state) {
    switch (state) {
    case state::planned:
    case state::preparing:
    case state::prepared:
        return migrated_resource_state::metadata_locked;
    case state::executing:
    case state::executed:
    case state::canceling:
        return migrated_resource_state::read_only;
    case state::cut_over:
        return migrated_resource_state::fully_blocked;
    case state::finished:
    case state::cancelled:
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
      migration_meta.migration, [this, &migration_meta](auto& migration) {
          using migration_type = std::remove_cvref_t<decltype(migration)>;
          auto target_state = get_resource_state<migration_type>(
            migration_meta.state);
          if (target_state == migrated_resource_state::non_restricted) {
              remove_migration(migration_meta);
          } else {
              for (const auto& t : migration.topic_nts()) {
                  _topics[t] = {
                    .migration_id = migration_meta.id,
                    .state = target_state,
                  };
              }
              for (const auto& gr : migration.groups) {
                  _groups[gr] = {
                    .migration_id = migration_meta.id,
                    .state = target_state,
                  };
              }
          }
      });
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
