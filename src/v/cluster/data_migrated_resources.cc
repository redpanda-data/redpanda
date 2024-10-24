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
    // Although two !active! migrations cannot share a resource it is legit to
    // encounter a resource still locked by another migration here:
    // 1. Migration 0 is created and reaches a state where it locks topic T
    // 2. We lose controller leadership
    // 3. Migration 0 reaches finished state where it doesn't lock T any longer
    // 4. Migration 1 involving T is created and reaches finished state as well
    // 5. We receive a raft snapshot that includes (3) and (4)
    // 6. We process (4) first, it tries to unblock T for migration 1: ignore!
    // 7. Then we process (3), to unblock T for migration 0: do it
    if (it != resources.cend() && it->second.migration_id == id) {
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
        return migrated_resource_state::create_only;
    case state::prepared:
    case state::canceling:
    case state::executing:
    case state::executed:
    case state::cut_over:
        return migrated_resource_state::fully_blocked;
    case state::finished:
    case state::cancelled:
        return migrated_resource_state::non_restricted;
    case state::deleted:
        vassert(false, "a migration cannot be in deleted state");
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
    case state::deleted:
        vassert(false, "a migration cannot be in deleted state");
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

void migrated_resources::apply_snapshot(
  const std::vector<migration_metadata>& deleted,
  const std::vector<std::reference_wrapper<migration_metadata>>& updated) {
    std::vector<std::reference_wrapper<migration_metadata>> updated_restricted;

    // 1st pass: delete restrictions for deleted and unrestricted migrations
    for (auto& migration_meta : deleted) {
        remove_migration(migration_meta);
    }
    for (auto& migration_meta_ref : updated) {
        auto& migration_meta = migration_meta_ref.get();
        ss::visit(
          migration_meta.migration,
          [this, &migration_meta_ref, &migration_meta, &updated_restricted](
            auto& migration) {
              using migration_type = std::remove_cvref_t<decltype(migration)>;
              auto target_state = get_resource_state<migration_type>(
                migration_meta.state);
              if (target_state == migrated_resource_state::non_restricted) {
                  remove_migration(migration_meta);
              } else {
                  updated_restricted.push_back(migration_meta_ref);
              }
          });
    }

    // 2nd pass: update restricted
    for (auto& migration_meta_ref : updated_restricted) {
        apply_update(migration_meta_ref.get());
    }
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
