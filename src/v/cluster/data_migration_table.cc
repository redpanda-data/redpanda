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
#include "cluster/data_migration_table.h"

#include "cluster/controller_snapshot.h"
#include "cluster/data_migrated_resources.h"
#include "cluster/data_migration_types.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/topic_table.h"
#include "container/fragmented_vector.h"

#include <seastar/util/variant_utils.hh>

#include <ranges>

namespace cluster::data_migrations {

migrations_table::migrations_table(
  ss::sharded<migrated_resources>& resources,
  ss::sharded<topic_table>& topics,
  bool enabled)
  : _resources(resources)
  , _topics(topics)
  , _enabled(enabled) {}

ss::future<> migrations_table::stop() { return ss::now(); }

bool migrations_table::is_valid_state_transition(state current, state target) {
    switch (current) {
    case state::planned:
        return target == state::preparing;
    case state::preparing:
        return target == state::prepared || target == state::canceling;
    case state::prepared:
        return target == state::executing || target == state::canceling;
    case state::executing:
        return target == state::executed || target == state::canceling;
    case state::executed:
        return target == state::cut_over || target == state::canceling;
    case state::cut_over:
        return target == state::finished;
    case state::canceling:
        return target == state::cancelled;
    /**
     * Those are the terminal states, there it is impossible to get out of them
     * in other way than deleting migration object
     **/
    case state::cancelled:
    case state::finished:
        [[fallthrough]];
    case state::deleted:
        return false;
    }
}

bool migrations_table::is_empty_migration(const data_migration& m) {
    return ss::visit(
      m, [](const auto& m) { return m.topics.empty() && m.groups.empty(); });
}

ss::future<std::error_code>
migrations_table::apply_update(model::record_batch batch) {
    auto cmd = co_await deserialize(std::move(batch), commands);

    co_return co_await std::visit(
      [this](auto cmd) { return apply(std::move(cmd)); }, std::move(cmd));
}

ss::future<>
migrations_table::fill_snapshot(controller_snapshot& snapshot) const {
    snapshot.data_migrations.next_id = _next_id;
    snapshot.data_migrations.migrations.reserve(_migrations.size());
    for (auto& [id, migration] : _migrations) {
        snapshot.data_migrations.migrations.emplace(id, migration.copy());
    }

    co_return;
}

ss::future<> migrations_table::apply_snapshot(
  model::offset, const controller_snapshot& snapshot) {
    _next_id = snapshot.data_migrations.next_id;

    auto snapshot_size = snapshot.data_migrations.migrations.size();
    _migrations.reserve(snapshot_size);

    std::vector<id> affected_ids;
    affected_ids.reserve(snapshot_size);

    std::vector<migration_metadata> deleted;
    auto it = _migrations.cbegin();
    while (it != _migrations.cend()) {
        auto prev = it++;
        if (!snapshot.data_migrations.migrations.contains(prev->first)) {
            auto extracted = _migrations.extract(prev);
            affected_ids.push_back(extracted.key());
            deleted.push_back(std::move(extracted.mapped()));
        }
    }

    // iterators and therefore references remain valid as we reserved the space
    std::vector<std::reference_wrapper<migration_metadata>> updated;
    for (const auto& [id, migration] : snapshot.data_migrations.migrations) {
        auto it = _migrations.find(id);
        if (it == _migrations.end()) {
            it = _migrations.emplace(id, migration.copy()).first;
        } else {
            if (it->second.state == migration.state) {
                continue;
            }
            it->second.state = migration.state;
        }
        affected_ids.push_back(id);
        updated.emplace_back(it->second);
    }

    // notify callbacks after resources, see comment in migrations_table::apply
    co_await _resources.invoke_on_all(
      [&deleted, &updated](migrated_resources& resources) {
          resources.apply_snapshot(deleted, updated);
      });

    for (auto id : affected_ids) {
        _callbacks.notify(id);
    }
}

std::optional<std::reference_wrapper<const migration_metadata>>
migrations_table::get_migration(id id) const {
    if (auto it = _migrations.find(id); it != _migrations.end()) {
        return std::cref(it->second);
    }
    return {};
}

chunked_vector<id> migrations_table::get_migrations() const {
    auto view = _migrations | std::views::keys;
    return {view.begin(), view.end()};
}

migrations_table::notification_id
migrations_table::register_notification(notification_callback clb) {
    return _callbacks.register_cb(std::move(clb));
}

void migrations_table::unregister_notification(notification_id id) {
    _callbacks.unregister_cb(id);
}

ss::future<std::error_code>
migrations_table::apply(create_data_migration_cmd cmd) {
    auto migration = std::move(cmd.value.migration);
    const auto id = cmd.value.id;
    vlog(
      dm_log.debug,
      "applying create data migration: {} with id: {}",
      migration,
      id);
    if (id <= _last_applied) {
        co_return errc::data_migration_already_exists;
    }
    /**
     * We do not allow to create empty data migrations
     */
    if (is_empty_migration(migration)) {
        co_return errc::data_migration_invalid_resources;
    }

    auto err = validate_migrated_resources(migration);
    if (err) {
        vlog(dm_log.info, "migration validation error: {}", err.value());
        co_return errc::data_migration_invalid_resources;
    }

    auto [it, success] = _migrations.try_emplace(
      id, migration_metadata{.id = id, .migration = std::move(migration)});

    if (!success) {
        // TODO: consider explaining to the client that we had an internal race
        // condition and it should retry
        co_return errc::data_migration_already_exists;
    }
    _last_applied = id;
    _next_id = std::max(_next_id, _last_applied + data_migrations::id(1));
    // It is vital to update resources before subscribers, backend in
    // particular. When backend works on the migrated entities it relies on them
    // being locked by resources.
    co_await _resources.invoke_on_all(
      [&meta = it->second](migrated_resources& resources) {
          resources.apply_update(meta);
      });
    _callbacks.notify(id);

    co_return errc::success;
}

std::optional<migrations_table::validation_error>
migrations_table::validate_migrated_resources(
  const data_migration& migration) const {
    // cloud_storage_api is checked on startup
    if (!_enabled) {
        return validation_error{"cloud storage disabled"};
    }

    return ss::visit(migration, [this](const auto& migration) {
        return validate_migrated_resources(migration);
    });
}

std::optional<migrations_table::validation_error>
migrations_table::validate_migrated_resources(
  const inbound_migration& idm) const {
    for (const auto& t : idm.topics) {
        if (_topics.local().contains(t.effective_topic_name())) {
            return validation_error{ssx::sformat(
              "topic with name {} already exists in this cluster",
              t.effective_topic_name())};
        }

        if (_resources.local().is_already_migrated(t.effective_topic_name())) {
            return validation_error{ssx::sformat(
              "topic with name {} is already part of active migration",
              t.effective_topic_name())};
        }
    }

    for (const auto& group : idm.groups) {
        if (_resources.local().is_already_migrated(group)) {
            return validation_error{ssx::sformat(
              "group with name {} is already part of active migration", group)};
        }
    }

    return std::nullopt;
}

std::optional<migrations_table::validation_error>
migrations_table::validate_migrated_resources(
  const outbound_migration& odm) const {
    for (const auto& t : odm.topics) {
        if (t.ns != model::kafka_namespace) {
            return validation_error{ssx::sformat(
              "topic with name {} is not in default namespace, so probably it "
              "has archiver disabled",
              t)};
        }

        auto maybe_topic_cfg = _topics.local().get_topic_cfg(t);
        if (!maybe_topic_cfg) {
            return validation_error{ssx::sformat(
              "topic with name {} does not exists in current cluster", t)};
        }

        if (!model::is_archival_enabled(
              maybe_topic_cfg->properties.shadow_indexing.value_or(
                model::shadow_indexing_mode::disabled))) {
            return validation_error{ssx::sformat(
              "topic with name {} does not have archiving enabled", t)};
        }

        if (_resources.local().is_already_migrated(t)) {
            return validation_error{ssx::sformat(
              "topic with name {} is already part of active migration", t)};
        }
    }

    for (const auto& group : odm.groups) {
        if (_resources.local().is_already_migrated(group)) {
            return validation_error{ssx::sformat(
              "group with name {} is already part of active migration", group)};
        }
    }

    return std::nullopt;
}

ss::future<std::error_code>
migrations_table::apply(update_data_migration_state_cmd cmd) {
    const auto id = cmd.value.id;
    const auto requested_state = cmd.value.requested_state;
    vlog(
      dm_log.debug,
      "applying update data migration {} state to {}",
      id,
      requested_state);
    auto it = _migrations.find(id);
    if (it == _migrations.end()) {
        vlog(
          dm_log.warn,
          "can not update migration {} state to {}, migration not "
          "found",
          id,
          requested_state);
        co_return errc::data_migration_not_exists;
    }

    if (!is_valid_state_transition(it->second.state, requested_state)) {
        // invalid state transition
        vlog(
          dm_log.info,
          "can not update migration {} state from {} to {}, this transition is "
          "invalid",
          id,
          it->second.state,
          requested_state);

        co_return errc::invalid_data_migration_state;
    }
    it->second.state = requested_state;
    // notify callbacks after resources, see comment in migrations_table::apply
    co_await _resources.invoke_on_all(
      [&meta = it->second](migrated_resources& resources) {
          resources.apply_update(meta);
      });
    _callbacks.notify(id);

    co_return errc::success;
}

ss::future<std::error_code>
migrations_table::apply(remove_data_migration_cmd cmd) {
    const auto id = cmd.value.id;
    auto it = _migrations.find(id);
    vlog(dm_log.debug, "applying remove migration {} command", id);

    if (it == _migrations.end()) {
        co_return errc::data_migration_not_exists;
    }

    switch (it->second.state) {
    case state::cancelled:
    case state::finished:
    case state::planned: {
        auto meta = std::move(it->second);
        _migrations.erase(it);
        co_await _resources.invoke_on_all(
          [&meta](migrated_resources& resources) {
              resources.remove_migration(meta);
          });
        _callbacks.notify(id);

        co_return errc::success;
    }
    default:
        vlog(
          dm_log.warn,
          "can not remove migration with id {} which is in {} state",
          id,
          it->second.state);

        co_return errc::invalid_data_migration_state;
    }
}

} // namespace cluster::data_migrations
