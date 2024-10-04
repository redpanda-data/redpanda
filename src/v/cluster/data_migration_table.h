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
#include "cluster/commands.h"
#include "cluster/data_migrated_resources.h"
#include "cluster/data_migration_types.h"
#include "cluster/topic_table.h"
#include "container/fragmented_vector.h"
#include "utils/named_type.h"
#include "utils/notification_list.h"

#include <absl/container/node_hash_map.h>

namespace cluster::data_migrations {
namespace testing_details {
class data_migration_table_test_accessor;
}

/**
 * A class keeping track of migration state. The data_migration_table updates
 * are driven by data_migration controller commands. The data migration table is
 * instantiated only on shard 0. It allows to register notification which are
 * fired every time the migration is update. The data migration table is also
 * responsible for driving migrated resources updates.
 */
class migrations_table {
public:
    using validation_error
      = named_type<ss::sstring, struct validation_error_tag>;

    explicit migrations_table(
      ss::sharded<migrated_resources>& resources,
      ss::sharded<topic_table>& topics,
      bool enabled);

    ss::future<> stop();

    using notification_id = named_type<uint64_t, struct notification_id_tag>;
    using notification_callback = ss::noncopyable_function<void(id)>;

    static constexpr auto commands = make_commands_list<
      create_data_migration_cmd,
      update_data_migration_state_cmd,
      remove_data_migration_cmd>();

    bool is_batch_applicable(const model::record_batch& b) const {
        return b.header().type == model::record_batch_type::data_migration_cmd;
    }
    // an entry point for controller stm, receives a record batch if is
    // applicable to data migration state machine.
    ss::future<std::error_code> apply_update(model::record_batch);

    /**
     * Fills the snapshot with data from the migration table when requested to
     * do so by the controller state machine
     */
    ss::future<> fill_snapshot(controller_snapshot&) const;
    /**
     * Called withe controller snapshot when the controller state machine is
     * replied.
     */
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&);
    /**
     * Returns a single migration with requested id or empty optional if no
     * migration is found.
     */
    std::optional<std::reference_wrapper<const migration_metadata>>
      get_migration(id) const;
    /**
     * Returns a list of all existing migration ids.
     */
    chunked_vector<id> get_migrations() const;

    /**
     * Registers notification that if fired every time a data migration state is
     * updated.
     *
     * @return id of notification that can be used to unregister the
     * notification
     */
    notification_id register_notification(notification_callback);

    /**
     * Unregisters notification with requested id.
     */
    void unregister_notification(notification_id);

    /**
     * Returns metadata of all currently active data migrations.
     */
    chunked_vector<migration_metadata> list_migrations() const {
        chunked_vector<migration_metadata> ret;
        ret.reserve(_migrations.size());
        for (const auto& [_, meta] : _migrations) {
            ret.push_back(meta.copy());
        }
        return ret;
    }

private:
    friend class frontend;
    friend testing_details::data_migration_table_test_accessor;

    ss::future<std::error_code> apply(create_data_migration_cmd);
    ss::future<std::error_code> apply(update_data_migration_state_cmd);
    ss::future<std::error_code> apply(remove_data_migration_cmd);

    id get_next_id() { return _next_id++; }

    std::optional<validation_error>
    validate_migrated_resources(const inbound_migration&) const;
    std::optional<validation_error>
    validate_migrated_resources(const outbound_migration&) const;

    static bool
    is_valid_state_transition(state current_state, state target_state);

    static bool is_empty_migration(const data_migration&);

    std::optional<validation_error>
    validate_migrated_resources(const data_migration&) const;

private:
    id _next_id{0};
    id _last_applied{};

    absl::node_hash_map<id, migration_metadata> _migrations;
    ss::sharded<migrated_resources>& _resources;
    ss::sharded<topic_table>& _topics;
    bool _enabled;

    notification_list<notification_callback, notification_id> _callbacks;
};
} // namespace cluster::data_migrations
