/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/update_schema_action.h"

#include "iceberg/schema.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"

#include <seastar/core/coroutine.hh>

namespace iceberg {

ss::future<action::action_outcome> update_schema_action::build_updates() && {
    updates_and_reqs ret;
    const auto cur_schema_id = table_.current_schema_id;
    auto highest_schema_id = cur_schema_id;
    // Look for an existing schema that matches the type.
    for (const auto& s : table_.schemas) {
        highest_schema_id = std::max(highest_schema_id, s.schema_id);
        if (new_schema_.schema_struct == s.schema_struct) {
            if (s.schema_id == cur_schema_id) {
                // This operation is a no-op: the current schema has the
                // target type.
                co_return updates_and_reqs{};
            }
            // The new schema matches an existing one. Just set the table
            // schema to the existing schema.
            ret.updates.emplace_back(
              table_update::set_current_schema{s.schema_id});
            ret.requirements.emplace_back(
              table_requirement::assert_current_schema_id{cur_schema_id});
            co_return ret;
        }
    }
    // No matches, this is a new schema. Add it to the table and set it as the
    // current schema.
    const schema::id_t new_schema_id{highest_schema_id() + 1};
    new_schema_.schema_id = new_schema_id;

    // NOTE: assumes that
    //   - the table's last_column_id increases monotonically
    //   - carry-over fields in a compat-checked schema already have
    //     IDs assigned
    //   - those carry-over IDs are all strictly <= table.last
    // so we start column ID assignment for any new fields at table.last + 1
    if (
      auto schm_res = new_schema_.assign_fresh_ids(table_.last_column_id + 1);
      schm_res.has_error()) {
        co_return errc::unexpected_state;
    }
    auto last_column_id = new_schema_.highest_field_id();
    ret.updates.emplace_back(
      table_update::add_schema{
        .schema = std::move(new_schema_),
        .last_column_id = last_column_id,
      });
    // NOTE: -1 indicates that we should set the schema to the one added in
    // this update.
    ret.updates.emplace_back(
      table_update::set_current_schema{
        table_update::set_current_schema::last_added});
    ret.requirements.emplace_back(
      table_requirement::assert_current_schema_id{cur_schema_id});
    co_return ret;
}

} // namespace iceberg
