// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/update_schema_action.h"

#include "iceberg/schema.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"

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
    auto last_column_id = new_schema_.highest_field_id();
    ret.updates.emplace_back(table_update::add_schema{
      .schema = std::move(new_schema_),
      .last_column_id = last_column_id,
    });
    // NOTE: -1 indicates that we should set the schema to the one added in
    // this update.
    ret.updates.emplace_back(
      table_update::set_current_schema{schema::unassigned_id});
    ret.requirements.emplace_back(
      table_requirement::assert_current_schema_id{cur_schema_id});
    co_return ret;
}

} // namespace iceberg
