/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "container/chunked_vector.h"
#include "iceberg/action.h"
#include "iceberg/datatypes.h"
#include "iceberg/table_metadata.h"

#include <vector>

namespace iceberg {

/// Action that renames columns by nested field path, producing a new schema
/// version with the renamed fields. Follows the same pattern as
/// update_schema_action: copies the current schema, applies renames, then
/// emits add_schema + set_current_schema updates.
class rename_columns_action : public action {
public:
    struct rename_entry {
        /// Path to the field within the schema. Each element is a field
        /// name; the traversal descends into struct types and list element
        /// types. E.g. {"redpanda", "headers", "key"} reaches the "key"
        /// field inside the list element struct of "headers".
        std::vector<ss::sstring> field_path;
        ss::sstring new_name;
    };
    rename_columns_action(
      const table_metadata& table, chunked_vector<rename_entry> renames);

protected:
    ss::future<action_outcome> build_updates() && final;

private:
    const table_metadata& table_;
    chunked_vector<rename_entry> renames_;
};

} // namespace iceberg
