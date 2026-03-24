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
#include "iceberg/datatypes.h"
#include "iceberg/schema.h"

#include <seastar/core/sstring.hh>

#include <vector>

namespace datalake {

/// Describes a single column that needs renaming for v25.3 migration.
struct column_migration_entry {
    /// Nested path to the field, suitable for passing directly to
    /// rename_columns_action. E.g. {"redpanda", "timestamp"}.
    std::vector<ss::sstring> field_path;
    ss::sstring column_path;
    ss::sstring current_name;
    ss::sstring suggested_new_name;
    ss::sstring reason;
};

/// Detects columns in the redpanda system struct that need renaming for
/// the v25.3 schema migration. Checks:
///   - redpanda.timestamp: timestamp -> timestamptz
///   - redpanda.headers[].key: binary -> string
chunked_vector<column_migration_entry>
detect_redpanda_struct_renames(const iceberg::schema& table_schema);

/// Detects user-schema columns that need renaming by comparing the existing
/// table schema against the expected schema produced by the new translator.
/// Skips the "redpanda" struct (already handled by
/// detect_redpanda_struct_renames).
chunked_vector<column_migration_entry> detect_user_schema_renames(
  const iceberg::struct_type& existing,
  const iceberg::struct_type& expected,
  const ss::sstring& path_prefix = "");

} // namespace datalake
