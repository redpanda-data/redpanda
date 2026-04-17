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

#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "iceberg/datatypes.h"
#include "serde/parquet/column_array.h"
#include "serde/parquet/file_io.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"

#include <seastar/core/future.hh>

#include <variant>

namespace iceberg {

/// Sorted position delete entries for a specific data file.
/// Positions are file-global row indices (not per row group).
struct position_delete_set {
    chunked_vector<int64_t> positions;
};

/// Equality delete key set for a specific data file.
///
/// Contains field IDs identifying the key columns and a hash set of
/// delete key tuples. During merge-on-read, every data row whose key
/// columns match an entry in this set is filtered out.
///
/// Sequence number filtering (equality deletes only apply to data with
/// strictly lower sequence numbers) is the caller's responsibility —
/// the reader applies the set unconditionally.
struct equality_delete_set {
    chunked_vector<int32_t> field_ids;
    chunked_hash_set<
      serde::parquet::group_value,
      serde::parquet::group_value_hash>
      keys;
};

using delete_file_entry
  = std::variant<position_delete_set, equality_delete_set>;

struct parquet_reader_result {
    /// Schema matching the columns in row_groups. Derived from the
    /// table struct_type structure with physical types from the file.
    /// Names come from the table schema, not the file.
    serde::parquet::schema_element schema;
    chunked_vector<serde::parquet::columnar_batch> row_groups;
};

/// \brief Read a parquet file with Iceberg schema evolution semantics.
///
/// Matches columns by field ID between read_schema and the file's
/// embedded schema. Results are in table schema order.
ss::future<parquet_reader_result> read_parquet(
  const struct_type& read_schema,
  serde::parquet::file_io& io,
  chunked_vector<delete_file_entry> delete_files = {});

} // namespace iceberg
