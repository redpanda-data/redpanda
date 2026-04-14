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
#include "serde/parquet/column_array.h"
#include "serde/parquet/file_io.h"
#include "serde/parquet/schema.h"

#include <seastar/core/future.hh>

namespace iceberg {

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
ss::future<parquet_reader_result>
read_parquet(const struct_type& read_schema, serde::parquet::file_io& io);

} // namespace iceberg
