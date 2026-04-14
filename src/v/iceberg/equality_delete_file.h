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

#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "iceberg/datatypes.h"
#include "iceberg/manifest_entry.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"

#include <seastar/core/future.hh>

namespace iceberg {

struct delete_file_result {
    iobuf file_data;
    data_file manifest_entry;
};

struct equality_delete_options {
    serde::parquet::schema_element parquet_schema;
    chunked_vector<nested_field::id_t> equality_field_ids;
    bool compress = false;
};

/// \brief Write an equality delete file in parquet format from key column
/// values.
///
/// The schema must match the equality columns from the table schema. The
/// resulting data_file has content_type=equality_deletes and equality_ids
/// populated from the provided field IDs.
ss::future<delete_file_result> write_equality_delete_file(
  equality_delete_options opts,
  chunked_vector<serde::parquet::group_value> rows);

} // namespace iceberg
