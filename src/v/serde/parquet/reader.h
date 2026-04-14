/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "serde/parquet/column_array.h"
#include "serde/parquet/metadata.h"
#include "serde/parquet/value.h"

#include <seastar/core/future.hh>

namespace serde::parquet {

struct reader_options {
    /// Column paths to read (projection pushdown). Empty = all columns.
    chunked_vector<chunked_vector<ss::sstring>> column_projection;
};

struct file_reader_result {
    /// Full file metadata from the parquet footer.
    file_metadata metadata;
    /// Schema matching the columns in row_groups. When column_projection
    /// is used, this is a pruned schema containing only the projected
    /// leaves and their ancestor groups.
    schema_element schema;
    chunked_vector<columnar_batch> row_groups;
};

/// \brief Read a complete parquet file from an iobuf.
///
/// Parses the footer, recovers the schema, decodes column chunks (async
/// for decompression), and returns columnar batches per row group.
ss::future<file_reader_result>
read_file(iobuf file_data, reader_options opts = {});

/// \brief Read a parquet file and assemble records.
///
/// Convenience wrapper that calls read_file() then assembles all row
/// groups into group_value records using the record assembler.
ss::future<chunked_vector<group_value>>
read_file_as_records(iobuf file_data, reader_options opts = {});

} // namespace serde::parquet
