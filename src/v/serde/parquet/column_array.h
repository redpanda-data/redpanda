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

#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "serde/parquet/schema.h"

#include <cstdint>
#include <variant>

namespace serde::parquet {

// TODO: Nullability is currently tracked via definition levels in
// columnar_batch::level_data, not via a per-column validity bitmap. If a
// future execution/scan layer needs a materialized bitmap for vectorized null
// skipping (as Arrow and Velox do), derive it from def levels at that
// boundary. A chunked_vector<uint64_t>-backed packed bitmap with O(1) access
// would be the natural choice — see the assembler for how def levels map to
// null positions.

/// \brief A decoded column of homogeneous physical type.
///
/// Fixed-width types use chunked_vector<T> (128KiB chunks, O(1) access).
/// Variable-width types use offset+iobuf.
/// Null positions are not tracked here — use the corresponding def levels
/// in columnar_batch::level_data.
struct column_array {
    physical_type ptype;
    logical_type ltype;

    struct boolean_data {
        iobuf packed_bits;
        int64_t num_values = 0;
    };
    struct i32_data {
        chunked_vector<int32_t> values;
    };
    struct i64_data {
        chunked_vector<int64_t> values;
    };
    struct f32_data {
        chunked_vector<float> values;
    };
    struct f64_data {
        chunked_vector<double> values;
    };
    struct byte_array_data {
        /// N+1 offsets for N non-null values. offsets[0] == 0.
        chunked_vector<int64_t> offsets;
        /// Concatenated byte arrays.
        iobuf data;
    };
    struct fixed_byte_array_data {
        int32_t fixed_length = 0;
        /// Concatenated fixed-length values.
        iobuf data;
    };

    using data_type = std::variant<
      boolean_data,
      i32_data,
      i64_data,
      f32_data,
      f64_data,
      byte_array_data,
      fixed_byte_array_data>;

    data_type data;

    /// Total number of logical values (including nulls).
    int64_t length = 0;
};

/// \brief A batch of decoded columns from one row group.
///
/// Nested types are NOT materialized as nested column_array structures —
/// they stay as flat leaf columns with definition/repetition levels,
/// assembled lazily by the record assembler.
struct columnar_batch {
    /// One column_array per leaf in the schema, ordered by schema position.
    chunked_vector<column_array> columns;

    struct level_data {
        chunked_vector<def_level> def_levels;
        chunked_vector<rep_level> rep_levels;
    };

    /// Definition and repetition levels for each column.
    chunked_vector<level_data> levels;

    int64_t num_rows = 0;
};

} // namespace serde::parquet
