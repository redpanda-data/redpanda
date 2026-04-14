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
#include "iceberg/datatypes.h"
#include "serde/parquet/metadata.h"

namespace iceberg::conversion {

struct iceberg_column_stats {
    std::optional<chunked_hash_map<nested_field::id_t, int64_t>> column_sizes;
    std::optional<chunked_hash_map<nested_field::id_t, int64_t>> value_counts;
    std::optional<chunked_hash_map<nested_field::id_t, int64_t>>
      null_value_counts;
    std::optional<chunked_hash_map<nested_field::id_t, iobuf>> lower_bounds;
    std::optional<chunked_hash_map<nested_field::id_t, iobuf>> upper_bounds;
};

/// \brief Extract Iceberg-formatted column statistics from parquet
/// file_metadata.
///
/// Walks the flattened schema to find leaf columns with field_ids, then
/// merges statistics across all row groups: sums for counts/sizes, min
/// for lower_bounds, max for upper_bounds. Columns without field_ids are
/// skipped. If no column has min/max stats, bounds are left as nullopt.
iceberg_column_stats
extract_iceberg_stats(const serde::parquet::file_metadata& metadata);

} // namespace iceberg::conversion
