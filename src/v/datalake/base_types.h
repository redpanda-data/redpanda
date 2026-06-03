/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once
#include "base/format_to.h"
#include "bytes/bytes.h"
#include "container/chunked_vector.h"
#include "serde/envelope.h"
#include "serde/rw/bytes.h"
#include "utils/named_type.h"

#include <cstdint>
#include <filesystem>
#include <optional>

namespace datalake {
/**
 * Definitions of local and remote paths, as the name indicates the local path
 * is always pointing to the location on local disk wheras the remote path is a
 * path of the object in the object store.
 */
using local_path = named_type<std::filesystem::path, struct local_path_tag>;
using remote_path = named_type<std::filesystem::path, struct remote_path_tag>;

/// Statistics for a single column within a parquet file, keyed by Iceberg
/// nested_field::id_t. Bounds are in Iceberg binary single-value format
/// (identical to Parquet PLAIN encoding for all supported types).
struct per_column_stats
  : serde::
      envelope<per_column_stats, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(
          field_id,
          lower_bound,
          upper_bound,
          null_value_count,
          value_count,
          column_size_bytes);
    }

    int32_t field_id = -1;
    std::optional<bytes> lower_bound;
    std::optional<bytes> upper_bound;
    int64_t null_value_count = 0;
    int64_t value_count = 0;
    int64_t column_size_bytes = 0;

    friend bool
    operator==(const per_column_stats&, const per_column_stats&) = default;
};

/**
 * Simple type describing local parquet file metadata with its path and basic
 * statistics
 */
struct local_file_metadata {
    local_path path;
    size_t row_count = 0;
    size_t size_bytes = 0;
    chunked_vector<per_column_stats> column_stats;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{relative_path: {}, size_bytes: {}, row_count: {}}}",
          path,
          size_bytes,
          row_count);
    }
};
} // namespace datalake
