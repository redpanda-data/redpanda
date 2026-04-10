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

#include "base/seastarx.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "serde/envelope.h"
#include "serde/rw/bytes.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/optional.h"

#include <seastar/core/sstring.hh>

#include <fmt/core.h>

namespace datalake::coordinator {

/// \brief Per-column statistics for a single Iceberg field, in a flat
/// serde-friendly format. Each entry corresponds to one leaf column in the
/// parquet file and carries the Iceberg field id so the committer can
/// rebuild the per-field maps expected by iceberg::data_file.
struct column_stat_entry
  : serde::
      envelope<column_stat_entry, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(
          field_id,
          column_size,
          value_count,
          null_value_count,
          lower_bound,
          upper_bound);
    }
    int32_t field_id{0};
    int64_t column_size{0};
    int64_t value_count{0};
    int64_t null_value_count{0};
    iobuf lower_bound;
    iobuf upper_bound;

    column_stat_entry copy() const {
        return {
          .field_id = field_id,
          .column_size = column_size,
          .value_count = value_count,
          .null_value_count = null_value_count,
          .lower_bound = lower_bound.copy(),
          .upper_bound = upper_bound.copy(),
        };
    }

    friend bool operator==(const column_stat_entry&, const column_stat_entry&)
      = default;
};

// Represents a file that exists in object storage.
struct data_file
  : serde::envelope<data_file, serde::version<2>, serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(
          remote_path,
          row_count,
          file_size_bytes,
          hour_deprecated,
          table_schema_id,
          partition_spec_id,
          partition_key,
          column_stats,
          split_offsets);
    }
    ss::sstring remote_path = "";
    size_t row_count = 0;
    size_t file_size_bytes = 0;
    // After datalake_iceberg_ga feature flag is enabled, this field won't be
    // set. Use `partition_key` instead.
    int hour_deprecated = 0;

    int32_t table_schema_id = -1;
    int32_t partition_spec_id = -1;
    // Contains partition key fields serialized with Iceberg "binary
    // single-value serialization" (see iceberg/values_bytes.h).
    // Nulls are represented by std::nullopt.
    chunked_vector<std::optional<bytes>> partition_key;

    // Per-column statistics extracted from parquet file metadata.
    // Added in version 2.
    std::optional<chunked_vector<column_stat_entry>> column_stats;

    // Row group byte offsets within the parquet file, used by parallel
    // readers to split work across row group boundaries.
    std::optional<chunked_vector<int64_t>> split_offsets;

    data_file copy() const {
        data_file ret{
          .remote_path = remote_path,
          .row_count = row_count,
          .file_size_bytes = file_size_bytes,
          .hour_deprecated = hour_deprecated,
          .table_schema_id = table_schema_id,
          .partition_spec_id = partition_spec_id,
          .partition_key = partition_key.copy(),
        };
        if (column_stats) {
            chunked_vector<column_stat_entry> stats_copy;
            stats_copy.reserve(column_stats->size());
            for (const auto& e : *column_stats) {
                stats_copy.push_back(e.copy());
            }
            ret.column_stats = std::move(stats_copy);
        }
        if (split_offsets) {
            ret.split_offsets = split_offsets->copy();
        }
        return ret;
    }

    friend bool operator==(const data_file&, const data_file&) = default;
};

std::ostream& operator<<(std::ostream& o, const data_file& f);

} // namespace datalake::coordinator
