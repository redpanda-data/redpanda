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
#include "container/chunked_hash_map.h"
#include "iceberg/partition_key.h"
#include "iceberg/uri.h"
#include "iceberg/values.h"

#include <seastar/core/sstring.hh>

namespace iceberg {

enum class data_file_content_type {
    data,
    position_deletes,
    equality_deletes,
};

enum class data_file_format {
    avro,
    orc,
    parquet,
};

struct data_file {
    data_file_content_type content_type;
    uri file_path;
    data_file_format file_format;

    partition_key partition;

    size_t record_count;
    size_t file_size_bytes;
    std::optional<chunked_hash_map<nested_field::id_t, int64_t>> column_sizes;
    std::optional<chunked_hash_map<nested_field::id_t, int64_t>> value_counts;
    std::optional<chunked_hash_map<nested_field::id_t, int64_t>>
      null_value_counts;
    std::optional<chunked_hash_map<nested_field::id_t, int64_t>>
      nan_value_counts;
    std::optional<chunked_hash_map<nested_field::id_t, iobuf>> lower_bounds;
    std::optional<chunked_hash_map<nested_field::id_t, iobuf>> upper_bounds;
    std::optional<iobuf> key_metadata;
    std::optional<chunked_vector<int64_t>> split_offsets;
    std::optional<chunked_vector<nested_field::id_t>> equality_ids;
    std::optional<int32_t> sort_order_id;
    std::optional<uri> referenced_data_file;

    friend bool operator==(const data_file&, const data_file&) = default;
    data_file copy() const;
};

enum class manifest_entry_status {
    existing,
    added,
    deleted,
};

using snapshot_id = named_type<int64_t, struct snapshot_id_tag>;
// some catalogs use -1 to indicate that the current snapshot id is not present
static constexpr snapshot_id invalid_snapshot_id{-1};

using sequence_number = named_type<int64_t, struct data_seq_tag>;
using file_sequence_number = named_type<int64_t, struct file_seq_tag>;
struct manifest_entry {
    manifest_entry_status status;
    std::optional<snapshot_id> snapshot_id;
    std::optional<sequence_number> sequence_number;
    std::optional<file_sequence_number> file_sequence_number;
    data_file data_file;
    friend bool
    operator==(const manifest_entry&, const manifest_entry&) = default;
    manifest_entry copy() const;
};

} // namespace iceberg
