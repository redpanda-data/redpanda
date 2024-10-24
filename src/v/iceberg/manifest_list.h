// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "bytes/bytes.h"
#include "container/fragmented_vector.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/partition.h"
#include "utils/named_type.h"

namespace iceberg {

struct field_summary {
    bool contains_null{false};
    std::optional<bool> contains_nan;
    std::optional<bytes> lower_bound;
    std::optional<bytes> upper_bound;
    friend bool operator==(const field_summary&, const field_summary&)
      = default;
};

enum class manifest_file_content {
    data = 0,
    deletes = 1,

    // If new values are ever added that don't form a contiguous range, stop
    // using these as bounds checks for deserialization validation!
    min_supported = data,
    max_supported = deletes,
};

struct manifest_file {
    ss::sstring manifest_path;
    size_t manifest_length;
    partition_spec::id_t partition_spec_id;
    manifest_file_content content;
    sequence_number seq_number;
    sequence_number min_seq_number;
    snapshot_id added_snapshot_id;
    size_t added_files_count;
    size_t existing_files_count;
    size_t deleted_files_count;
    size_t added_rows_count;
    size_t existing_rows_count;
    size_t deleted_rows_count;
    chunked_vector<field_summary> partitions;

    // TODO: the avrogen schema doesn't include this. We should add it to the
    // schema and then uncomment this.
    // std::optional<bytes> key_metadata;

    friend bool operator==(const manifest_file&, const manifest_file&)
      = default;

    manifest_file copy() const {
        return manifest_file{
          .manifest_path = manifest_path,
          .manifest_length = manifest_length,
          .partition_spec_id = partition_spec_id,
          .content = content,
          .seq_number = seq_number,
          .min_seq_number = min_seq_number,
          .added_snapshot_id = added_snapshot_id,
          .added_files_count = added_files_count,
          .existing_files_count = existing_files_count,
          .deleted_files_count = deleted_files_count,
          .added_rows_count = added_rows_count,
          .existing_rows_count = existing_rows_count,
          .deleted_rows_count = deleted_rows_count,
          .partitions = partitions.copy(),
        };
    }
};

struct manifest_list {
    chunked_vector<manifest_file> files;
    friend bool operator==(const manifest_list&, const manifest_list&)
      = default;
};

} // namespace iceberg
