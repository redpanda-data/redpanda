// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/manifest_entry.h"

namespace iceberg {
namespace {

chunked_hash_map<nested_field::id_t, size_t>
copy_map(const chunked_hash_map<nested_field::id_t, size_t>& m) {
    chunked_hash_map<nested_field::id_t, size_t> ret;
    ret.reserve(m.size());
    for (auto& [k, v] : m) {
        ret.emplace(k, v);
    }
    return ret;
}
} // namespace
data_file data_file::copy() const {
    return data_file{
      .content_type = content_type,
      .file_path = file_path,
      .file_format = file_format,
      .partition = partition.copy(),
      .record_count = record_count,
      .file_size_bytes = file_size_bytes,
      .column_sizes = copy_map(column_sizes),
      .value_counts = copy_map(value_counts),
      .null_value_counts = copy_map(null_value_counts),
      .distinct_counts = copy_map(distinct_counts),
      .nan_value_counts = copy_map(nan_value_counts),
    };
}

manifest_entry manifest_entry::copy() const {
    return manifest_entry{
      .status = status,
      .snapshot_id = snapshot_id,
      .sequence_number = sequence_number,
      .file_sequence_number = file_sequence_number,
      .data_file = data_file.copy(),
    };
}

} // namespace iceberg
