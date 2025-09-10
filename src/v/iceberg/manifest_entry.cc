/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

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
