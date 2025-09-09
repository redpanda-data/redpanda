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

template<typename KeyT, typename ValT>
std::optional<chunked_hash_map<KeyT, ValT>>
copy_primitive_map(const std::optional<chunked_hash_map<KeyT, ValT>>& m) {
    if (!m.has_value()) {
        return std::nullopt;
    }
    chunked_hash_map<KeyT, ValT> ret;
    ret.reserve(m->size());
    for (auto& [k, v] : *m) {
        ret.emplace(k, v);
    }
    return ret;
}

template<typename ElementT>
std::optional<chunked_vector<ElementT>>
copy_primitive_list(const std::optional<chunked_vector<ElementT>>& m) {
    if (!m.has_value()) {
        return std::nullopt;
    }
    chunked_vector<ElementT> ret;
    ret.reserve(m->size());
    for (auto& [k, v] : *m) {
        ret.emplace(k, v);
    }
    return ret;
}

std::optional<chunked_hash_map<nested_field::id_t, iobuf>> copy_bounds_map(
  const std::optional<chunked_hash_map<nested_field::id_t, iobuf>>& m) {
    if (!m.has_value()) {
        return std::nullopt;
    }
    chunked_hash_map<nested_field::id_t, iobuf> ret;
    ret.reserve(m->size());
    for (auto& [k, v] : *m) {
        ret.emplace(k, v.copy());
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
      .column_sizes = copy_primitive_map(column_sizes),
      .value_counts = copy_primitive_map(value_counts),
      .null_value_counts = copy_primitive_map(null_value_counts),
      .nan_value_counts = copy_primitive_map(nan_value_counts),
      .lower_bounds = copy_bounds_map(lower_bounds),
      .upper_bounds = copy_bounds_map(upper_bounds),
      .key_metadata = key_metadata ? std::make_optional(key_metadata->copy())
                                   : std::nullopt,
      .split_offsets = split_offsets ? std::make_optional(split_offsets->copy())
                                     : std::nullopt,
      .equality_ids = equality_ids ? std::make_optional(equality_ids->copy())
                                   : std::nullopt,
      .sort_order_id = sort_order_id,
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
