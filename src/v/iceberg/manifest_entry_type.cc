// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/manifest_entry_type.h"

namespace iceberg {

namespace {

struct_type data_file_type(partition_key_type partition_type) {
    struct_type r2_type;
    // TODO: somehow make this not a magic number.
    r2_type.fields.reserve(17);
    r2_type.fields.emplace_back(
      nested_field::create(134, "content", field_required::yes, int_type()));
    r2_type.fields.emplace_back(nested_field::create(
      100, "file_path", field_required::yes, string_type()));
    r2_type.fields.emplace_back(nested_field::create(
      101, "file_format", field_required::yes, string_type()));
    r2_type.fields.emplace_back(nested_field::create(
      102, "partition", field_required::yes, std::move(partition_type.type)));
    r2_type.fields.emplace_back(nested_field::create(
      103, "record_count", field_required::yes, long_type()));
    r2_type.fields.emplace_back(nested_field::create(
      104, "file_size_in_bytes", field_required::yes, long_type()));
    r2_type.fields.emplace_back(nested_field::create(
      108,
      "column_sizes",
      field_required::no,
      map_type::create(
        117, int_type(), 118, field_required::yes, long_type())));
    r2_type.fields.emplace_back(nested_field::create(
      109,
      "value_counts",
      field_required::no,
      map_type::create(
        119, int_type(), 120, field_required::yes, long_type())));
    r2_type.fields.emplace_back(nested_field::create(
      110,
      "null_value_counts",
      field_required::no,
      map_type::create(
        121, int_type(), 122, field_required::yes, long_type())));
    r2_type.fields.emplace_back(nested_field::create(
      111,
      "distinct_counts",
      field_required::no,
      map_type::create(
        123, int_type(), 124, field_required::yes, long_type())));
    r2_type.fields.emplace_back(nested_field::create(
      137,
      "nan_value_counts",
      field_required::no,
      map_type::create(
        138, int_type(), 139, field_required::yes, long_type())));
    r2_type.fields.emplace_back(nested_field::create(
      125,
      "lower_bounds",
      field_required::no,
      map_type::create(
        126, int_type(), 127, field_required::yes, binary_type())));
    r2_type.fields.emplace_back(nested_field::create(
      128,
      "upper_bounds",
      field_required::no,
      map_type::create(
        129, int_type(), 130, field_required::yes, binary_type())));
    r2_type.fields.emplace_back(nested_field::create(
      131, "key_metadata", field_required::no, binary_type()));
    r2_type.fields.emplace_back(nested_field::create(
      132,
      "split_offsets",
      field_required::no,
      list_type::create(133, field_required::yes, long_type())));
    r2_type.fields.emplace_back(nested_field::create(
      135,
      "equality_ids",
      field_required::no,
      list_type::create(136, field_required::yes, int_type())));
    r2_type.fields.emplace_back(nested_field::create(
      140, "sort_order_id", field_required::no, int_type()));
    return r2_type;
}

} // namespace

struct_type manifest_entry_type(partition_key_type partition_type) {
    struct_type manifest_entry_type;
    manifest_entry_type.fields.reserve(partition_type.type.fields.size());
    manifest_entry_type.fields.emplace_back(
      nested_field::create(0, "status", field_required::yes, int_type()));
    manifest_entry_type.fields.emplace_back(
      nested_field::create(1, "snapshot_id", field_required::no, long_type()));
    manifest_entry_type.fields.emplace_back(nested_field::create(
      3, "sequence_number", field_required::no, long_type()));
    manifest_entry_type.fields.emplace_back(nested_field::create(
      4, "file_sequence_number", field_required::no, long_type()));
    manifest_entry_type.fields.emplace_back(nested_field::create(
      2,
      "data_file",
      field_required::yes,
      data_file_type(std::move(partition_type))));
    return manifest_entry_type;
}

} // namespace iceberg
