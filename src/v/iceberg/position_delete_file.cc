/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/position_delete_file.h"

#include "bytes/iostream.h"
#include "iceberg/conversion/stats_parquet.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "serde/parquet/writer.h"

#include <limits>

namespace iceberg {

namespace {

serde::parquet::schema_element position_delete_schema() {
    using serde::parquet::byte_array_type;
    using serde::parquet::field_repetition_type;
    using serde::parquet::i64_type;
    using serde::parquet::schema_element;
    using serde::parquet::string_type;

    // Iceberg spec reserves field IDs for position delete columns.
    // See org.apache.iceberg.MetadataColumns in the Iceberg library.
    constexpr auto int_max = std::numeric_limits<int32_t>::max();
    constexpr int32_t file_path_field_id = int_max - 101;
    constexpr int32_t pos_field_id = int_max - 102;

    chunked_vector<schema_element> children;
    children.push_back(
      schema_element{
        .type = byte_array_type{},
        .repetition_type = field_repetition_type::required,
        .path = {ss::sstring("file_path")},
        .field_id = file_path_field_id,
        .logical_type = string_type{},
      });
    children.push_back(
      schema_element{
        .type = i64_type{},
        .repetition_type = field_repetition_type::required,
        .path = {ss::sstring("pos")},
        .field_id = pos_field_id,
      });
    return schema_element{
      .type = std::monostate{},
      .repetition_type = field_repetition_type::required,
      .path = {ss::sstring("position_deletes")},
      .children = std::move(children),
    };
}

} // namespace

ss::future<delete_file_result> write_position_delete_file(
  chunked_vector<position_delete_entry> deletes, bool compress) {
    auto schema = position_delete_schema();
    iobuf file;
    serde::parquet::writer w(
      {.schema = std::move(schema), .compress = compress},
      make_iobuf_ref_output_stream(file));
    co_await w.init();

    auto record_count = deletes.size();
    for (auto& entry : deletes) {
        serde::parquet::group_value row;
        row.push_back(
          serde::parquet::group_member{
            serde::parquet::byte_array_value{iobuf::from(entry.file_path())}});
        row.push_back(
          serde::parquet::group_member{serde::parquet::int64_value{entry.pos}});
        co_await w.write_row(std::move(row));
    }

    auto file_metadata = co_await w.close();
    auto stats = conversion::extract_iceberg_stats(file_metadata);

    data_file df{
      .content_type = data_file_content_type::position_deletes,
      .file_path = uri{},
      .file_format = data_file_format::parquet,
      .partition = partition_key{},
      .record_count = record_count,
      .file_size_bytes = file.size_bytes(),
      .column_sizes = std::move(stats.column_sizes),
      .value_counts = std::move(stats.value_counts),
      .null_value_counts = std::move(stats.null_value_counts),
      .lower_bounds = std::move(stats.lower_bounds),
      .upper_bounds = std::move(stats.upper_bounds),
    };

    co_return delete_file_result{
      .file_data = std::move(file),
      .manifest_entry = std::move(df),
    };
}

} // namespace iceberg
