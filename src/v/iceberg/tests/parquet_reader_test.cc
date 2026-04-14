/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "iceberg/conversion/schema_parquet.h"
#include "iceberg/datatypes.h"
#include "iceberg/parquet_reader.h"
#include "serde/parquet/assembler.h"
#include "serde/parquet/file_io.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "serde/parquet/writer.h"

#include <gtest/gtest.h>

namespace iceberg {
namespace {

namespace sp = serde::parquet;

/// Write rows to a parquet file in an iobuf using the given schema.
iobuf write_to_iobuf(
  sp::schema_element schema,
  chunked_vector<sp::group_value> rows,
  bool compress = false) {
    iobuf file;
    sp::writer w(
      {.schema = std::move(schema), .compress = compress},
      make_iobuf_ref_output_stream(file));
    w.init().get();
    for (auto& row : rows) {
        w.write_row(std::move(row)).get();
    }
    w.close().get();
    return file;
}

/// Convert an Iceberg struct_type to a parquet schema_element using
/// schema_to_parquet, suitable for writing.
sp::schema_element iceberg_to_write_schema(const struct_type& st) {
    // We inline the conversion from schema_parquet.cc. The iceberg BUILD target
    // already has this dependency, but to keep this test self-contained we use
    // the public API.
    return schema_to_parquet(st);
}

/// Build a group_value record from variadic parquet values.
template<typename... Args>
sp::group_value record(Args... field) {
    chunked_vector<sp::group_member> fields;
    (fields.push_back(sp::group_member{std::move(field)}), ...);
    return fields;
}

// NOLINTBEGIN(*magic-number*)

TEST(IcebergParquetReader, FieldIdReordering) {
    // Write schema: {a: int32 (id=1), b: int64 (id=2)}
    struct_type write_schema;
    write_schema.fields.push_back(
      nested_field::create(
        1, "a", field_required::yes, primitive_type{int_type{}}));
    write_schema.fields.push_back(
      nested_field::create(
        2, "b", field_required::yes, primitive_type{long_type{}}));

    auto pq_schema = iceberg_to_write_schema(write_schema);

    chunked_vector<sp::group_value> rows;
    rows.push_back(record(sp::int32_value{10}, sp::int64_value{100}));
    rows.push_back(record(sp::int32_value{20}, sp::int64_value{200}));
    rows.push_back(record(sp::int32_value{30}, sp::int64_value{300}));

    auto file = write_to_iobuf(std::move(pq_schema), std::move(rows));

    // Read schema: {b: int64 (id=2), a: int32 (id=1)} -- reversed
    struct_type read_schema;
    read_schema.fields.push_back(
      nested_field::create(
        2, "b", field_required::yes, primitive_type{long_type{}}));
    read_schema.fields.push_back(
      nested_field::create(
        1, "a", field_required::yes, primitive_type{int_type{}}));

    sp::iobuf_file_io io(std::move(file));
    auto result = read_parquet(read_schema, io).get();

    ASSERT_EQ(result.row_groups.size(), 1);
    const auto& batch = result.row_groups[0];
    ASSERT_EQ(batch.columns.size(), 2);
    EXPECT_EQ(batch.num_rows, 3);

    // First column should be b (int64)
    const auto& col_b = batch.columns[0];
    ASSERT_TRUE(std::holds_alternative<sp::column_array::i64_data>(col_b.data));
    const auto& b_vals = std::get<sp::column_array::i64_data>(col_b.data);
    ASSERT_EQ(b_vals.values.size(), 3);
    EXPECT_EQ(b_vals.values[0], 100);
    EXPECT_EQ(b_vals.values[1], 200);
    EXPECT_EQ(b_vals.values[2], 300);

    // Second column should be a (int32)
    const auto& col_a = batch.columns[1];
    ASSERT_TRUE(std::holds_alternative<sp::column_array::i32_data>(col_a.data));
    const auto& a_vals = std::get<sp::column_array::i32_data>(col_a.data);
    ASSERT_EQ(a_vals.values.size(), 3);
    EXPECT_EQ(a_vals.values[0], 10);
    EXPECT_EQ(a_vals.values[1], 20);
    EXPECT_EQ(a_vals.values[2], 30);

    // Schema names should come from the table (read) schema.
    ASSERT_EQ(result.schema.children.size(), 2);
    EXPECT_EQ(result.schema.children[0].name(), "b");
    EXPECT_EQ(result.schema.children[1].name(), "a");
}

TEST(IcebergParquetReader, RenamedColumn) {
    // Write with {old_name: int32 (id=1)}
    struct_type write_schema;
    write_schema.fields.push_back(
      nested_field::create(
        1, "old_name", field_required::yes, primitive_type{int_type{}}));

    auto pq_schema = iceberg_to_write_schema(write_schema);

    chunked_vector<sp::group_value> rows;
    rows.push_back(record(sp::int32_value{42}));
    rows.push_back(record(sp::int32_value{99}));

    auto file = write_to_iobuf(std::move(pq_schema), std::move(rows));

    // Read with {new_name: int32 (id=1)}
    struct_type read_schema;
    read_schema.fields.push_back(
      nested_field::create(
        1, "new_name", field_required::yes, primitive_type{int_type{}}));

    sp::iobuf_file_io io(std::move(file));
    auto result = read_parquet(read_schema, io).get();

    ASSERT_EQ(result.row_groups.size(), 1);
    const auto& batch = result.row_groups[0];
    ASSERT_EQ(batch.columns.size(), 1);

    const auto& col = batch.columns[0];
    ASSERT_TRUE(std::holds_alternative<sp::column_array::i32_data>(col.data));
    const auto& vals = std::get<sp::column_array::i32_data>(col.data);
    ASSERT_EQ(vals.values.size(), 2);
    EXPECT_EQ(vals.values[0], 42);
    EXPECT_EQ(vals.values[1], 99);

    // Result schema uses the table name, not the file name.
    ASSERT_EQ(result.schema.children.size(), 1);
    EXPECT_EQ(result.schema.children[0].name(), "new_name");
}

TEST(IcebergParquetReader, DroppedColumn) {
    // Write with {a: int32 (id=1), b: int64 (id=2), c: double (id=3)}
    struct_type write_schema;
    write_schema.fields.push_back(
      nested_field::create(
        1, "a", field_required::yes, primitive_type{int_type{}}));
    write_schema.fields.push_back(
      nested_field::create(
        2, "b", field_required::yes, primitive_type{long_type{}}));
    write_schema.fields.push_back(
      nested_field::create(
        3, "c", field_required::yes, primitive_type{double_type{}}));

    auto pq_schema = iceberg_to_write_schema(write_schema);

    chunked_vector<sp::group_value> rows;
    rows.push_back(
      record(sp::int32_value{1}, sp::int64_value{10}, sp::float64_value{1.5}));
    rows.push_back(
      record(sp::int32_value{2}, sp::int64_value{20}, sp::float64_value{2.5}));

    auto file = write_to_iobuf(std::move(pq_schema), std::move(rows));

    // Read with {a: int32 (id=1), b: int64 (id=2)} -- c dropped
    struct_type read_schema;
    read_schema.fields.push_back(
      nested_field::create(
        1, "a", field_required::yes, primitive_type{int_type{}}));
    read_schema.fields.push_back(
      nested_field::create(
        2, "b", field_required::yes, primitive_type{long_type{}}));

    sp::iobuf_file_io io(std::move(file));
    auto result = read_parquet(read_schema, io).get();

    ASSERT_EQ(result.row_groups.size(), 1);
    const auto& batch = result.row_groups[0];
    // Only a and b should be present.
    ASSERT_EQ(batch.columns.size(), 2);

    const auto& col_a = batch.columns[0];
    ASSERT_TRUE(std::holds_alternative<sp::column_array::i32_data>(col_a.data));
    EXPECT_EQ(
      std::get<sp::column_array::i32_data>(col_a.data).values.size(), 2);

    const auto& col_b = batch.columns[1];
    ASSERT_TRUE(std::holds_alternative<sp::column_array::i64_data>(col_b.data));
    EXPECT_EQ(
      std::get<sp::column_array::i64_data>(col_b.data).values.size(), 2);

    ASSERT_EQ(result.schema.children.size(), 2);
    EXPECT_EQ(result.schema.children[0].name(), "a");
    EXPECT_EQ(result.schema.children[1].name(), "b");
}

TEST(IcebergParquetReader, MissingColumnNullFill) {
    // Write with {a: int32 (id=1)}, 3 rows.
    struct_type write_schema;
    write_schema.fields.push_back(
      nested_field::create(
        1, "a", field_required::yes, primitive_type{int_type{}}));

    auto pq_schema = iceberg_to_write_schema(write_schema);

    chunked_vector<sp::group_value> rows;
    rows.push_back(record(sp::int32_value{1}));
    rows.push_back(record(sp::int32_value{2}));
    rows.push_back(record(sp::int32_value{3}));

    auto file = write_to_iobuf(std::move(pq_schema), std::move(rows));

    // Read with {a: int32 (id=1), b: int64 (id=2)} -- b not in file
    struct_type read_schema;
    read_schema.fields.push_back(
      nested_field::create(
        1, "a", field_required::yes, primitive_type{int_type{}}));
    read_schema.fields.push_back(
      nested_field::create(
        2, "b", field_required::no, primitive_type{long_type{}}));

    sp::iobuf_file_io io(std::move(file));
    auto result = read_parquet(read_schema, io).get();

    ASSERT_EQ(result.row_groups.size(), 1);
    const auto& batch = result.row_groups[0];
    ASSERT_EQ(batch.columns.size(), 2);
    EXPECT_EQ(batch.num_rows, 3);

    // Column a: [1, 2, 3]
    const auto& col_a = batch.columns[0];
    ASSERT_TRUE(std::holds_alternative<sp::column_array::i32_data>(col_a.data));
    const auto& a_vals = std::get<sp::column_array::i32_data>(col_a.data);
    ASSERT_EQ(a_vals.values.size(), 3);
    EXPECT_EQ(a_vals.values[0], 1);
    EXPECT_EQ(a_vals.values[1], 2);
    EXPECT_EQ(a_vals.values[2], 3);

    // Column b: null-filled (length=3, all def_levels=0, no actual values)
    const auto& col_b = batch.columns[1];
    ASSERT_TRUE(std::holds_alternative<sp::column_array::i64_data>(col_b.data));
    const auto& b_vals = std::get<sp::column_array::i64_data>(col_b.data);
    EXPECT_EQ(b_vals.values.size(), 0);
    EXPECT_EQ(col_b.length, 3);

    const auto& b_levels = batch.levels[1];
    ASSERT_EQ(b_levels.def_levels.size(), 3);
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(b_levels.def_levels[i], sp::def_level(0));
        EXPECT_EQ(b_levels.rep_levels[i], sp::rep_level(0));
    }

    ASSERT_EQ(result.schema.children.size(), 2);
    EXPECT_EQ(result.schema.children[0].name(), "a");
    EXPECT_EQ(result.schema.children[1].name(), "b");
    EXPECT_EQ(
      result.schema.children[1].repetition_type,
      sp::field_repetition_type::optional);
}

TEST(IcebergParquetReader, NullFillAssemblesRecords) {
    // Write with {a: int32 (id=1)}, 3 rows.
    struct_type write_schema;
    write_schema.fields.push_back(
      nested_field::create(
        1, "a", field_required::yes, primitive_type{int_type{}}));

    auto pq_schema = iceberg_to_write_schema(write_schema);

    chunked_vector<sp::group_value> rows;
    rows.push_back(record(sp::int32_value{1}));
    rows.push_back(record(sp::int32_value{2}));
    rows.push_back(record(sp::int32_value{3}));

    auto file = write_to_iobuf(std::move(pq_schema), std::move(rows));

    // Read with {a: int32 (id=1), b: int64 (id=2, optional)}
    struct_type read_schema;
    read_schema.fields.push_back(
      nested_field::create(
        1, "a", field_required::yes, primitive_type{int_type{}}));
    read_schema.fields.push_back(
      nested_field::create(
        2, "b", field_required::no, primitive_type{long_type{}}));

    sp::iobuf_file_io io(std::move(file));
    auto result = read_parquet(read_schema, io).get();

    ASSERT_EQ(result.row_groups.size(), 1);
    auto records = sp::assemble_records(result.schema, result.row_groups[0]);
    ASSERT_EQ(records.size(), 3);

    // Each record: {a: int32, b: null}
    for (int i = 0; i < 3; ++i) {
        ASSERT_EQ(records[i].size(), 2);
        // a field
        ASSERT_TRUE(
          std::holds_alternative<sp::int32_value>(records[i][0].field));
        EXPECT_EQ(std::get<sp::int32_value>(records[i][0].field).val, i + 1);
        // b field: null
        EXPECT_TRUE(
          std::holds_alternative<sp::null_value>(records[i][1].field));
    }
}

TEST(IcebergParquetReader, CompressedZstd) {
    struct_type schema;
    schema.fields.push_back(
      nested_field::create(
        1, "val", field_required::yes, primitive_type{int_type{}}));

    auto pq_schema = iceberg_to_write_schema(schema);

    chunked_vector<sp::group_value> rows;
    for (int32_t i = 0; i < 50; ++i) {
        rows.push_back(record(sp::int32_value{i}));
    }

    auto file = write_to_iobuf(
      std::move(pq_schema), std::move(rows), /*compress=*/true);

    sp::iobuf_file_io io(std::move(file));
    auto result = read_parquet(schema, io).get();

    ASSERT_EQ(result.row_groups.size(), 1);
    const auto& batch = result.row_groups[0];
    ASSERT_EQ(batch.columns.size(), 1);
    EXPECT_EQ(batch.num_rows, 50);

    const auto& col = batch.columns[0];
    ASSERT_TRUE(std::holds_alternative<sp::column_array::i32_data>(col.data));
    const auto& vals = std::get<sp::column_array::i32_data>(col.data);
    ASSERT_EQ(vals.values.size(), 50);
    for (int32_t i = 0; i < 50; ++i) {
        EXPECT_EQ(vals.values[i], i);
    }
}

// NOLINTEND(*magic-number*)

} // namespace
} // namespace iceberg
