/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iostream.h"
#include "serde/parquet/reader.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "serde/parquet/writer.h"

#include <gtest/gtest.h>

namespace serde::parquet {

namespace {

template<typename... Args>
schema_element
group_node(ss::sstring name, field_repetition_type rep_type, Args... args) {
    chunked_vector<schema_element> children;
    (children.push_back(std::move(args)), ...);
    return {
      .type = std::monostate{},
      .repetition_type = rep_type,
      .path = {std::move(name)},
      .children = std::move(children),
    };
}

schema_element leaf_node(
  ss::sstring name,
  field_repetition_type rep_type,
  physical_type ptype,
  logical_type ltype = logical_type{}) {
    return {
      .type = ptype,
      .repetition_type = rep_type,
      .path = {std::move(name)},
      .logical_type = ltype,
    };
}

template<typename... Args>
group_value record(Args... field) {
    chunked_vector<group_member> fields;
    (fields.push_back(group_member{std::move(field)}), ...);
    return fields;
}

iobuf write_to_iobuf(
  schema_element schema,
  chunked_vector<group_value> rows,
  bool compress = false) {
    iobuf file;
    writer w(
      {.schema = std::move(schema), .compress = compress},
      make_iobuf_ref_output_stream(file));
    w.init().get();
    for (auto& row : rows) {
        w.write_row(std::move(row)).get();
    }
    w.close().get();
    return file;
}

} // namespace

// NOLINTBEGIN(*magic-number*)

TEST(ParquetReader, FlatRequiredInt32) {
    using field_repetition_type::required;
    auto schema = group_node(
      "root",
      required,
      leaf_node("a", required, i32_type()),
      leaf_node("b", required, i64_type()));

    chunked_vector<group_value> rows;
    rows.push_back(record(int32_value{1}, int64_value{10}));
    rows.push_back(record(int32_value{2}, int64_value{20}));
    rows.push_back(record(int32_value{3}, int64_value{30}));

    auto file = write_to_iobuf(std::move(schema), std::move(rows));
    auto records = read_file_as_records(std::move(file)).get();

    ASSERT_EQ(records.size(), 3);
    EXPECT_EQ(records[0], record(int32_value{1}, int64_value{10}));
    EXPECT_EQ(records[1], record(int32_value{2}, int64_value{20}));
    EXPECT_EQ(records[2], record(int32_value{3}, int64_value{30}));
}

TEST(ParquetReader, OptionalWithNulls) {
    using field_repetition_type::optional;
    using field_repetition_type::required;
    auto schema = group_node(
      "root",
      required,
      leaf_node("a", optional, i32_type()),
      leaf_node("b", optional, i64_type()));

    chunked_vector<group_value> rows;
    rows.push_back(record(int32_value{42}, null_value{}));
    rows.push_back(record(null_value{}, int64_value{99}));
    rows.push_back(record(int32_value{7}, int64_value{88}));

    auto file = write_to_iobuf(std::move(schema), std::move(rows));
    auto records = read_file_as_records(std::move(file)).get();

    ASSERT_EQ(records.size(), 3);
    EXPECT_EQ(records[0], record(int32_value{42}, null_value{}));
    EXPECT_EQ(records[1], record(null_value{}, int64_value{99}));
    EXPECT_EQ(records[2], record(int32_value{7}, int64_value{88}));
}

TEST(ParquetReader, CompressedZstd) {
    using field_repetition_type::required;
    auto schema = group_node(
      "root", required, leaf_node("val", required, i32_type()));

    chunked_vector<group_value> rows;
    for (int i = 0; i < 50; ++i) {
        rows.push_back(record(int32_value{i}));
    }

    auto file = write_to_iobuf(
      std::move(schema), std::move(rows), /*compress=*/true);
    auto records = read_file_as_records(std::move(file)).get();

    ASSERT_EQ(records.size(), 50);
    for (int i = 0; i < 50; ++i) {
        EXPECT_EQ(records[i], record(int32_value{i}));
    }
}

TEST(ParquetReader, StringColumn) {
    using field_repetition_type::required;
    auto schema = group_node(
      "root",
      required,
      leaf_node("name", required, byte_array_type{}, string_type()));

    chunked_vector<group_value> rows;
    rows.push_back(record(byte_array_value{iobuf::from("alice")}));
    rows.push_back(record(byte_array_value{iobuf::from("bob")}));
    rows.push_back(record(byte_array_value{iobuf::from("charlie")}));

    auto file = write_to_iobuf(std::move(schema), std::move(rows));
    auto records = read_file_as_records(std::move(file)).get();

    ASSERT_EQ(records.size(), 3);
    EXPECT_EQ(records[0], record(byte_array_value{iobuf::from("alice")}));
    EXPECT_EQ(records[1], record(byte_array_value{iobuf::from("bob")}));
    EXPECT_EQ(records[2], record(byte_array_value{iobuf::from("charlie")}));
}

TEST(ParquetReader, NestedOptionalStruct) {
    using field_repetition_type::optional;
    using field_repetition_type::required;
    auto schema = group_node(
      "root",
      required,
      group_node(
        "inner",
        optional,
        leaf_node("x", required, i32_type()),
        leaf_node("y", required, i32_type())),
      leaf_node("z", required, i64_type()));

    chunked_vector<group_value> rows;
    rows.push_back(
      record(value(record(int32_value{1}, int32_value{2})), int64_value{100}));
    rows.push_back(record(null_value{}, int64_value{200}));

    auto file = write_to_iobuf(std::move(schema), std::move(rows));
    auto records = read_file_as_records(std::move(file)).get();

    ASSERT_EQ(records.size(), 2);
    EXPECT_EQ(
      records[0],
      record(value(record(int32_value{1}, int32_value{2})), int64_value{100}));
    EXPECT_EQ(records[1], record(null_value{}, int64_value{200}));
}

TEST(ParquetReader, ColumnarAccess) {
    using field_repetition_type::required;
    auto schema = group_node(
      "root",
      required,
      leaf_node("a", required, i32_type()),
      leaf_node("b", required, i64_type()));

    chunked_vector<group_value> rows;
    for (int i = 0; i < 10; ++i) {
        rows.push_back(record(int32_value{i}, int64_value{i * 100}));
    }

    auto file = write_to_iobuf(std::move(schema), std::move(rows));
    auto result = read_file(std::move(file)).get();

    EXPECT_EQ(result.metadata.version, 2);
    EXPECT_EQ(result.metadata.num_rows, 10);
    ASSERT_EQ(result.row_groups.size(), 1);
    auto& batch = result.row_groups[0];
    EXPECT_EQ(batch.num_rows, 10);
    ASSERT_EQ(batch.columns.size(), 2);

    auto* col_a = std::get_if<column_array::i32_data>(&batch.columns[0].data);
    ASSERT_NE(col_a, nullptr);
    ASSERT_EQ(col_a->values.size(), 10);
    EXPECT_EQ(col_a->values[0], 0);
    EXPECT_EQ(col_a->values[9], 9);

    auto* col_b = std::get_if<column_array::i64_data>(&batch.columns[1].data);
    ASSERT_NE(col_b, nullptr);
    ASSERT_EQ(col_b->values.size(), 10);
    EXPECT_EQ(col_b->values[0], 0);
    EXPECT_EQ(col_b->values[9], 900);
}

TEST(ParquetReader, ColumnProjection) {
    using field_repetition_type::required;
    auto schema = group_node(
      "root",
      required,
      leaf_node("a", required, i32_type()),
      leaf_node("b", required, i64_type()),
      leaf_node("c", required, f64_type()));

    chunked_vector<group_value> rows;
    for (int i = 0; i < 5; ++i) {
        rows.push_back(
          record(int32_value{i}, int64_value{i * 10}, float64_value{i * 1.5}));
    }

    auto file = write_to_iobuf(std::move(schema), std::move(rows));

    // Project only column "b"
    reader_options opts;
    chunked_vector<ss::sstring> proj_path;
    proj_path.push_back("b");
    opts.column_projection.push_back(std::move(proj_path));

    auto result = read_file(std::move(file), std::move(opts)).get();

    ASSERT_EQ(result.row_groups.size(), 1);
    auto& batch = result.row_groups[0];
    // Only one column should be decoded
    ASSERT_EQ(batch.columns.size(), 1);
    auto* col_b = std::get_if<column_array::i64_data>(&batch.columns[0].data);
    ASSERT_NE(col_b, nullptr);
    ASSERT_EQ(col_b->values.size(), 5);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(col_b->values[i], i * 10);
    }
}

TEST(ParquetReader, ReadAsRecordsWithProjection) {
    using field_repetition_type::required;
    auto schema = group_node(
      "root",
      required,
      leaf_node("a", required, i32_type()),
      leaf_node("b", required, i64_type()));

    chunked_vector<group_value> rows;
    rows.push_back(record(int32_value{1}, int64_value{10}));
    rows.push_back(record(int32_value{2}, int64_value{20}));

    auto file = write_to_iobuf(std::move(schema), std::move(rows));

    // Project only column "a" — should produce records with only "a"
    reader_options opts;
    chunked_vector<ss::sstring> proj_path;
    proj_path.push_back("a");
    opts.column_projection.push_back(std::move(proj_path));

    auto records = read_file_as_records(std::move(file), std::move(opts)).get();

    ASSERT_EQ(records.size(), 2);
    // Records should have only the projected column
    EXPECT_EQ(records[0], record(int32_value{1}));
    EXPECT_EQ(records[1], record(int32_value{2}));
}

// NOLINTEND(*magic-number*)

} // namespace serde::parquet
