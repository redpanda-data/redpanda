/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/equality_delete_file.h"
#include "serde/parquet/reader.h"
#include "serde/parquet/value.h"

#include <gtest/gtest.h>

namespace iceberg {

namespace {

serde::parquet::schema_element two_column_key_schema() {
    using serde::parquet::byte_array_type;
    using serde::parquet::field_repetition_type;
    using serde::parquet::i32_type;
    using serde::parquet::schema_element;
    using serde::parquet::string_type;

    chunked_vector<schema_element> children;
    children.push_back(
      schema_element{
        .type = i32_type{},
        .repetition_type = field_repetition_type::required,
        .path = {ss::sstring("id")},
        .field_id = 1,
      });
    children.push_back(
      schema_element{
        .type = byte_array_type{},
        .repetition_type = field_repetition_type::required,
        .path = {ss::sstring("name")},
        .field_id = 2,
        .logical_type = string_type{},
      });
    return schema_element{
      .type = std::monostate{},
      .repetition_type = field_repetition_type::required,
      .path = {ss::sstring("equality_deletes")},
      .children = std::move(children),
    };
}

serde::parquet::group_value make_row(int32_t id, const ss::sstring& name) {
    serde::parquet::group_value row;
    row.push_back(
      serde::parquet::group_member{serde::parquet::int32_value{id}});
    row.push_back(
      serde::parquet::group_member{
        serde::parquet::byte_array_value{iobuf::from(name)}});
    return row;
}

} // namespace

// NOLINTBEGIN(*magic-number*)

TEST(EqualityDeleteFile, RoundTrip) {
    chunked_vector<serde::parquet::group_value> rows;
    rows.push_back(make_row(1, "alice"));
    rows.push_back(make_row(2, "bob"));
    rows.push_back(make_row(3, "charlie"));

    chunked_vector<nested_field::id_t> field_ids;
    field_ids.push_back(nested_field::id_t{1});
    field_ids.push_back(nested_field::id_t{2});

    equality_delete_options opts{
      .parquet_schema = two_column_key_schema(),
      .equality_field_ids = std::move(field_ids),
    };

    auto result
      = write_equality_delete_file(std::move(opts), std::move(rows)).get();

    EXPECT_EQ(
      result.manifest_entry.content_type,
      data_file_content_type::equality_deletes);
    EXPECT_EQ(result.manifest_entry.file_format, data_file_format::parquet);
    EXPECT_EQ(result.manifest_entry.record_count, 3);
    EXPECT_GT(result.manifest_entry.file_size_bytes, 0);

    ASSERT_TRUE(result.manifest_entry.equality_ids.has_value());
    auto& eq_ids = result.manifest_entry.equality_ids.value();
    ASSERT_EQ(eq_ids.size(), 2);
    EXPECT_EQ(eq_ids[0], nested_field::id_t{1});
    EXPECT_EQ(eq_ids[1], nested_field::id_t{2});

    auto records
      = serde::parquet::read_file_as_records(std::move(result.file_data)).get();

    ASSERT_EQ(records.size(), 3);
    EXPECT_EQ(records[0], make_row(1, "alice"));
    EXPECT_EQ(records[1], make_row(2, "bob"));
    EXPECT_EQ(records[2], make_row(3, "charlie"));
}

TEST(EqualityDeleteFile, RoundTripCompressed) {
    chunked_vector<serde::parquet::group_value> rows;
    rows.push_back(make_row(42, "eve"));

    chunked_vector<nested_field::id_t> field_ids;
    field_ids.push_back(nested_field::id_t{1});
    field_ids.push_back(nested_field::id_t{2});

    equality_delete_options opts{
      .parquet_schema = two_column_key_schema(),
      .equality_field_ids = std::move(field_ids),
      .compress = true,
    };

    auto result
      = write_equality_delete_file(std::move(opts), std::move(rows)).get();

    EXPECT_EQ(result.manifest_entry.record_count, 1);

    auto records
      = serde::parquet::read_file_as_records(std::move(result.file_data)).get();

    ASSERT_EQ(records.size(), 1);
    EXPECT_EQ(records[0], make_row(42, "eve"));
}

// NOLINTEND(*magic-number*)

} // namespace iceberg
