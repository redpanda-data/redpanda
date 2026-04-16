/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/partition_key.h"
#include "iceberg/position_delete_file.h"
#include "iceberg/values.h"
#include "serde/parquet/reader.h"
#include "serde/parquet/value.h"

#include <gtest/gtest.h>

namespace iceberg {

namespace {

// Helper to build a group_value for comparison with reader output.
serde::parquet::group_value expected_row(ss::sstring file_path, int64_t pos) {
    serde::parquet::group_value row;
    row.push_back(
      serde::parquet::group_member{
        serde::parquet::byte_array_value{iobuf::from(file_path)}});
    row.push_back(
      serde::parquet::group_member{serde::parquet::int64_value{pos}});
    return row;
}

partition_key empty_partition() {
    return partition_key{std::make_unique<struct_value>()};
}

} // namespace

// NOLINTBEGIN(*magic-number*)

TEST(PositionDeleteFile, RoundTrip) {
    chunked_vector<position_delete_entry> deletes;
    deletes.push_back(
      {.file_path = uri{"s3://bucket/data/file_a.parquet"},
       .pos = 0,
       .partition = empty_partition()});
    deletes.push_back(
      {.file_path = uri{"s3://bucket/data/file_a.parquet"},
       .pos = 42,
       .partition = empty_partition()});
    deletes.push_back(
      {.file_path = uri{"s3://bucket/data/file_b.parquet"},
       .pos = 7,
       .partition = empty_partition()});

    auto result = write_position_delete_file(std::move(deletes)).get();

    EXPECT_EQ(
      result.manifest_entry.content_type,
      data_file_content_type::position_deletes);
    EXPECT_EQ(result.manifest_entry.file_format, data_file_format::parquet);
    EXPECT_EQ(result.manifest_entry.record_count, 3);
    EXPECT_GT(result.manifest_entry.file_size_bytes, 0);

    auto records
      = serde::parquet::read_file_as_records(std::move(result.file_data)).get();

    ASSERT_EQ(records.size(), 3);
    EXPECT_EQ(records[0], expected_row("s3://bucket/data/file_a.parquet", 0));
    EXPECT_EQ(records[1], expected_row("s3://bucket/data/file_a.parquet", 42));
    EXPECT_EQ(records[2], expected_row("s3://bucket/data/file_b.parquet", 7));
}

TEST(PositionDeleteFile, RoundTripCompressed) {
    chunked_vector<position_delete_entry> deletes;
    deletes.push_back(
      {.file_path = uri{"s3://bucket/data/file.parquet"},
       .pos = 100,
       .partition = empty_partition()});

    auto result
      = write_position_delete_file(std::move(deletes), /*compress=*/true).get();

    EXPECT_EQ(result.manifest_entry.record_count, 1);

    auto records
      = serde::parquet::read_file_as_records(std::move(result.file_data)).get();

    ASSERT_EQ(records.size(), 1);
    EXPECT_EQ(records[0], expected_row("s3://bucket/data/file.parquet", 100));
}

// NOLINTEND(*magic-number*)

} // namespace iceberg
