/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/stats_parquet.h"
#include "serde/parquet/encoding.h"
#include "serde/parquet/metadata.h"
#include "serde/parquet/schema.h"

#include <gtest/gtest.h>

namespace iceberg::conversion {
namespace {

using namespace serde::parquet;

// Build a minimal flattened schema with a root group and leaf columns.
// Each leaf gets a field_id starting from 1.
file_metadata make_metadata_with_schema(
  std::vector<std::pair<physical_type, std::optional<int32_t>>> leaves) {
    file_metadata meta;
    meta.version = 2;
    meta.num_rows = 0;
    meta.created_by = "test";

    // Root element (group, not a leaf)
    flattened_schema root;
    root.type = std::monostate{};
    root.repetition_type = field_repetition_type::required;
    root.name = "schema";
    root.num_children = static_cast<int32_t>(leaves.size());
    root.field_id = std::nullopt;
    root.logical_type = std::monostate{};
    meta.schema.push_back(std::move(root));

    int col_idx = 0;
    for (auto& [ptype, fid] : leaves) {
        flattened_schema leaf;
        leaf.type = ptype;
        leaf.repetition_type = field_repetition_type::optional;
        leaf.name = fmt::format("col_{}", col_idx);
        leaf.num_children = 0;
        leaf.field_id = fid;
        leaf.logical_type = std::monostate{};
        meta.schema.push_back(std::move(leaf));
        ++col_idx;
    }
    return meta;
}

column_chunk make_column_chunk(
  physical_type type,
  int64_t num_values,
  int64_t compressed_size,
  std::optional<statistics> stats = std::nullopt) {
    column_chunk cc;
    cc.meta_data.type = type;
    cc.meta_data.codec = compression_codec::uncompressed;
    cc.meta_data.num_values = num_values;
    cc.meta_data.total_uncompressed_size = compressed_size;
    cc.meta_data.total_compressed_size = compressed_size;
    cc.meta_data.data_page_offset = 0;
    cc.meta_data.stats = std::move(stats);
    return cc;
}

TEST(StatsParquetTest, SingleRowGroupWithStats) {
    auto meta = make_metadata_with_schema({
      {i32_type{}, 1},
      {i64_type{}, 2},
    });

    statistics stats_col0;
    stats_col0.null_count = 3;
    stats_col0.min = statistics::bound{
      .value = encode_for_stats(int32_value{10}), .is_exact = true};
    stats_col0.max = statistics::bound{
      .value = encode_for_stats(int32_value{100}), .is_exact = true};

    statistics stats_col1;
    stats_col1.null_count = 0;
    stats_col1.min = statistics::bound{
      .value = encode_for_stats(int64_value{-500}), .is_exact = true};
    stats_col1.max = statistics::bound{
      .value = encode_for_stats(int64_value{999}), .is_exact = true};

    row_group rg;
    rg.total_byte_size = 1024;
    rg.num_rows = 100;
    rg.file_offset = 0;
    rg.total_compressed_size = 512;
    rg.ordinal = 0;
    rg.columns.push_back(
      make_column_chunk(i32_type{}, 100, 200, std::move(stats_col0)));
    rg.columns.push_back(
      make_column_chunk(i64_type{}, 100, 300, std::move(stats_col1)));

    meta.row_groups.push_back(std::move(rg));
    meta.num_rows = 100;

    auto result = extract_iceberg_stats(meta);

    auto id1 = nested_field::id_t{1};
    auto id2 = nested_field::id_t{2};

    // column_sizes
    ASSERT_TRUE(result.column_sizes.has_value());
    EXPECT_EQ(result.column_sizes->at(id1), 200);
    EXPECT_EQ(result.column_sizes->at(id2), 300);

    // value_counts
    ASSERT_TRUE(result.value_counts.has_value());
    EXPECT_EQ(result.value_counts->at(id1), 100);
    EXPECT_EQ(result.value_counts->at(id2), 100);

    // null_value_counts
    ASSERT_TRUE(result.null_value_counts.has_value());
    EXPECT_EQ(result.null_value_counts->at(id1), 3);
    EXPECT_EQ(result.null_value_counts->at(id2), 0);

    // lower_bounds
    ASSERT_TRUE(result.lower_bounds.has_value());
    EXPECT_EQ(result.lower_bounds->at(id1), encode_for_stats(int32_value{10}));
    EXPECT_EQ(
      result.lower_bounds->at(id2), encode_for_stats(int64_value{-500}));

    // upper_bounds
    ASSERT_TRUE(result.upper_bounds.has_value());
    EXPECT_EQ(result.upper_bounds->at(id1), encode_for_stats(int32_value{100}));
    EXPECT_EQ(result.upper_bounds->at(id2), encode_for_stats(int64_value{999}));
}

TEST(StatsParquetTest, NoStatsPresent) {
    auto meta = make_metadata_with_schema({
      {i32_type{}, 1},
      {i64_type{}, 2},
    });

    row_group rg;
    rg.total_byte_size = 512;
    rg.num_rows = 50;
    rg.file_offset = 0;
    rg.total_compressed_size = 256;
    rg.ordinal = 0;
    rg.columns.push_back(make_column_chunk(i32_type{}, 50, 100));
    rg.columns.push_back(make_column_chunk(i64_type{}, 50, 150));

    meta.row_groups.push_back(std::move(rg));
    meta.num_rows = 50;

    auto result = extract_iceberg_stats(meta);

    auto id1 = nested_field::id_t{1};
    auto id2 = nested_field::id_t{2};

    ASSERT_TRUE(result.column_sizes.has_value());
    EXPECT_EQ(result.column_sizes->at(id1), 100);
    EXPECT_EQ(result.column_sizes->at(id2), 150);

    ASSERT_TRUE(result.value_counts.has_value());
    EXPECT_EQ(result.value_counts->at(id1), 50);
    EXPECT_EQ(result.value_counts->at(id2), 50);

    // null_value_counts should still be populated (just zero from no stats)
    ASSERT_TRUE(result.null_value_counts.has_value());

    // Bounds should be nullopt since no column had min/max
    EXPECT_FALSE(result.lower_bounds.has_value());
    EXPECT_FALSE(result.upper_bounds.has_value());
}

TEST(StatsParquetTest, MultipleRowGroupsMerging) {
    auto meta = make_metadata_with_schema({
      {i32_type{}, 1},
    });

    // Row group 0: values 10..200, 5 nulls
    statistics stats_rg0;
    stats_rg0.null_count = 5;
    stats_rg0.min = statistics::bound{
      .value = encode_for_stats(int32_value{10}), .is_exact = true};
    stats_rg0.max = statistics::bound{
      .value = encode_for_stats(int32_value{200}), .is_exact = true};

    row_group rg0;
    rg0.total_byte_size = 400;
    rg0.num_rows = 50;
    rg0.file_offset = 0;
    rg0.total_compressed_size = 200;
    rg0.ordinal = 0;
    rg0.columns.push_back(
      make_column_chunk(i32_type{}, 50, 200, std::move(stats_rg0)));

    // Row group 1: values 5..150, 3 nulls
    statistics stats_rg1;
    stats_rg1.null_count = 3;
    stats_rg1.min = statistics::bound{
      .value = encode_for_stats(int32_value{5}), .is_exact = true};
    stats_rg1.max = statistics::bound{
      .value = encode_for_stats(int32_value{150}), .is_exact = true};

    row_group rg1;
    rg1.total_byte_size = 300;
    rg1.num_rows = 40;
    rg1.file_offset = 400;
    rg1.total_compressed_size = 150;
    rg1.ordinal = 1;
    rg1.columns.push_back(
      make_column_chunk(i32_type{}, 40, 150, std::move(stats_rg1)));

    meta.row_groups.push_back(std::move(rg0));
    meta.row_groups.push_back(std::move(rg1));
    meta.num_rows = 90;

    auto result = extract_iceberg_stats(meta);
    auto id1 = nested_field::id_t{1};

    // Sizes and counts are summed
    ASSERT_TRUE(result.column_sizes.has_value());
    EXPECT_EQ(result.column_sizes->at(id1), 200 + 150);

    ASSERT_TRUE(result.value_counts.has_value());
    EXPECT_EQ(result.value_counts->at(id1), 50 + 40);

    ASSERT_TRUE(result.null_value_counts.has_value());
    EXPECT_EQ(result.null_value_counts->at(id1), 5 + 3);

    // Lower bound is byte-wise min(5, 10) = 5
    ASSERT_TRUE(result.lower_bounds.has_value());
    EXPECT_EQ(result.lower_bounds->at(id1), encode_for_stats(int32_value{5}));

    // Upper bound is byte-wise max(200, 150) = 200
    ASSERT_TRUE(result.upper_bounds.has_value());
    EXPECT_EQ(result.upper_bounds->at(id1), encode_for_stats(int32_value{200}));
}

TEST(StatsParquetTest, ColumnsWithoutFieldIdSkipped) {
    // Two leaves: one with field_id, one without
    auto meta = make_metadata_with_schema({
      {i32_type{}, std::nullopt},
      {i64_type{}, 5},
    });

    statistics stats;
    stats.null_count = 1;
    stats.min = statistics::bound{
      .value = encode_for_stats(int64_value{42}), .is_exact = true};
    stats.max = statistics::bound{
      .value = encode_for_stats(int64_value{42}), .is_exact = true};

    row_group rg;
    rg.total_byte_size = 256;
    rg.num_rows = 10;
    rg.file_offset = 0;
    rg.total_compressed_size = 128;
    rg.ordinal = 0;
    rg.columns.push_back(make_column_chunk(i32_type{}, 10, 50));
    rg.columns.push_back(
      make_column_chunk(i64_type{}, 10, 80, std::move(stats)));

    meta.row_groups.push_back(std::move(rg));
    meta.num_rows = 10;

    auto result = extract_iceberg_stats(meta);
    auto id5 = nested_field::id_t{5};

    // Only field_id 5 should appear
    ASSERT_TRUE(result.column_sizes.has_value());
    EXPECT_EQ(result.column_sizes->size(), 1);
    EXPECT_EQ(result.column_sizes->at(id5), 80);

    ASSERT_TRUE(result.lower_bounds.has_value());
    EXPECT_EQ(result.lower_bounds->size(), 1);
    EXPECT_EQ(result.lower_bounds->at(id5), encode_for_stats(int64_value{42}));
}

} // namespace
} // namespace iceberg::conversion
