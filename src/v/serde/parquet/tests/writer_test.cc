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
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "serde/parquet/writer.h"

#include <gtest/gtest.h>

namespace serde::parquet {
namespace {

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

schema_element simple_schema() {
    chunked_vector<schema_element> children;
    children.push_back(
      leaf_node("data", field_repetition_type::required, byte_array_type{}));
    return {
      .repetition_type = field_repetition_type::required,
      .path = {"root"},
      .children = std::move(children),
    };
}

group_value make_row(size_t data_size) {
    chunked_vector<group_member> fields;
    fields.push_back(
      group_member{byte_array_value{iobuf::from(std::string(data_size, 'x'))}});
    return fields;
}

} // namespace

schema_element two_column_schema() {
    chunked_vector<schema_element> children;
    children.push_back(
      leaf_node("str_col", field_repetition_type::required, byte_array_type{}));
    children.push_back(
      leaf_node("int_col", field_repetition_type::required, i32_type{}));
    return {
      .repetition_type = field_repetition_type::required,
      .path = {"root"},
      .children = std::move(children),
    };
}

group_value make_two_col_row(ss::sstring str_val, int32_t int_val) {
    chunked_vector<group_member> fields;
    fields.push_back(
      group_member{byte_array_value{iobuf::from(std::move(str_val))}});
    fields.push_back(group_member{int32_value{int_val}});
    return fields;
}

// NOLINTBEGIN(*magic-number*)

TEST(ParquetWriter, FlushesRowGroupWhenSizeExceeded) {
    constexpr size_t row_group_size = 1_KiB;
    constexpr size_t row_size = 256;
    constexpr size_t rows_to_write = 5;

    iobuf file;
    writer w(
      {
        .schema = simple_schema(),
        .row_group_size = row_group_size,
      },
      make_iobuf_ref_output_stream(file));
    w.init().get();

    file_stats stats;
    for (size_t i = 0; i < rows_to_write; ++i) {
        stats = w.write_row(make_row(row_size)).get();
    }
    EXPECT_GE(stats.flushed_size, row_group_size);
    EXPECT_LT(stats.buffered_size, row_group_size);

    w.close().get();
}

TEST(ParquetWriter, NoEarlyFlushWhenUnderLimit) {
    constexpr size_t row_group_size = 64_MiB;
    constexpr size_t row_size = 256;
    constexpr size_t rows_to_write = 5;

    iobuf file;
    writer w(
      {
        .schema = simple_schema(),
        .row_group_size = row_group_size,
      },
      make_iobuf_ref_output_stream(file));
    w.init().get();

    file_stats stats;
    for (size_t i = 0; i < rows_to_write; ++i) {
        stats = w.write_row(make_row(row_size)).get();
    }

    // flushed_size should only be the magic bytes (PAR1 = 4 bytes)
    EXPECT_EQ(stats.flushed_size, 4);
    EXPECT_GE(stats.buffered_size, row_size * rows_to_write);

    w.close().get();
}

TEST(ParquetWriter, StatsTruncation) {
    constexpr int32_t max_stats_len = 16;

    iobuf file;
    writer w(
      {
        .schema = two_column_schema(),
        .max_stats_truncate_length = max_stats_len,
      },
      make_iobuf_ref_output_stream(file));
    w.init().get();

    // Write rows with long strings (50 bytes) that should be truncated.
    // Use distinct min/max values to exercise both truncation paths.
    std::string long_min(50, 'A');
    std::string long_max(50, 'Z');
    w.write_row(make_two_col_row(ss::sstring(long_min), 1)).get();
    w.write_row(make_two_col_row(ss::sstring(long_max), 100)).get();

    auto metadata = w.close().get();

    ASSERT_EQ(metadata.row_groups.size(), 1);
    auto& rg = metadata.row_groups[0];
    ASSERT_GE(rg.columns.size(), 2);

    // Column 0: byte_array (string) -- should be truncated
    auto& str_stats = rg.columns[0].meta_data.stats;
    ASSERT_TRUE(str_stats.has_value());
    ASSERT_TRUE(str_stats->min.has_value());
    ASSERT_TRUE(str_stats->max.has_value());
    EXPECT_LE(
      static_cast<int32_t>(str_stats->min->value.size_bytes()), max_stats_len);
    EXPECT_FALSE(str_stats->min->is_exact);
    EXPECT_LE(
      static_cast<int32_t>(str_stats->max->value.size_bytes()), max_stats_len);
    EXPECT_FALSE(str_stats->max->is_exact);

    // Column 1: int32 -- should NOT be truncated (always small)
    auto& int_stats = rg.columns[1].meta_data.stats;
    ASSERT_TRUE(int_stats.has_value());
    ASSERT_TRUE(int_stats->min.has_value());
    ASSERT_TRUE(int_stats->max.has_value());
    EXPECT_TRUE(int_stats->min->is_exact);
    EXPECT_TRUE(int_stats->max->is_exact);
}

TEST(ParquetWriter, StatsNotTruncatedWhenShort) {
    constexpr int32_t max_stats_len = 16;

    iobuf file;
    writer w(
      {
        .schema = simple_schema(),
        .max_stats_truncate_length = max_stats_len,
      },
      make_iobuf_ref_output_stream(file));
    w.init().get();

    // Write short strings (10 bytes) -- should not be truncated
    w.write_row(make_row(10)).get();

    auto metadata = w.close().get();

    ASSERT_EQ(metadata.row_groups.size(), 1);
    auto& stats = metadata.row_groups[0].columns[0].meta_data.stats;
    ASSERT_TRUE(stats.has_value());
    ASSERT_TRUE(stats->min.has_value());
    ASSERT_TRUE(stats->max.has_value());
    EXPECT_EQ(static_cast<int32_t>(stats->min->value.size_bytes()), 10);
    EXPECT_TRUE(stats->min->is_exact);
    EXPECT_EQ(static_cast<int32_t>(stats->max->value.size_bytes()), 10);
    EXPECT_TRUE(stats->max->is_exact);
}

TEST(ParquetWriter, StatsTruncateMaxAllFF) {
    constexpr int32_t max_stats_len = 4;

    iobuf file;
    writer w(
      {
        .schema = simple_schema(),
        .max_stats_truncate_length = max_stats_len,
      },
      make_iobuf_ref_output_stream(file));
    w.init().get();

    // Create a string where the first max_stats_len bytes are all 0xFF,
    // which cannot be incremented for max truncation.
    std::string all_ff(10, '\xFF');
    chunked_vector<group_member> fields;
    fields.push_back(
      group_member{byte_array_value{iobuf::from(std::move(all_ff))}});
    w.write_row(group_value{std::move(fields)}).get();

    auto metadata = w.close().get();

    ASSERT_EQ(metadata.row_groups.size(), 1);
    auto& stats = metadata.row_groups[0].columns[0].meta_data.stats;
    ASSERT_TRUE(stats.has_value());
    ASSERT_TRUE(stats->max.has_value());
    // When all prefix bytes are 0xFF, truncation keeps the full prefix
    // and marks it as exact since it can't form a tighter upper bound.
    EXPECT_EQ(
      static_cast<int32_t>(stats->max->value.size_bytes()), max_stats_len);
    EXPECT_TRUE(stats->max->is_exact);
}

// NOLINTEND(*magic-number*)

} // namespace serde::parquet
