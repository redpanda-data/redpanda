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

#include "base/vlog.h"
#include "bytes/iostream.h"
#include "serde/parquet/column_stats_collector.h"
#include "serde/parquet/column_writer.h"
#include "serde/parquet/encoding.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "serde/parquet/writer.h"

#include <seastar/core/memory.hh>
#include <seastar/util/log.hh>

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <random>
#include <type_traits>

namespace serde::parquet {
namespace {

ss::logger test_log("parquet_writer_test");

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

schema_element single_col_schema(physical_type ptype) {
    chunked_vector<schema_element> children;
    children.push_back(
      leaf_node("col", field_repetition_type::required, ptype));
    return {
      .repetition_type = field_repetition_type::required,
      .path = {"root"},
      .children = std::move(children),
    };
}

schema_element single_string_schema() {
    chunked_vector<schema_element> children;
    children.push_back(leaf_node(
      "col",
      field_repetition_type::required,
      byte_array_type{},
      string_type{}));
    return {
      .repetition_type = field_repetition_type::required,
      .path = {"root"},
      .children = std::move(children),
    };
}

group_value make_string_row(std::string s) {
    chunked_vector<group_member> fields;
    fields.push_back(group_member{byte_array_value{iobuf::from(std::move(s))}});
    return fields;
}

schema_element n_byte_array_schema(size_t column_count) {
    chunked_vector<schema_element> children;
    for (size_t i = 0; i < column_count; ++i) {
        children.push_back(leaf_node(
          fmt::format("c{}", i),
          field_repetition_type::required,
          byte_array_type{}));
    }
    return {
      .repetition_type = field_repetition_type::required,
      .path = {"root"},
      .children = std::move(children),
    };
}
group_value n_byte_array_row(size_t column_count) {
    chunked_vector<group_member> fields;
    for (size_t i = 0; i < column_count; ++i) {
        fields.push_back(group_member{byte_array_value{iobuf::from("x")}});
    }
    return group_value{std::move(fields)};
}

/// Writes `values` to a single-column parquet file and verifies that the
/// column chunk min/max stats match what column_stats_collector computes.
/// Uses max_stats_truncate_length=0 (no truncation) to get exact stats.
template<typename ValueType, auto Comparator>
void check_col_stats(
  physical_type ptype, const std::vector<ValueType>& values) {
    static_assert(std::is_trivially_copyable_v<ValueType>);

    column_stats_collector<ValueType, Comparator> oracle;
    for (auto v : values) {
        oracle.record_value(v);
    }

    iobuf file;
    writer w(
      {.schema = single_col_schema(ptype), .max_stats_truncate_length = 0},
      make_iobuf_ref_output_stream(file));
    w.init().get();
    for (auto v : values) {
        chunked_vector<group_member> fields;
        fields.push_back(group_member{v});
        w.write_row(group_value{std::move(fields)}).get();
    }
    auto metadata = w.close().get();

    ASSERT_FALSE(metadata.row_groups.empty());
    auto& stats = metadata.row_groups[0].columns[0].meta_data.stats;
    ASSERT_TRUE(stats.has_value());

    auto oracle_min = oracle.min();
    if (oracle_min) {
        ASSERT_TRUE(stats->min.has_value());
        EXPECT_EQ(stats->min->value, encode_for_stats(*oracle_min));
        EXPECT_TRUE(stats->min->is_exact);
    } else {
        EXPECT_FALSE(stats->min.has_value());
    }

    auto oracle_max = oracle.max();
    if (oracle_max) {
        ASSERT_TRUE(stats->max.has_value());
        EXPECT_EQ(stats->max->value, encode_for_stats(*oracle_max));
        EXPECT_TRUE(stats->max->is_exact);
    } else {
        EXPECT_FALSE(stats->max.has_value());
    }
}

/// Same as check_col_stats but for byte_array columns, where values are
/// passed as strings to avoid iobuf's move-only restriction.
void check_byte_array_stats(const std::vector<std::string>& values) {
    column_stats_collector<byte_array_value, ordering::byte_array> oracle;
    for (const auto& s : values) {
        byte_array_value bav{iobuf::from(s)};
        oracle.record_value(bav);
    }

    iobuf file;
    writer w(
      {.schema = single_col_schema(byte_array_type{}),
       .max_stats_truncate_length = 0},
      make_iobuf_ref_output_stream(file));
    w.init().get();
    for (const auto& s : values) {
        chunked_vector<group_member> fields;
        fields.push_back(group_member{byte_array_value{iobuf::from(s)}});
        w.write_row(group_value{std::move(fields)}).get();
    }
    auto metadata = w.close().get();

    ASSERT_FALSE(metadata.row_groups.empty());
    auto& stats = metadata.row_groups[0].columns[0].meta_data.stats;
    ASSERT_TRUE(stats.has_value());

    auto& oracle_min = oracle.min();
    if (oracle_min) {
        ASSERT_TRUE(stats->min.has_value());
        EXPECT_EQ(stats->min->value, encode_for_stats(*oracle_min));
        EXPECT_TRUE(stats->min->is_exact);
    } else {
        EXPECT_FALSE(stats->min.has_value());
    }

    auto& oracle_max = oracle.max();
    if (oracle_max) {
        ASSERT_TRUE(stats->max.has_value());
        EXPECT_EQ(stats->max->value, encode_for_stats(*oracle_max));
        EXPECT_TRUE(stats->max->is_exact);
    } else {
        EXPECT_FALSE(stats->max.has_value());
    }
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

TEST(ParquetWriter, BufferedMemoryZeroAfterFlush) {
    iobuf file;
    writer w({.schema = simple_schema()}, make_iobuf_ref_output_stream(file));
    w.init().get();
    for (size_t i = 0; i < 3; ++i) {
        w.write_row(make_row(/*data_size=*/64)).get();
    }
    EXPECT_GT(w.stats().buffered_size, 0);
    w.flush_row_group().get();
    EXPECT_EQ(w.stats().buffered_size, 0);
    w.close().get();
}

TEST(ParquetWriter, ColumnMemoryEstimateCoversActual) {
#ifdef SEASTAR_DEFAULT_ALLOCATOR
    GTEST_SKIP() << "memory::stats() reports fixed values under the system "
                    "allocator, and the sanitizers that select it intercept "
                    "malloc, so there is nothing truthful to measure";
#endif
    auto measure = [](size_t column_count) -> long {
        auto before = seastar::memory::stats().allocated_memory();
        iobuf file;
        writer w(
          {.schema = n_byte_array_schema(column_count)},
          make_iobuf_ref_output_stream(file));
        w.init().get();
        w.write_row(n_byte_array_row(column_count)).get();
        // Read before the writer goes out of scope and frees its buffers.
        auto used = static_cast<long>(
          seastar::memory::stats().allocated_memory() - before);
        w.close().get();
        return used;
    };
    // Warm up the allocator. This first call is discarded: growing the pools
    // leaves spans checked out for good, which is not a column's cost.
    measure(1000);

    // Then take two measurements of different column counts, cutting out any
    // fixed per-writer cost to just get a per column cost.
    auto per_leaf = static_cast<size_t>((measure(2000) - measure(1000)) / 1000);
    auto estimate = estimated_column_memory();
    vlog(
      test_log.info,
      "estimated_column_memory() needs at least {} to cover a byte_array "
      "column; it is {}",
      per_leaf,
      estimate);

    EXPECT_GE(estimate, per_leaf)
      << "estimate " << estimate << " < measured per-leaf " << per_leaf
      << "; raise estimated_column_memory()";
    EXPECT_LE(estimate, 2 * per_leaf)
      << "estimate " << estimate << " >> measured per-leaf " << per_leaf;
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
    // When all prefix bytes are 0xFF, no valid truncated upper bound exists,
    // so max stats are omitted entirely.
    EXPECT_FALSE(stats->max.has_value());
}

TEST(ParquetWriter, StatsRandomInt32) {
    std::mt19937 rng(42);
    std::uniform_int_distribution<int32_t> dist(
      std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
    std::vector<int32_value> values;
    values.reserve(1000);
    for (int i = 0; i < 1000; ++i) {
        values.push_back(int32_value{dist(rng)});
    }
    check_col_stats<int32_value, ordering::int32>(i32_type{}, values);
}

TEST(ParquetWriter, StatsRandomInt64) {
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<int64_t> dist(
      std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
    std::vector<int64_value> values;
    values.reserve(1000);
    for (int i = 0; i < 1000; ++i) {
        values.push_back(int64_value{dist(rng)});
    }
    check_col_stats<int64_value, ordering::int64>(i64_type{}, values);
}

TEST(ParquetWriter, StatsRandomFloat32) {
    std::mt19937 rng(42);
    std::uniform_real_distribution<float> dist(-1e30f, 1e30f);
    std::vector<float32_value> values;
    values.reserve(1002);
    for (int i = 0; i < 1000; ++i) {
        values.push_back(float32_value{dist(rng)});
    }
    // Include zeros to exercise the zero-normalization path
    values.push_back(float32_value{0.0f});
    values.push_back(float32_value{-0.0f});
    check_col_stats<float32_value, ordering::float32>(f32_type{}, values);
}

TEST(ParquetWriter, StatsRandomFloat64) {
    std::mt19937_64 rng(42);
    std::uniform_real_distribution<double> dist(-1e300, 1e300);
    std::vector<float64_value> values;
    values.reserve(1002);
    for (int i = 0; i < 1000; ++i) {
        values.push_back(float64_value{dist(rng)});
    }
    values.push_back(float64_value{0.0});
    values.push_back(float64_value{-0.0});
    check_col_stats<float64_value, ordering::float64>(f64_type{}, values);
}

TEST(ParquetWriter, StatsRandomByteArray) {
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> len_dist(0, 32);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    std::vector<std::string> values;
    values.reserve(100);
    for (int i = 0; i < 100; ++i) {
        int len = len_dist(rng);
        std::string s(len, '\0');
        for (auto& c : s) {
            c = static_cast<char>(byte_dist(rng));
        }
        values.push_back(std::move(s));
    }
    check_byte_array_stats(values);
}

TEST(ParquetWriter, StatsEdgeInt32) {
    using lim = std::numeric_limits<int32_t>;
    // Single minimum value: min == max == INT32_MIN
    check_col_stats<int32_value, ordering::int32>(
      i32_type{}, {int32_value{lim::min()}});
    // Single maximum value: min == max == INT32_MAX
    check_col_stats<int32_value, ordering::int32>(
      i32_type{}, {int32_value{lim::max()}});
    // Both extremes: min == INT32_MIN, max == INT32_MAX
    check_col_stats<int32_value, ordering::int32>(
      i32_type{}, {int32_value{lim::min()}, int32_value{lim::max()}});
}

TEST(ParquetWriter, StatsEdgeInt64) {
    using lim = std::numeric_limits<int64_t>;
    check_col_stats<int64_value, ordering::int64>(
      i64_type{}, {int64_value{lim::min()}});
    check_col_stats<int64_value, ordering::int64>(
      i64_type{}, {int64_value{lim::max()}});
    check_col_stats<int64_value, ordering::int64>(
      i64_type{}, {int64_value{lim::min()}, int64_value{lim::max()}});
}

TEST(ParquetWriter, StatsEdgeFloat32) {
    using lim = std::numeric_limits<float>;
    // All NaN: no min/max stats
    check_col_stats<float32_value, ordering::float32>(
      f32_type{}, {float32_value{lim::quiet_NaN()}});
    // Infinities
    check_col_stats<float32_value, ordering::float32>(
      f32_type{},
      {float32_value{lim::infinity()}, float32_value{-lim::infinity()}});
    // Zero normalization: both +0 and -0 present; min stored as -0, max as +0
    check_col_stats<float32_value, ordering::float32>(
      f32_type{}, {float32_value{0.0f}, float32_value{-0.0f}});
    // Finite extremes
    check_col_stats<float32_value, ordering::float32>(
      f32_type{}, {float32_value{lim::lowest()}, float32_value{lim::max()}});
}

TEST(ParquetWriter, StatsEdgeFloat64) {
    using lim = std::numeric_limits<double>;
    check_col_stats<float64_value, ordering::float64>(
      f64_type{}, {float64_value{lim::quiet_NaN()}});
    check_col_stats<float64_value, ordering::float64>(
      f64_type{},
      {float64_value{lim::infinity()}, float64_value{-lim::infinity()}});
    check_col_stats<float64_value, ordering::float64>(
      f64_type{}, {float64_value{0.0}, float64_value{-0.0}});
    check_col_stats<float64_value, ordering::float64>(
      f64_type{}, {float64_value{lim::lowest()}, float64_value{lim::max()}});
}

TEST(ParquetWriter, StatsEdgeByteArray) {
    // Empty string: min == max == ""
    check_byte_array_stats({""});
    // Single all-zero byte
    check_byte_array_stats({std::string(1, '\x00')});
    // Single all-0xFF byte
    check_byte_array_stats({std::string(1, '\xFF')});
    // Long all-0xFF: without truncation, stats are exact
    check_byte_array_stats({std::string(32, '\xFF')});
    // Mixed values including empty and binary content
    check_byte_array_stats({"", "abc", "\x01\x02\x03"});
}

TEST(ParquetWriter, StatsNoTruncationForFixedSizeTypes) {
    // Fixed-size column types (int32=4B, int64=8B, etc.) must never be
    // truncated regardless of max_stats_truncate_length, because truncating
    // their fixed-size plain encoding produces an invalid stat.
    constexpr int32_t tiny = 1; // smaller than any fixed-size encoding

    auto check =
      [&](physical_type ptype, group_value row, int32_t expected_stat_bytes) {
          iobuf file;
          writer w(
            {.schema = single_col_schema(ptype),
             .max_stats_truncate_length = tiny},
            make_iobuf_ref_output_stream(file));
          w.init().get();
          w.write_row(std::move(row)).get();
          auto metadata = w.close().get();
          auto& stats = metadata.row_groups[0].columns[0].meta_data.stats;
          ASSERT_TRUE(stats.has_value());
          ASSERT_TRUE(stats->min.has_value());
          ASSERT_TRUE(stats->max.has_value());
          EXPECT_TRUE(stats->min->is_exact);
          EXPECT_TRUE(stats->max->is_exact);
          EXPECT_EQ(
            static_cast<int32_t>(stats->min->value.size_bytes()),
            expected_stat_bytes);
          EXPECT_EQ(
            static_cast<int32_t>(stats->max->value.size_bytes()),
            expected_stat_bytes);
      };

    auto row32 = [](int32_t v) {
        chunked_vector<group_member> f;
        f.push_back(group_member{int32_value{v}});
        return group_value{std::move(f)};
    };
    auto row64 = [](int64_t v) {
        chunked_vector<group_member> f;
        f.push_back(group_member{int64_value{v}});
        return group_value{std::move(f)};
    };

    check(i32_type{}, row32(42), 4);
    check(i64_type{}, row64(1234567890LL), 8);
}

TEST(ParquetWriter, StatsTruncationUtf8CharBoundary) {
    // "hello€world" = 5 + 3 + 5 = 13 bytes. With max_stats_truncate_length=7,
    // a byte-level truncation would produce "hello\xE2\x82" (7 bytes, mid-€).
    // The UTF-8-aware truncation must stop at the last complete codepoint,
    // giving "hello" (5 bytes) for min and a valid UTF-8 upper bound for max.
    constexpr int32_t max_len = 7;
    std::string s = "hello\xE2\x82\xAC"
                    "world"; // "hello€world", 13 bytes

    iobuf file;
    writer w(
      {.schema = single_string_schema(), .max_stats_truncate_length = max_len},
      make_iobuf_ref_output_stream(file));
    w.init().get();
    w.write_row(make_string_row(s)).get();
    w.write_row(make_string_row(s)).get(); // two identical rows

    auto metadata = w.close().get();
    ASSERT_EQ(metadata.row_groups.size(), 1);
    auto& stats = metadata.row_groups[0].columns[0].meta_data.stats;
    ASSERT_TRUE(stats.has_value());
    ASSERT_TRUE(stats->min.has_value());

    // Min must be "hello" — not the 7-byte mid-sequence prefix.
    EXPECT_EQ(stats->min->value.linearize_to_string(), "hello");
    EXPECT_FALSE(stats->min->is_exact);

    // Max must be a valid UTF-8 upper bound strictly greater than "hello".
    ASSERT_TRUE(stats->max.has_value());
    EXPECT_GT(stats->max->value.linearize_to_string(), ss::sstring("hello"));
    EXPECT_FALSE(stats->max->is_exact);
}

TEST(ParquetWriter, StatsTruncationUtf8MaxNoInvalidByteIncrement) {
    // "aÿ..." — "ÿ" = U+00FF = 0xC3 0xBF (2 bytes). With max_len=3 the prefix
    // is "aÿ" (3 bytes). Byte-level increment of 0xBF→0xC0 is INVALID UTF-8.
    // The correct result increments U+00FF → U+0100 (Ā) = 0xC4 0x80, giving
    // the max bound "aĀ" = [0x61, 0xC4, 0x80].
    constexpr int32_t max_len = 3;
    std::string s = "a\xC3\xBF"
                    "extra"; // "aÿextra", 7 bytes

    iobuf file;
    writer w(
      {.schema = single_string_schema(), .max_stats_truncate_length = max_len},
      make_iobuf_ref_output_stream(file));
    w.init().get();
    w.write_row(make_string_row(s)).get();
    w.write_row(make_string_row(s)).get();

    auto metadata = w.close().get();
    ASSERT_EQ(metadata.row_groups.size(), 1);
    auto& stats = metadata.row_groups[0].columns[0].meta_data.stats;
    ASSERT_TRUE(stats.has_value());
    ASSERT_TRUE(stats->min.has_value());
    ASSERT_TRUE(stats->max.has_value());

    EXPECT_EQ(stats->min->value.linearize_to_string(), "a\xC3\xBF");
    // Must be "aĀ" (valid UTF-8), not "a\xC3\xC0" (invalid).
    EXPECT_EQ(
      stats->max->value.linearize_to_string(), ss::sstring("a\xC4\x80"));
    EXPECT_FALSE(stats->max->is_exact);
}

TEST(ParquetWriter, StatsTruncationUtf8InvalidInput) {
    // Input contains invalid UTF-8 bytes after valid prefix "abc".
    // With max_len=4, the UTF-8-aware min must stop at the last complete valid
    // codepoint ("abc", 3 bytes), not include the invalid byte. The max, if
    // produced, must be valid UTF-8 and a true upper bound (e.g. "abd").
    constexpr int32_t max_len = 4;
    std::string s = "abc\x80\x81\x82"; // "abc" + 3 invalid continuation bytes

    iobuf file;
    writer w(
      {.schema = single_string_schema(), .max_stats_truncate_length = max_len},
      make_iobuf_ref_output_stream(file));
    w.init().get();
    w.write_row(make_string_row(s)).get();
    w.write_row(make_string_row(s)).get();

    auto metadata = w.close().get();
    ASSERT_EQ(metadata.row_groups.size(), 1);
    auto& stats = metadata.row_groups[0].columns[0].meta_data.stats;
    ASSERT_TRUE(stats.has_value());
    ASSERT_TRUE(stats->min.has_value());

    EXPECT_EQ(stats->min->value.linearize_to_string(), "abc");
    EXPECT_FALSE(stats->min->is_exact);

    // Max must be a valid UTF-8 upper bound strictly greater than the min.
    ASSERT_TRUE(stats->max.has_value());
    EXPECT_GT(stats->max->value.linearize_to_string(), ss::sstring("abc"));
    EXPECT_FALSE(stats->max->is_exact);
}

// NOLINTEND(*magic-number*)

} // namespace serde::parquet
