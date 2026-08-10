/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/parquet_write_config.h"

#include <gtest/gtest.h>

using namespace datalake;
using namespace iceberg;

namespace {

std::optional<table_properties_t>
make_props(std::string_view key, std::string_view value) {
    table_properties_t props;
    props.emplace(ss::sstring(key), ss::sstring(value));
    return std::make_optional(std::move(props));
}

std::optional<table_properties_t> make_codec_prop(std::string_view codec) {
    return make_props("write.parquet.compression-codec", codec);
}

std::optional<table_properties_t> make_props_multi(
  std::initializer_list<std::pair<std::string_view, std::string_view>> kvs) {
    table_properties_t props;
    for (const auto& [k, v] : kvs) {
        props.emplace(ss::sstring(k), ss::sstring(v));
    }
    return std::make_optional(std::move(props));
}

const parquet_write_config::bloom_column*
find_bloom_column(const parquet_write_config& config, std::string_view name) {
    for (const auto& col : config.bloom_filter_columns) {
        if (col.name == name) {
            return &col;
        }
    }
    return nullptr;
}

} // namespace

TEST(ParquetWriteConfigTest, DefaultsToCompressed) {
    // No properties at all.
    auto config = parquet_write_config::from_properties(std::nullopt);
    EXPECT_TRUE(config.compress);

    // Empty properties map.
    auto empty = std::make_optional<table_properties_t>();
    config = parquet_write_config::from_properties(empty);
    EXPECT_TRUE(config.compress);
}

TEST(ParquetWriteConfigTest, Zstd) {
    for (auto codec : {"zstd", "ZSTD", "Zstd"}) {
        auto props = make_codec_prop(codec);
        auto config = parquet_write_config::from_properties(props);
        EXPECT_TRUE(config.compress) << "codec: " << codec;
    }
}

TEST(ParquetWriteConfigTest, Uncompressed) {
    for (auto codec : {"uncompressed", "UNCOMPRESSED", "Uncompressed"}) {
        auto props = make_codec_prop(codec);
        auto config = parquet_write_config::from_properties(props);
        EXPECT_FALSE(config.compress) << "codec: " << codec;
    }
}

TEST(ParquetWriteConfigTest, None) {
    for (auto codec : {"none", "NONE", "None"}) {
        auto props = make_codec_prop(codec);
        auto config = parquet_write_config::from_properties(props);
        EXPECT_FALSE(config.compress) << "codec: " << codec;
    }
}

TEST(ParquetWriteConfigTest, UnsupportedCodecFallsBackToZstd) {
    for (auto codec : {"gzip", "snappy", "lz4_raw", "brotli", "garbage"}) {
        auto props = make_codec_prop(codec);
        auto config = parquet_write_config::from_properties(props);
        EXPECT_TRUE(config.compress) << "codec: " << codec;
    }
}

TEST(ParquetWriteConfigTest, UnrelatedPropertiesIgnored) {
    auto props = make_props("write.parquet.page-size-bytes", "1048576");
    auto config = parquet_write_config::from_properties(props);
    EXPECT_TRUE(config.compress);
}

TEST(ParquetWriteConfigTest, BloomFilterEnabledDefaultNdv) {
    auto props = make_props(
      "write.parquet.bloom-filter-enabled.column.redpanda.offset", "true");
    auto config = parquet_write_config::from_properties(props);
    ASSERT_EQ(config.bloom_filter_columns.size(), 1);
    EXPECT_EQ(config.bloom_filter_columns[0].name, "redpanda.offset");
    EXPECT_EQ(
      config.bloom_filter_columns[0].ndv,
      parquet_write_config::default_bloom_filter_ndv);
}

TEST(ParquetWriteConfigTest, BloomFilterEnabledWithNdv) {
    auto props = make_props_multi({
      {"write.parquet.bloom-filter-enabled.column.my_col", "true"},
      {"write.parquet.bloom-filter-ndv.column.my_col", "50000"},
    });
    auto config = parquet_write_config::from_properties(props);
    ASSERT_EQ(config.bloom_filter_columns.size(), 1);
    EXPECT_EQ(config.bloom_filter_columns[0].name, "my_col");
    EXPECT_EQ(config.bloom_filter_columns[0].ndv, 50000);
}

TEST(ParquetWriteConfigTest, BloomFilterNdvWithoutEnabledIsIgnored) {
    auto props = make_props(
      "write.parquet.bloom-filter-ndv.column.my_col", "50000");
    auto config = parquet_write_config::from_properties(props);
    EXPECT_TRUE(config.bloom_filter_columns.empty());
}

TEST(ParquetWriteConfigTest, BloomFilterDisabled) {
    auto props = make_props(
      "write.parquet.bloom-filter-enabled.column.my_col", "false");
    auto config = parquet_write_config::from_properties(props);
    EXPECT_TRUE(config.bloom_filter_columns.empty());
}

TEST(ParquetWriteConfigTest, BloomFilterCaseInsensitive) {
    for (auto val : {"true", "TRUE", "True"}) {
        auto props = make_props(
          "write.parquet.bloom-filter-enabled.column.col", val);
        auto config = parquet_write_config::from_properties(props);
        EXPECT_EQ(config.bloom_filter_columns.size(), 1) << "val: " << val;
    }
    for (auto val : {"false", "FALSE", "False"}) {
        auto props = make_props(
          "write.parquet.bloom-filter-enabled.column.col", val);
        auto config = parquet_write_config::from_properties(props);
        EXPECT_TRUE(config.bloom_filter_columns.empty()) << "val: " << val;
    }
}

TEST(ParquetWriteConfigTest, BloomFilterMultipleColumns) {
    auto props = make_props_multi({
      {"write.parquet.bloom-filter-enabled.column.a", "true"},
      {"write.parquet.bloom-filter-enabled.column.b", "false"},
      {"write.parquet.bloom-filter-enabled.column.c", "true"},
      {"write.parquet.bloom-filter-ndv.column.c", "99999"},
    });
    auto config = parquet_write_config::from_properties(props);
    EXPECT_EQ(config.bloom_filter_columns.size(), 2);
    auto* a = find_bloom_column(config, "a");
    auto* c = find_bloom_column(config, "c");
    ASSERT_NE(a, nullptr);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(a->ndv, parquet_write_config::default_bloom_filter_ndv);
    EXPECT_EQ(c->ndv, 99999);
    EXPECT_EQ(find_bloom_column(config, "b"), nullptr);
}

TEST(ParquetWriteConfigTest, BloomFilterMalformedEnabled) {
    auto props = make_props(
      "write.parquet.bloom-filter-enabled.column.col", "yes");
    auto config = parquet_write_config::from_properties(props);
    EXPECT_TRUE(config.bloom_filter_columns.empty());
}

TEST(ParquetWriteConfigTest, BloomFilterMalformedNdv) {
    auto props = make_props_multi({
      {"write.parquet.bloom-filter-enabled.column.col", "true"},
      {"write.parquet.bloom-filter-ndv.column.col", "not_a_number"},
    });
    auto config = parquet_write_config::from_properties(props);
    ASSERT_EQ(config.bloom_filter_columns.size(), 1);
    // Malformed NDV is ignored, falls back to default.
    EXPECT_EQ(
      config.bloom_filter_columns[0].ndv,
      parquet_write_config::default_bloom_filter_ndv);
}

TEST(ParquetWriteConfigTest, BloomFilterNdvZeroIgnored) {
    auto props = make_props_multi({
      {"write.parquet.bloom-filter-enabled.column.col", "true"},
      {"write.parquet.bloom-filter-ndv.column.col", "0"},
    });
    auto config = parquet_write_config::from_properties(props);
    ASSERT_EQ(config.bloom_filter_columns.size(), 1);
    // NDV=0 is ignored, falls back to default.
    EXPECT_EQ(
      config.bloom_filter_columns[0].ndv,
      parquet_write_config::default_bloom_filter_ndv);
}

TEST(ParquetWriteConfigTest, BloomFilterNdvClampedToMax) {
    auto props = make_props_multi({
      {"write.parquet.bloom-filter-enabled.column.col", "true"},
      {"write.parquet.bloom-filter-ndv.column.col", "999999999"},
    });
    auto config = parquet_write_config::from_properties(props);
    ASSERT_EQ(config.bloom_filter_columns.size(), 1);
    EXPECT_EQ(
      config.bloom_filter_columns[0].ndv,
      parquet_write_config::max_bloom_filter_ndv);
}
