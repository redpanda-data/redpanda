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
