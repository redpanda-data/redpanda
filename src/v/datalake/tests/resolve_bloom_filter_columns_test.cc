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
#include "datalake/serde_parquet_writer.h"
#include "iceberg/datatypes.h"

#include <gtest/gtest.h>

using namespace datalake;
using namespace iceberg;

namespace {

// Flat schema: {str_col: string, int_col: int, bool_col: boolean}
struct_type flat_schema() {
    struct_type schema;
    schema.fields.push_back(
      nested_field::create(1, "str_col", field_required::yes, string_type{}));
    schema.fields.push_back(
      nested_field::create(2, "int_col", field_required::yes, int_type{}));
    schema.fields.push_back(
      nested_field::create(3, "bool_col", field_required::yes, boolean_type{}));
    return schema;
}

// Nested schema: {outer: {inner: string}}
struct_type nested_schema() {
    struct_type inner;
    inner.fields.push_back(
      nested_field::create(2, "inner", field_required::yes, string_type{}));
    struct_type schema;
    schema.fields.push_back(
      nested_field::create(1, "outer", field_required::yes, std::move(inner)));
    return schema;
}

parquet_write_config
make_config(std::initializer_list<parquet_write_config::bloom_column> columns) {
    parquet_write_config config;
    for (auto& col : columns) {
        config.bloom_filter_columns.push_back(col);
    }
    return config;
}

} // namespace

TEST(ResolveBloomFilterColumnsTest, EmptyConfig) {
    auto config = make_config({});
    auto result = resolve_bloom_filter_columns(config, flat_schema());
    EXPECT_TRUE(result.empty());
}

TEST(ResolveBloomFilterColumnsTest, MatchesFlatColumn) {
    auto config = make_config({{"str_col", 5000}});
    auto result = resolve_bloom_filter_columns(config, flat_schema());
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result.at("str_col"), 5000);
}

TEST(ResolveBloomFilterColumnsTest, MatchesMultipleColumns) {
    auto config = make_config({{"str_col", 5000}, {"int_col", 8000}});
    auto result = resolve_bloom_filter_columns(config, flat_schema());
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result.at("str_col"), 5000);
    EXPECT_EQ(result.at("int_col"), 8000);
}

TEST(ResolveBloomFilterColumnsTest, UnknownColumnIgnored) {
    auto config = make_config({{"nonexistent", 5000}});
    auto result = resolve_bloom_filter_columns(config, flat_schema());
    EXPECT_TRUE(result.empty());
}

TEST(ResolveBloomFilterColumnsTest, MatchesNestedColumn) {
    auto config = make_config({{"outer.inner", 7000}});
    auto result = resolve_bloom_filter_columns(config, nested_schema());
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result.at("outer.inner"), 7000);
}

TEST(ResolveBloomFilterColumnsTest, NestedColumnParentNameDoesNotMatch) {
    // "outer" is a struct, not a leaf — should not match.
    auto config = make_config({{"outer", 7000}});
    auto result = resolve_bloom_filter_columns(config, nested_schema());
    EXPECT_TRUE(result.empty());
}

TEST(ResolveBloomFilterColumnsTest, MixOfMatchedAndUnknown) {
    auto config = make_config({{"str_col", 5000}, {"no_such_col", 1000}});
    auto result = resolve_bloom_filter_columns(config, flat_schema());
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result.at("str_col"), 5000);
}
