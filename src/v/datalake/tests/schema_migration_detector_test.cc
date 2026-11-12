/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/schema_migration_detector.h"
#include "iceberg/datatypes.h"
#include "iceberg/schema.h"

#include <gtest/gtest.h>

using namespace datalake;
using namespace iceberg;

namespace {

// Build a redpanda struct with the specified types for timestamp and
// headers key, matching the table_definition.cc structure.
schema make_rp_schema(field_type timestamp_type, field_type headers_key_type) {
    struct_type headers_kv;
    headers_kv.fields.emplace_back(
      nested_field::create(
        7, "key", field_required::no, std::move(headers_key_type)));
    headers_kv.fields.emplace_back(
      nested_field::create(8, "value", field_required::no, binary_type{}));

    struct_type system_fields;
    system_fields.fields.emplace_back(
      nested_field::create(2, "partition", field_required::no, int_type{}));
    system_fields.fields.emplace_back(
      nested_field::create(3, "offset", field_required::no, long_type{}));
    system_fields.fields.emplace_back(
      nested_field::create(
        4, "timestamp", field_required::no, std::move(timestamp_type)));
    system_fields.fields.emplace_back(
      nested_field::create(
        5,
        "headers",
        field_required::no,
        list_type::create(6, field_required::no, std::move(headers_kv))));
    system_fields.fields.emplace_back(
      nested_field::create(9, "key", field_required::no, binary_type{}));

    struct_type root;
    root.fields.emplace_back(
      nested_field::create(
        1, "redpanda", field_required::no, std::move(system_fields)));

    return schema{
      .schema_struct = std::move(root),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
}

} // namespace

TEST(SchemaMigrationDetectorTest, DetectsOldTimestampType) {
    auto s = make_rp_schema(timestamp_type{}, string_type{});
    auto renames = detect_redpanda_struct_renames(s);
    ASSERT_EQ(renames.size(), 1);
    EXPECT_EQ(renames[0].column_path, "redpanda.timestamp");
    EXPECT_EQ(renames[0].suggested_new_name, "timestamp_v1");
    std::vector<ss::sstring> expected_path = {"redpanda", "timestamp"};
    EXPECT_EQ(renames[0].field_path, expected_path);
}

TEST(SchemaMigrationDetectorTest, DetectsOldHeadersKeyType) {
    auto s = make_rp_schema(timestamptz_type{}, binary_type{});
    auto renames = detect_redpanda_struct_renames(s);
    ASSERT_EQ(renames.size(), 1);
    EXPECT_EQ(renames[0].column_path, "redpanda.headers.key");
    EXPECT_EQ(renames[0].suggested_new_name, "key_v1");
    std::vector<ss::sstring> expected_path = {"redpanda", "headers", "key"};
    EXPECT_EQ(renames[0].field_path, expected_path);
}

TEST(SchemaMigrationDetectorTest, DetectsBothOldTypes) {
    auto s = make_rp_schema(timestamp_type{}, binary_type{});
    auto renames = detect_redpanda_struct_renames(s);
    ASSERT_EQ(renames.size(), 2);
    EXPECT_EQ(renames[0].column_path, "redpanda.timestamp");
    EXPECT_EQ(renames[1].column_path, "redpanda.headers.key");
}

TEST(SchemaMigrationDetectorTest, NoRenamesForCurrentSchema) {
    auto s = make_rp_schema(timestamptz_type{}, string_type{});
    auto renames = detect_redpanda_struct_renames(s);
    EXPECT_TRUE(renames.empty());
}

TEST(SchemaMigrationDetectorTest, UserSchemaDetectsTypeMismatch) {
    struct_type existing;
    existing.fields.emplace_back(
      nested_field::create(100, "status", field_required::no, int_type{}));
    existing.fields.emplace_back(
      nested_field::create(101, "name", field_required::no, string_type{}));

    struct_type expected;
    expected.fields.emplace_back(
      nested_field::create(100, "status", field_required::no, string_type{}));
    expected.fields.emplace_back(
      nested_field::create(101, "name", field_required::no, string_type{}));

    auto renames = detect_user_schema_renames(existing, expected);
    ASSERT_EQ(renames.size(), 1);
    EXPECT_EQ(renames[0].column_path, "status");
    EXPECT_EQ(renames[0].suggested_new_name, "status_v1");
    std::vector<ss::sstring> expected_path = {"status"};
    EXPECT_EQ(renames[0].field_path, expected_path);
}

TEST(SchemaMigrationDetectorTest, UserSchemaSkipsRedpandaStruct) {
    struct_type existing;
    existing.fields.emplace_back(
      nested_field::create(1, "redpanda", field_required::no, struct_type{}));
    existing.fields.emplace_back(
      nested_field::create(100, "data", field_required::no, int_type{}));

    struct_type expected;
    expected.fields.emplace_back(
      nested_field::create(1, "redpanda", field_required::no, struct_type{}));
    expected.fields.emplace_back(
      nested_field::create(100, "data", field_required::no, string_type{}));

    auto renames = detect_user_schema_renames(existing, expected);
    ASSERT_EQ(renames.size(), 1);
    EXPECT_EQ(renames[0].column_path, "data");
}

TEST(SchemaMigrationDetectorTest, UserSchemaNoMismatch) {
    struct_type existing;
    existing.fields.emplace_back(
      nested_field::create(100, "name", field_required::no, string_type{}));

    auto renames = detect_user_schema_renames(existing, existing);
    EXPECT_TRUE(renames.empty());
}
