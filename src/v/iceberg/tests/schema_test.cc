// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/schema.h"
#include "iceberg/tests/test_schemas.h"

#include <gtest/gtest.h>

using namespace iceberg;

TEST(SchemaTest, TestGetTypesNestedSchema) {
    schema s{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
    const auto ids_to_types = s.ids_to_types();
    ASSERT_EQ(17, ids_to_types.size());
    // First check all ids are accounted for.
    for (int i = 1; i <= 17; i++) {
        ASSERT_TRUE(ids_to_types.contains(nested_field::id_t{i}));
    }
    auto get_primitive_type = [&](int32_t id) {
        auto* type = ids_to_types.at(nested_field::id_t{id});
        EXPECT_TRUE(type);
        EXPECT_TRUE(std::holds_alternative<primitive_type>(*type));
        return std::get<primitive_type>(*type);
    };
    auto check_list_type = [&](int32_t id) {
        auto* type = ids_to_types.at(nested_field::id_t{id});
        EXPECT_TRUE(type);
        ASSERT_TRUE(std::holds_alternative<list_type>(*type));
    };
    auto check_struct_type = [&](int32_t id) {
        auto* type = ids_to_types.at(nested_field::id_t{id});
        EXPECT_TRUE(type);
        ASSERT_TRUE(std::holds_alternative<struct_type>(*type));
    };
    auto check_map_type = [&](int32_t id) {
        auto* type = ids_to_types.at(nested_field::id_t{id});
        EXPECT_TRUE(type);
        ASSERT_TRUE(std::holds_alternative<map_type>(*type));
    };
    // Now check the types of each id.
    ASSERT_TRUE(std::holds_alternative<string_type>(get_primitive_type(1)));
    ASSERT_TRUE(std::holds_alternative<int_type>(get_primitive_type(2)));
    ASSERT_TRUE(std::holds_alternative<boolean_type>(get_primitive_type(3)));
    ASSERT_NO_FATAL_FAILURE(check_list_type(4));
    ASSERT_TRUE(std::holds_alternative<string_type>(get_primitive_type(5)));
    ASSERT_NO_FATAL_FAILURE(check_map_type(6));
    ASSERT_TRUE(std::holds_alternative<string_type>(get_primitive_type(7)));
    ASSERT_NO_FATAL_FAILURE(check_map_type(8));
    ASSERT_TRUE(std::holds_alternative<string_type>(get_primitive_type(9)));
    ASSERT_TRUE(std::holds_alternative<int_type>(get_primitive_type(10)));
    ASSERT_NO_FATAL_FAILURE(check_list_type(11));
    ASSERT_NO_FATAL_FAILURE(check_struct_type(12));
    ASSERT_TRUE(std::holds_alternative<float_type>(get_primitive_type(13)));
    ASSERT_TRUE(std::holds_alternative<float_type>(get_primitive_type(14)));
    ASSERT_NO_FATAL_FAILURE(check_struct_type(15));
    ASSERT_TRUE(std::holds_alternative<string_type>(get_primitive_type(16)));
    ASSERT_TRUE(std::holds_alternative<int_type>(get_primitive_type(17)));
}

TEST(SchemaTest, TestGetFromNullSchema) {
    struct_type type;
    type.fields.emplace_back(nullptr);
    schema s{
      .schema_struct = std::move(type),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
    const auto ids_to_types = s.ids_to_types();
    ASSERT_TRUE(ids_to_types.empty());
    ASSERT_FALSE(s.highest_field_id().has_value());
}

TEST(SchemaTest, TestGetFromEmptySchema) {
    struct_type type;
    schema s{
      .schema_struct = std::move(type),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
    const auto ids_to_types = s.ids_to_types();
    ASSERT_TRUE(ids_to_types.empty());
    ASSERT_FALSE(s.highest_field_id().has_value());
}

TEST(SchemaTest, TestGetTypesNestedSchemaNoneFilter) {
    schema s{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
    // Filter for a field that doesn't exist.
    const auto ids_to_types = s.ids_to_types({nested_field::id_t{0}});
    ASSERT_EQ(0, ids_to_types.size());
}

TEST(SchemaTest, TestGetTypesNestedSchemaNestedFilter) {
    schema s{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
    // Filter for a couple child fields.
    const auto ids_to_types = s.ids_to_types(
      {nested_field::id_t{14}, nested_field::id_t{17}});
    ASSERT_EQ(2, ids_to_types.size());
    {
        auto* type = ids_to_types.at(nested_field::id_t{14});
        ASSERT_TRUE(type);
        EXPECT_TRUE(std::holds_alternative<primitive_type>(*type));
        ASSERT_TRUE(
          std::holds_alternative<float_type>(std::get<primitive_type>(*type)));
    }
    {
        auto* type = ids_to_types.at(nested_field::id_t{17});
        ASSERT_TRUE(type);
        EXPECT_TRUE(std::holds_alternative<primitive_type>(*type));
        ASSERT_TRUE(
          std::holds_alternative<int_type>(std::get<primitive_type>(*type)));
    }
}

TEST(SchemaTest, TestCopy) {
    schema s{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{4},
      .identifier_field_ids = {nested_field::id_t{10}},
    };
    auto s_copy = s.copy();
    ASSERT_EQ(s, s_copy);
}

TEST(SchemaTest, TestGetHighestField) {
    schema s{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{4},
      .identifier_field_ids = {nested_field::id_t{10}},
    };
    auto highest = s.highest_field_id();
    ASSERT_TRUE(highest.has_value());
    ASSERT_EQ(highest.value(), nested_field::id_t{17});
}
