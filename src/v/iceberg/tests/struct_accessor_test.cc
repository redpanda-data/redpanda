/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/struct_accessor.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/tests/value_generator.h"

#include <gtest/gtest.h>

using namespace iceberg;

TEST(StructAccessorTest, TestGetAccessorsNestedSchema) {
    auto schema_field_type = test_nested_schema_type();
    const auto& schema_struct = std::get<struct_type>(schema_field_type);
    auto accessors = struct_accessor::from_struct_type(schema_struct);
    auto random_val = tests::make_value({}, schema_field_type);
    const auto& random_struct = std::get<std::unique_ptr<struct_value>>(
      random_val);
    // Ensures that the values in the generated struct have the given type and
    // are all non-null.
    auto check_primitive_val = [&](int32_t id, const primitive_type& t) {
        auto& acc = accessors.at(nested_field::id_t{id});
        const auto& val = acc->get(*random_struct);
        EXPECT_TRUE(val.has_value());
        EXPECT_TRUE(std::holds_alternative<primitive_value>(val.value()));
        EXPECT_TRUE(
          value_matches_type(std::get<primitive_value>(val.value()), t));
    };
    // In the test schema, only a few fields are not a part of a list or map.
    // We should only have accessors for those fields.
    ASSERT_EQ(5, accessors.size());
    ASSERT_NO_FATAL_FAILURE(check_primitive_val(1, string_type{}));
    ASSERT_NO_FATAL_FAILURE(check_primitive_val(2, int_type{}));
    ASSERT_NO_FATAL_FAILURE(check_primitive_val(3, boolean_type{}));
    ASSERT_NO_FATAL_FAILURE(check_primitive_val(16, string_type{}));
    ASSERT_NO_FATAL_FAILURE(check_primitive_val(17, int_type{}));
}

TEST(StructAccessorTest, TestGetAccessorsNestedSchemaNull) {
    auto schema_field_type = test_nested_schema_type();
    const auto& schema_struct = std::get<struct_type>(schema_field_type);
    auto accessors = struct_accessor::from_struct_type(schema_struct);
    auto null_val = tests::make_value(
      {.pattern = tests::value_pattern::zeros, .null_pct = 100},
      schema_field_type);
    const auto& null_struct = std::get<std::unique_ptr<struct_value>>(null_val);
    // Ensures that the values in the generated struct have the given type and
    // are all non-null.
    auto check_primitive_val = [&](int32_t id, const primitive_type& t) {
        auto& acc = accessors.at(nested_field::id_t{id});
        const auto& val = acc->get(*null_struct);
        ASSERT_TRUE(val.has_value());
        ASSERT_TRUE(std::holds_alternative<primitive_value>(val.value()));
        ASSERT_TRUE(
          value_matches_type(std::get<primitive_value>(val.value()), t));
    };
    // Ensures that the values in the generated struct are null.
    auto check_null_val = [&](int32_t id) {
        auto& acc = accessors.at(nested_field::id_t{id});
        const auto& val = acc->get(*null_struct);
        EXPECT_FALSE(val.has_value());
    };
    ASSERT_EQ(5, accessors.size());
    ASSERT_NO_FATAL_FAILURE(check_null_val(1));
    ASSERT_NO_FATAL_FAILURE(check_primitive_val(2, int_type{}));
    ASSERT_NO_FATAL_FAILURE(check_null_val(3));
    ASSERT_NO_FATAL_FAILURE(check_null_val(16));
    // While field 17 is required, its parent is not, so this ends up being
    // null as well.
    ASSERT_NO_FATAL_FAILURE(check_null_val(17));
}
