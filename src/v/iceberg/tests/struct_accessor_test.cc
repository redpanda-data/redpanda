// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
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
    auto check_primitive_val = [&](int32_t id, const primitive_type& t) {
        auto& acc = accessors.at(nested_field::id_t{id});
        const auto& val = acc->get(*random_struct);
        EXPECT_TRUE(val.has_value());
        EXPECT_TRUE(std::holds_alternative<primitive_value>(*val));
        EXPECT_TRUE(value_matches_type(std::get<primitive_value>(*val), t));
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
