// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/schema_avro.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/tests/value_generator.h"
#include "iceberg/values.h"
#include "iceberg/values_avro.h"

#include <gtest/gtest.h>

using namespace iceberg;

TEST(ValuesAvroTest, TestZeroVals) {
    auto schema_field_type = test_nested_schema_type();
    auto schema = struct_type_to_avro(
      std::get<struct_type>(schema_field_type), "nested");
    auto zero_val = tests::make_value({}, schema_field_type);
    const auto& random_struct = std::get<std::unique_ptr<struct_value>>(
      zero_val);

    auto datum = struct_to_avro(*random_struct, schema.root());
    auto roundtrip_val = val_from_avro(
      datum, schema_field_type, field_required::yes);
    ASSERT_TRUE(roundtrip_val.has_value());
    ASSERT_EQ(roundtrip_val.value(), zero_val);
}

TEST(ValuesAvroTest, TestRandomVals) {
    constexpr int num_iterations = 10;
    auto schema_field_type = test_nested_schema_type();
    auto schema = struct_type_to_avro(
      std::get<struct_type>(schema_field_type), "nested");

    for (int i = 0; i < num_iterations; ++i) {
        auto rand_val = tests::make_value(
          {.pattern = tests::value_pattern::random, .null_pct = 25},
          schema_field_type);
        const auto& random_struct = std::get<std::unique_ptr<struct_value>>(
          rand_val);
        auto datum = struct_to_avro(*random_struct, schema.root());
        auto roundtrip_val = val_from_avro(
          datum, schema_field_type, field_required::yes);
        ASSERT_TRUE(roundtrip_val.has_value());
        ASSERT_EQ(roundtrip_val.value(), rand_val);
    }
}
