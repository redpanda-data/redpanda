// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/partition.h"
#include "iceberg/partition_key_type.h"
#include "iceberg/schema.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/transform.h"

#include <gtest/gtest.h>

using namespace iceberg;

namespace {

partition_spec make_spec(
  int32_t spec_id,
  int32_t source_id,
  int32_t field_id,
  const ss::sstring& name,
  transform t) {
    partition_spec spec{
      .spec_id = partition_spec::id_t{spec_id},
      .fields = {partition_field{
        .source_id = nested_field::id_t{source_id},
        .field_id = partition_field::id_t{field_id},
        .name = name,
        .transform = t,
      }},
    };
    return spec;
}

schema nested_test_schema() {
    return schema{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
}

} // namespace

class PartitionKeyTypeTest : public ::testing::Test {
public:
    void check_pk_type_field(
      const nested_field_ptr& field,
      int32_t expected_id,
      const ss::sstring& expected_name,
      const primitive_type& expected_type) {
        ASSERT_FALSE(field->required);
        ASSERT_EQ(expected_id, field->id());
        ASSERT_STREQ(expected_name.c_str(), field->name.c_str());
        ASSERT_TRUE(std::holds_alternative<primitive_type>(field->type));
        ASSERT_EQ(std::get<primitive_type>(field->type), expected_type);
    }
};

TEST_F(PartitionKeyTypeTest, TestMissingField) {
    auto schema = nested_test_schema();
    auto fake_id = 42;
    auto spec = make_spec(0, fake_id, 1000, "ident", identity_transform{});
    ASSERT_THROW(
      partition_key_type::create(spec, schema), std::invalid_argument);
}

TEST_F(PartitionKeyTypeTest, TestMultipleFields) {
    auto schema = nested_test_schema();
    auto spec = make_spec(0, 1, 1000, "f1", identity_transform{});
    spec.fields.emplace_back(partition_field{
      .source_id = nested_field::id_t{2},
      .field_id = partition_field::id_t{1001},
      .name = "f2",
      .transform = identity_transform{},
    });
    auto pk_type = partition_key_type::create(spec, schema);
    const auto& pk_struct = pk_type.type;

    ASSERT_EQ(2, pk_struct.fields.size());
    ASSERT_NO_FATAL_FAILURE(
      check_pk_type_field(pk_struct.fields[0], 1000, "f1", string_type{}));
    ASSERT_NO_FATAL_FAILURE(
      check_pk_type_field(pk_struct.fields[1], 1001, "f2", int_type{}));
}

struct transform_expectation {
    int32_t source_field_id;
    transform transform;
    primitive_type expected_transformed_type;
};

class PartitionKeyTypeParamTest
  : public PartitionKeyTypeTest
  , public ::testing::WithParamInterface<transform_expectation> {};

TEST_P(PartitionKeyTypeParamTest, TestSingleFieldCreate) {
    const auto& expectation = GetParam();
    auto schema = nested_test_schema();
    auto spec = make_spec(
      0, expectation.source_field_id, 1000, "tf", expectation.transform);
    auto pk_type = partition_key_type::create(spec, schema);
    const auto& pk_struct = pk_type.type;

    ASSERT_EQ(1, pk_struct.fields.size());
    ASSERT_NO_FATAL_FAILURE(check_pk_type_field(
      pk_struct.fields[0], 1000, "tf", expectation.expected_transformed_type));
}
INSTANTIATE_TEST_SUITE_P(
  Expectations,
  PartitionKeyTypeParamTest,
  ::testing::Values(
    transform_expectation{
      .source_field_id = 1,
      .transform = identity_transform{},
      .expected_transformed_type = string_type{},
    },
    transform_expectation{
      .source_field_id = 2,
      .transform = identity_transform{},
      .expected_transformed_type = int_type{},
    },
    transform_expectation{
      .source_field_id = 1,
      .transform = bucket_transform{},
      .expected_transformed_type = int_type{},
    },
    transform_expectation{
      .source_field_id = 1,
      .transform = truncate_transform{},
      .expected_transformed_type = string_type{},
    },
    transform_expectation{
      .source_field_id = 2,
      .transform = truncate_transform{},
      .expected_transformed_type = int_type{},
    },
    transform_expectation{
      .source_field_id = 1,
      .transform = year_transform{},
      .expected_transformed_type = int_type{},
    },
    transform_expectation{
      .source_field_id = 1,
      .transform = month_transform{},
      .expected_transformed_type = int_type{},
    },
    transform_expectation{
      .source_field_id = 1,
      .transform = day_transform{},
      .expected_transformed_type = int_type{},
    },
    transform_expectation{
      .source_field_id = 1,
      .transform = hour_transform{},
      .expected_transformed_type = int_type{},
    },
    transform_expectation{
      .source_field_id = 1,
      .transform = void_transform{},
      .expected_transformed_type = int_type{},
    }));
