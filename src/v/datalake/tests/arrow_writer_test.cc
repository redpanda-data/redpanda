/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/arrow_translator.h"
#include "datalake/tests/test_data.h"
#include "iceberg/datatypes.h"
#include "iceberg/tests/value_generator.h"
#include "iceberg/values.h"

#include <arrow/array/array_base.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <stdexcept>

TEST(ArrowWriterTest, TranslatesSchemas) {
    datalake::arrow_translator writer(test_schema(iceberg::field_required::no));
    auto schema = writer.build_arrow_schema();
    ASSERT_NE(schema, nullptr);
    std::string expected_schema = R"(test_bool: bool
test_int: int32
test_long: int64
test_float: float
test_double: double
test_decimal: decimal128(16, 8)
test_date: date32[day]
test_time: time64[us]
test_timestamp: timestamp[us]
test_timestamptz: timestamp[us, tz=UTC]
test_string: string
test_uuid: fixed_size_binary[16]
test_fixed: fixed_size_binary[11]
test_binary: binary
test_struct: struct<nested_bool: bool, nested_int: int32, nested_long: int64>
test_list: list<element: string>
test_map: map<string, int64>)";
    EXPECT_EQ(schema->ToString(), expected_schema);
}

TEST(ArrowWriterTest, TranslatesSchemasWithRequired) {
    datalake::arrow_translator writer(
      test_schema(iceberg::field_required::yes));
    auto schema = writer.build_arrow_schema();
    ASSERT_NE(schema, nullptr);
    std::string expected_schema = R"(test_bool: bool not null
test_int: int32 not null
test_long: int64 not null
test_float: float not null
test_double: double not null
test_decimal: decimal128(16, 8) not null
test_date: date32[day] not null
test_time: time64[us] not null
test_timestamp: timestamp[us] not null
test_timestamptz: timestamp[us, tz=UTC] not null
test_string: string not null
test_uuid: fixed_size_binary[16] not null
test_fixed: fixed_size_binary[11] not null
test_binary: binary not null
test_struct: struct<nested_bool: bool not null, nested_int: int32 not null, nested_long: int64 not null> not null
test_list: list<element: string not null> not null
test_map: map<string, int64> not null)";
    EXPECT_EQ(schema->ToString(), expected_schema);
}

// putting this at the end so it doesn't clutter up the test.
std::string get_expected_translation_output();

TEST(ArrowWriterTest, TranslatesData) {
    datalake::arrow_translator schema_translator(
      test_schema(iceberg::field_required::no));

    for (int i = 0; i < 5; i++) {
        auto data = iceberg::tests::make_struct_value(
          iceberg::tests::value_spec{
            .forced_fixed_val = iobuf::from("Hello world")},
          test_schema(iceberg::field_required::no));
        schema_translator.add_data(std::move(data));
    }

    std::string result_string = schema_translator.take_chunk()->ToString();

    EXPECT_EQ(result_string, get_expected_translation_output());
}

TEST(ArrowWriterTest, FailsOnWrongFieldCount) {
    using namespace iceberg;
    struct_type schema;
    schema.fields.emplace_back(nested_field::create(
      1, "test_bool_1", field_required::no, boolean_type{}));
    schema.fields.emplace_back(nested_field::create(
      2, "test_bool_2", field_required::no, boolean_type{}));

    datalake::arrow_translator schema_translator(std::move(schema));

    std::unique_ptr<struct_value> too_little_data
      = std::make_unique<struct_value>();
    too_little_data->fields.emplace_back(boolean_value{true});

    EXPECT_THROW(
      schema_translator.add_data(std::move(*too_little_data)),
      std::invalid_argument);
}

struct required_field_test_params {
    iceberg::field_required required;
    bool present;
    bool should_fail;
};

class RequiredFieldTest
  : public ::testing::TestWithParam<required_field_test_params> {};

TEST_P(RequiredFieldTest, DoRequiredFieldTest) {
    // void missing_require_field_test(
    //   iceberg::field_required required, bool present, bool should_fail) {
    const auto params = GetParam();
    using namespace iceberg;
    struct_type schema;
    schema.fields.emplace_back(nested_field::create(
      1, "test_bool_1", field_required::no, boolean_type{}));
    schema.fields.emplace_back(
      nested_field::create(2, "test_bool_2", params.required, boolean_type{}));

    datalake::arrow_translator schema_translator(std::move(schema));

    std::unique_ptr<struct_value> missing_field
      = std::make_unique<struct_value>();
    missing_field->fields.emplace_back(boolean_value{true});

    if (params.present) {
        missing_field->fields.emplace_back(boolean_value{true});
    } else {
        missing_field->fields.emplace_back(std::nullopt);
    }

    if (params.should_fail) {
        EXPECT_THROW(
          schema_translator.add_data(std::move(*missing_field)),
          std::invalid_argument);
    } else {
        schema_translator.add_data(std::move(*missing_field));
    }
}

INSTANTIATE_TEST_SUITE_P(
  RequiredFieldTestInst,
  RequiredFieldTest,
  ::testing::Values(
    required_field_test_params{
      .required = iceberg::field_required::no,
      .present = true,
      .should_fail = false},
    required_field_test_params{
      .required = iceberg::field_required::no,
      .present = false,
      .should_fail = false},
    required_field_test_params{
      .required = iceberg::field_required::yes,
      .present = true,
      .should_fail = false},
    required_field_test_params{
      .required = iceberg::field_required::yes,
      .present = false,
      .should_fail = true}));

TEST(ArrowWriterTest, BadSchema) {
    using namespace iceberg;
    struct_type input_schema;

    // A decimal type with a precision of 0 will fail.
    input_schema.fields.emplace_back(nested_field::create(
      6, "test_decimal", field_required::yes, decimal_type{0, 16}));

    EXPECT_THROW(
      datalake::arrow_translator(std::move(input_schema)),
      std::invalid_argument);
}

std::string get_expected_translation_output() {
    return R"(-- is_valid: all not null
-- child 0 type: bool
  [
    false,
    false,
    false,
    false,
    false
  ]
-- child 1 type: int32
  [
    0,
    0,
    0,
    0,
    0
  ]
-- child 2 type: int64
  [
    0,
    0,
    0,
    0,
    0
  ]
-- child 3 type: float
  [
    0,
    0,
    0,
    0,
    0
  ]
-- child 4 type: double
  [
    0,
    0,
    0,
    0,
    0
  ]
-- child 5 type: decimal128(16, 8)
  [
    0.E-8,
    0.E-8,
    0.E-8,
    0.E-8,
    0.E-8
  ]
-- child 6 type: date32[day]
  [
    1970-01-01,
    1970-01-01,
    1970-01-01,
    1970-01-01,
    1970-01-01
  ]
-- child 7 type: time64[us]
  [
    00:00:00.000000,
    00:00:00.000000,
    00:00:00.000000,
    00:00:00.000000,
    00:00:00.000000
  ]
-- child 8 type: timestamp[us]
  [
    1970-01-01 00:00:00.000000,
    1970-01-01 00:00:00.000000,
    1970-01-01 00:00:00.000000,
    1970-01-01 00:00:00.000000,
    1970-01-01 00:00:00.000000
  ]
-- child 9 type: timestamp[us, tz=UTC]
  [
    1970-01-01 00:00:00.000000Z,
    1970-01-01 00:00:00.000000Z,
    1970-01-01 00:00:00.000000Z,
    1970-01-01 00:00:00.000000Z,
    1970-01-01 00:00:00.000000Z
  ]
-- child 10 type: string
  [
    "",
    "",
    "",
    "",
    ""
  ]
-- child 11 type: fixed_size_binary[16]
  [
    00000000000000000000000000000000,
    00000000000000000000000000000000,
    00000000000000000000000000000000,
    00000000000000000000000000000000,
    00000000000000000000000000000000
  ]
-- child 12 type: fixed_size_binary[11]
  [
    48656C6C6F20776F726C64,
    48656C6C6F20776F726C64,
    48656C6C6F20776F726C64,
    48656C6C6F20776F726C64,
    48656C6C6F20776F726C64
  ]
-- child 13 type: binary
  [
    ,
    ,
    ,
    ,
    
  ]
-- child 14 type: struct<nested_bool: bool, nested_int: int32, nested_long: int64>
  -- is_valid: all not null
  -- child 0 type: bool
    [
      false,
      false,
      false,
      false,
      false
    ]
  -- child 1 type: int32
    [
      0,
      0,
      0,
      0,
      0
    ]
  -- child 2 type: int64
    [
      0,
      0,
      0,
      0,
      0
    ]
-- child 15 type: list<element: string>
  [
    [
      "",
      "",
      "",
      "",
      ""
    ],
    [
      "",
      "",
      "",
      "",
      ""
    ],
    [
      "",
      "",
      "",
      "",
      ""
    ],
    [
      "",
      "",
      "",
      "",
      ""
    ],
    []
  ]
-- child 16 type: map<string, int64>
  [
    keys:
    [
      "",
      "",
      "",
      "",
      ""
    ]
    values:
    [
      0,
      0,
      0,
      0,
      0
    ],
    keys:
    [
      "",
      "",
      "",
      "",
      ""
    ]
    values:
    [
      0,
      0,
      0,
      0,
      0
    ],
    keys:
    [
      "",
      "",
      "",
      "",
      ""
    ]
    values:
    [
      0,
      0,
      0,
      0,
      0
    ],
    keys:
    [
      "",
      "",
      "",
      "",
      ""
    ]
    values:
    [
      0,
      0,
      0,
      0,
      0
    ],
    keys:
    [
      "",
      "",
      "",
      "",
      ""
    ]
    values:
    [
      0,
      0,
      0,
      0,
      0
    ]
  ])";
}
