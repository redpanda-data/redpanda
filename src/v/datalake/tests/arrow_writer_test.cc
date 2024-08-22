/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/arrow_schema_translator.h"
#include "iceberg/datatypes.h"

#include <gtest/gtest.h>

iceberg::struct_type test_schema(iceberg::field_required required) {
    using namespace iceberg;
    struct_type schema;

    schema.fields.emplace_back(
      nested_field::create(1, "test_bool", required, boolean_type{}));
    schema.fields.emplace_back(
      nested_field::create(2, "test_int", required, int_type{}));
    schema.fields.emplace_back(
      nested_field::create(3, "test_long", required, long_type{}));
    schema.fields.emplace_back(
      nested_field::create(4, "test_float", required, float_type{}));
    schema.fields.emplace_back(
      nested_field::create(5, "test_double", required, double_type{}));
    schema.fields.emplace_back(
      nested_field::create(6, "test_decimal", required, decimal_type{8, 16}));
    schema.fields.emplace_back(
      nested_field::create(7, "test_date", required, date_type{}));
    schema.fields.emplace_back(
      nested_field::create(8, "test_time", required, time_type{}));
    schema.fields.emplace_back(
      nested_field::create(9, "test_timestamp", required, timestamp_type{}));
    schema.fields.emplace_back(nested_field::create(
      10, "test_timestamptz", required, timestamptz_type{}));
    schema.fields.emplace_back(
      nested_field::create(11, "test_string", required, string_type{}));
    schema.fields.emplace_back(
      nested_field::create(12, "test_uuid", required, uuid_type{}));
    schema.fields.emplace_back(
      nested_field::create(13, "test_fixed", required, fixed_type{100}));
    schema.fields.emplace_back(
      nested_field::create(14, "test_binary", required, binary_type{}));

    struct_type nested_schema;
    nested_schema.fields.emplace_back(
      nested_field::create(1, "nested_bool", required, boolean_type{}));
    nested_schema.fields.emplace_back(
      nested_field::create(2, "nested_int", required, int_type{}));
    nested_schema.fields.emplace_back(
      nested_field::create(3, "nested_long", required, long_type{}));

    schema.fields.emplace_back(nested_field::create(
      15, "test_struct", required, std::move(nested_schema)));

    schema.fields.emplace_back(nested_field::create(
      16,
      "test_list",
      required,
      list_type::create(1, required, string_type{})));

    schema.fields.push_back(nested_field::create(
      17,
      "test_map",
      required,
      map_type::create(1, string_type{}, 2, required, long_type{})));
    return schema;
}

TEST(ArrowWriterTest, TranslatesSchemas) {
    datalake::arrow_schema_translator writer(
      test_schema(iceberg::field_required::no));
    auto schema = writer.build_arrow_schema();
    ASSERT_NE(schema, nullptr);
    std::string expected_schema = R"(test_bool: bool
test_int: int32
test_long: int64
test_float: float
test_double: double
test_decimal: decimal128(8, 16)
test_date: date32[day]
test_time: time64[us]
test_timestamp: timestamp[us]
test_timestamptz: timestamp[us, tz=UTC]
test_string: string
test_uuid: fixed_size_binary[16]
test_fixed: fixed_size_binary[100]
test_binary: binary
test_struct: struct<nested_bool: bool, nested_int: int32, nested_long: int64>
test_list: list<element: string>
test_map: map<string, int64>)";
    EXPECT_EQ(schema->ToString(), expected_schema);
}

TEST(ArrowWriterTest, TranslatesSchemasWithRequired) {
    datalake::arrow_schema_translator writer(
      test_schema(iceberg::field_required::yes));
    auto schema = writer.build_arrow_schema();
    ASSERT_NE(schema, nullptr);
    std::string expected_schema = R"(test_bool: bool not null
test_int: int32 not null
test_long: int64 not null
test_float: float not null
test_double: double not null
test_decimal: decimal128(8, 16) not null
test_date: date32[day] not null
test_time: time64[us] not null
test_timestamp: timestamp[us] not null
test_timestamptz: timestamp[us, tz=UTC] not null
test_string: string not null
test_uuid: fixed_size_binary[16] not null
test_fixed: fixed_size_binary[100] not null
test_binary: binary not null
test_struct: struct<nested_bool: bool not null, nested_int: int32 not null, nested_long: int64 not null> not null
test_list: list<element: string not null> not null
test_map: map<string, int64> not null)";
    EXPECT_EQ(schema->ToString(), expected_schema);
}

TEST(ArrowWriterTest, BadSchema) {
    using namespace iceberg;
    struct_type input_schema;

    // A decimal type with a precision of 0 will fail.
    input_schema.fields.emplace_back(nested_field::create(
      6, "test_decimal", field_required::yes, decimal_type{0, 16}));

    EXPECT_ANY_THROW(
      datalake::arrow_schema_translator(std::move(input_schema)));
}
