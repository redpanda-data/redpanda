/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "iceberg/datatypes.h"

inline iceberg::struct_type test_schema(iceberg::field_required required) {
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
      nested_field::create(6, "test_decimal", required, decimal_type{16, 8}));
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
    schema.fields.emplace_back(nested_field::create(
      13, "test_fixed", required, fixed_type{11})); // length of "Hello world"
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
