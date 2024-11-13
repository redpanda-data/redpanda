// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/avro_decimal.h"
#include "iceberg/schema_avro.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/tests/value_generator.h"
#include "iceberg/values.h"
#include "iceberg/values_avro.h"
#include "random/generators.h"

#include <gtest/gtest.h>

using namespace iceberg;

TEST(ValuesAvroTest, TestZeroVals) {
    auto schema_field_type = test_nested_schema_type_avro();
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
    auto schema_field_type = test_nested_schema_type_avro();
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

TEST(ValuesAvroTest, TestDecimal) {
    struct_type st;
    st.fields.push_back(nested_field::create(
      0,
      "decimal_val",
      field_required::yes,
      decimal_type{.precision = 10, .scale = 2}));

    field_type schema_field{std::move(st)};

    auto schema = struct_type_to_avro(
      std::get<struct_type>(schema_field), "st_with_decimal");

    auto make_struct = [](absl::int128 value) {
        struct_value ret;
        ret.fields.push_back(decimal_value{.val = value});
        return ret;
    };

    for (auto& v : {
           make_struct(std::numeric_limits<absl::int128>::max()),
           make_struct(std::numeric_limits<absl::int128>::max()),
           make_struct(0),
           make_struct(-1),
           make_struct(1),
         }) {
        auto datum = struct_to_avro(v, schema.root());
        auto roundtrip_val = val_from_avro(
          datum, schema_field, field_required::yes);

        ASSERT_TRUE(roundtrip_val.has_value());
        auto roundtrip_struct = std::get<std::unique_ptr<struct_value>>(
          std::move(*roundtrip_val));
        ASSERT_EQ(*roundtrip_struct, v);
    }
}

TEST(ValuesAvroTest, TestDecimalConversions) {
    for (int i = 0; i < 10000; ++i) {
        auto high_half = random_generators::get_int<int64_t>();
        auto low_half = random_generators::get_int<uint64_t>();

        auto decimal = absl::MakeInt128(high_half, low_half);

        ASSERT_EQ(decimal, decode_avro_decimal(encode_avro_decimal(decimal)));
        ASSERT_EQ(
          decimal, iobuf_to_avro_decimal(avro_decimal_to_iobuf(decimal, 16)));
    }
}

TEST(ValuesAvroTest, TestDecimalConversionsLimitedSize) {
    auto high_half = 0;
    auto low_half = random_generators::get_int<uint64_t>();

    auto decimal = absl::MakeInt128(high_half, low_half);

    ASSERT_EQ(
      decimal, iobuf_to_avro_decimal(avro_decimal_to_iobuf(decimal, 8)));
}

TEST(ValuesAvroTest, TestDecimalConversionAgainstJavaBigInteger) {
    // value of 65536
    ASSERT_EQ(
      encode_avro_decimal(absl::MakeInt128(0, 65536)),
      bytes({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0}));
    // value of 35209893291843950283695459221
    ASSERT_EQ(
      encode_avro_decimal(absl::MakeInt128(1908732140, 89247320981)),
      bytes({0, 0, 0, 0, 113, 196, 240, 236, 0, 0, 0, 20, 199, 142, 11, 149}));

    // value of -18218949492341193300753118315
    ASSERT_EQ(
      encode_avro_decimal(absl::MakeInt128(-987651231, 89247320981)),
      bytes(
        {255,
         255,
         255,
         255,
         197,
         33,
         163,
         97,
         0,
         0,
         0,
         20,
         199,
         142,
         11,
         149}));
}
