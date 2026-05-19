/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/avro_decimal.h"
#include "iceberg/schema_avro.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/tests/value_generator.h"
#include "iceberg/values.h"
#include "iceberg/values_avro.h"
#include "random/generators.h"

#include <avro/GenericDatum.hh>
#include <avro/LogicalType.hh>
#include <avro/Schema.hh>
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
    st.fields.push_back(
      nested_field::create(
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

    // Values must fit decimal(10,2): magnitude < 10^10.
    for (auto& v : {
           make_struct(9999999999),
           make_struct(-9999999999),
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

        ASSERT_EQ(
          decimal,
          decode_avro_decimal(
            encode_avro_fixed_decimal(decimal, max_decimal_bytes)));
        ASSERT_EQ(
          decimal,
          iobuf_to_avro_decimal(
            avro_fixed_decimal_to_iobuf(decimal, max_decimal_bytes)));
    }
}

TEST(ValuesAvroTest, TestDecimalConversionsLimitedSize) {
    // Value must fit in a signed 8-byte slot — pick from int64_t range.
    absl::int128 decimal{random_generators::get_int<int64_t>()};

    ASSERT_EQ(
      decimal, iobuf_to_avro_decimal(avro_fixed_decimal_to_iobuf(decimal, 8)));
}

TEST(ValuesAvroTest, TestDecimalConversionAgainstJavaBigInteger) {
    // value of 65536
    ASSERT_EQ(
      encode_avro_fixed_decimal(absl::MakeInt128(0, 65536), max_decimal_bytes),
      bytes({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0}));
    // value of 35209893291843950283695459221
    ASSERT_EQ(
      encode_avro_fixed_decimal(
        absl::MakeInt128(1908732140, 89247320981), max_decimal_bytes),
      bytes({0, 0, 0, 0, 113, 196, 240, 236, 0, 0, 0, 20, 199, 142, 11, 149}));

    // value of -18218949492341193300753118315
    ASSERT_EQ(
      encode_avro_fixed_decimal(
        absl::MakeInt128(-987651231, 89247320981), max_decimal_bytes),
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

// Decode a decimal value packed in an Avro fixed[N] payload where N is the
// minimum byte width for the column's precision (Iceberg spec). Hand-craft
// the datum so the decoder is exercised independently of the encoder.
namespace {
avro::GenericDatum make_decimal_fixed_datum(
  const decimal_type& dt, const std::vector<uint8_t>& payload) {
    auto n = static_cast<int>(payload.size());
    auto schema = avro::FixedSchema(n, "decimal");
    avro::LogicalType l_type(avro::LogicalType::DECIMAL);
    l_type.setPrecision(static_cast<int>(dt.precision));
    l_type.setScale(static_cast<int>(dt.scale));
    schema.root()->setLogicalType(l_type);
    return {schema.root(), avro::GenericFixed(schema.root(), payload)};
}
} // namespace

TEST(ValuesAvroTest, TestEncodeAvroFixedDecimalThrows) {
    EXPECT_THROW(encode_avro_fixed_decimal(0, 0), std::invalid_argument);
    EXPECT_THROW(
      encode_avro_fixed_decimal(0, max_decimal_bytes + 1),
      std::invalid_argument);
    // 128 does not fit in a signed 1-byte slot (range [-128, 127]).
    EXPECT_THROW(encode_avro_fixed_decimal(128, 1), std::invalid_argument);
    EXPECT_THROW(encode_avro_fixed_decimal(-129, 1), std::invalid_argument);
    // -128 is the inclusive lower bound and must succeed.
    EXPECT_NO_THROW(encode_avro_fixed_decimal(-128, 1));
    EXPECT_NO_THROW(encode_avro_fixed_decimal(127, 1));
}

TEST(ValuesAvroTest, TestDecodeIcebergFixedDecimal) {
    // decimal(10,2) → fixed[5]. 1234567 (=12345.67) and -1234567 (=-12345.67)
    // both fit in 3 bytes but must be sign-extended to 5 to fill the slot.
    const decimal_type dt{.precision = 10, .scale = 2};
    field_type expected_type{dt};

    struct case_t {
        absl::int128 value;
        std::vector<uint8_t> fixed5;
    };
    const std::array cases{
      case_t{1234567, {0x00, 0x00, 0x12, 0xD6, 0x87}},
      case_t{-1234567, {0xFF, 0xFF, 0xED, 0x29, 0x79}},
      case_t{0, {0x00, 0x00, 0x00, 0x00, 0x00}},
      case_t{-1, {0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
    };
    for (const auto& c : cases) {
        auto datum = make_decimal_fixed_datum(dt, c.fixed5);
        auto parsed = val_from_avro(datum, expected_type, field_required::yes);
        ASSERT_TRUE(parsed.has_value());
        const auto& dv = std::get<decimal_value>(
          std::get<primitive_value>(*parsed));
        ASSERT_EQ(dv.val, c.value);
    }
}
