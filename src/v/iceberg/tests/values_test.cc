// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/tests/value_generator.h"
#include "iceberg/values.h"

#include <gtest/gtest.h>

using namespace iceberg;

// Returns a list of unique primitive values.
chunked_vector<value> unique_values_primitive_types() {
    chunked_vector<value> vals;
    vals.emplace_back(boolean_value{true});
    vals.emplace_back(boolean_value{false});

    vals.emplace_back(int_value{0});
    vals.emplace_back(int_value{1});

    vals.emplace_back(long_value{0});
    vals.emplace_back(long_value{1});

    vals.emplace_back(float_value{0.0});
    vals.emplace_back(float_value{1.0});

    vals.emplace_back(double_value{0.0});
    vals.emplace_back(double_value{1.0});

    vals.emplace_back(decimal_value{0});
    vals.emplace_back(decimal_value{1});

    vals.emplace_back(date_value{0});
    vals.emplace_back(date_value{1});

    vals.emplace_back(time_value{0});
    vals.emplace_back(time_value{1});

    vals.emplace_back(timestamp_value{0});
    vals.emplace_back(timestamp_value{1});

    vals.emplace_back(timestamptz_value{0});
    vals.emplace_back(timestamptz_value{1});

    vals.emplace_back(string_value{iobuf{}});
    vals.emplace_back(string_value{iobuf::from("0")});
    vals.emplace_back(string_value{iobuf::from("1")});

    vals.emplace_back(binary_value{iobuf{}});
    vals.emplace_back(binary_value{iobuf::from("0")});
    vals.emplace_back(binary_value{iobuf::from("1")});

    vals.emplace_back(uuid_value{uuid_t{}});
    vals.emplace_back(uuid_value{uuid_t::create()});

    vals.emplace_back(fixed_value{iobuf{}});
    vals.emplace_back(fixed_value{iobuf::from("0")});
    vals.emplace_back(fixed_value{iobuf::from("1")});
    return vals;
};

void check_single_value_exists(
  const value& expected_val, const chunked_vector<value>& all_unique_vals) {
    size_t num_eq = 0;
    size_t num_ne = 0;
    for (const auto& v : all_unique_vals) {
        if (v == expected_val) {
            ++num_eq;
        }
        if (v != expected_val) {
            ++num_ne;
        }
    }
    ASSERT_EQ(num_eq, 1);
    ASSERT_EQ(num_ne, all_unique_vals.size() - 1);
}

TEST(ValuesTest, TestPrimitiveValuesEquality) {
    auto vals = unique_values_primitive_types();
    for (const auto& v : vals) {
        ASSERT_NO_FATAL_FAILURE(check_single_value_exists(v, vals));
    }
}

TEST(ValuesTest, TestStructEquality) {
    struct_value v1;
    v1.fields.emplace_back(int_value{0});
    v1.fields.emplace_back(boolean_value{false});
    ASSERT_EQ(v1, v1);

    struct_value v1_copy;
    v1_copy.fields.emplace_back(int_value{0});
    v1_copy.fields.emplace_back(boolean_value{false});
    ASSERT_EQ(v1, v1_copy);

    struct_value v2;
    v2.fields.emplace_back(int_value{0});
    v2.fields.emplace_back(boolean_value{true});
    ASSERT_NE(v1, v2);

    struct_value v3;
    v3.fields.emplace_back(int_value{1});
    v3.fields.emplace_back(boolean_value{false});
    ASSERT_NE(v1, v3);

    struct_value v4;
    v4.fields.emplace_back(int_value{0});
    ASSERT_NE(v1, v4);

    struct_value v5;
    v5.fields.emplace_back(boolean_value{false});
    ASSERT_NE(v1, v5);

    struct_value v6;
    v6.fields.emplace_back(int_value{0});
    v6.fields.emplace_back(std::nullopt);
    ASSERT_NE(v1, v6);

    struct_value v1_nested;
    v1_nested.fields.emplace_back(
      std::make_unique<struct_value>(std::move(v1_copy)));
    ASSERT_EQ(
      v1, *std::get<std::unique_ptr<struct_value>>(*v1_nested.fields[0]));
    ASSERT_NE(v1, v1_nested);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    ASSERT_NE(v1, v1_copy);

    struct_value v_null;
    v_null.fields.emplace_back(std::nullopt);
    v_null.fields.emplace_back(std::nullopt);
    ASSERT_NE(v1, v_null);
    ASSERT_EQ(v_null, v_null);
}

TEST(ValuesTest, TestListEquality) {
    list_value v1;
    v1.elements.emplace_back(int_value{0});
    v1.elements.emplace_back(int_value{1});
    ASSERT_EQ(v1, v1);

    list_value v1_copy;
    v1_copy.elements.emplace_back(int_value{0});
    v1_copy.elements.emplace_back(int_value{1});
    ASSERT_EQ(v1, v1_copy);

    list_value v2;
    v2.elements.emplace_back(int_value{0});
    v2.elements.emplace_back(int_value{2});
    ASSERT_NE(v1, v2);

    list_value v3;
    v3.elements.emplace_back(int_value{2});
    v3.elements.emplace_back(int_value{0});
    ASSERT_NE(v1, v3);

    list_value v4;
    v4.elements.emplace_back(int_value{0});
    ASSERT_NE(v1, v4);

    list_value v5;
    v5.elements.emplace_back(int_value{0});
    v5.elements.emplace_back(std::nullopt);
    ASSERT_NE(v1, v5);

    list_value v1_nested;
    v1_nested.elements.emplace_back(
      std::make_unique<list_value>(std::move(v1_copy)));
    ASSERT_EQ(
      v1, *std::get<std::unique_ptr<list_value>>(*v1_nested.elements[0]));
    ASSERT_NE(v1, v1_nested);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    ASSERT_NE(v1, v1_copy);

    list_value v_null;
    v_null.elements.emplace_back(std::nullopt);
    v_null.elements.emplace_back(std::nullopt);
    ASSERT_NE(v1, v_null);
    ASSERT_EQ(v_null, v_null);
}

TEST(ValuesTest, TestMapEquality) {
    map_value v1;
    v1.kvs.emplace_back(kv_value{int_value{0}, boolean_value{false}});
    v1.kvs.emplace_back(kv_value{int_value{1}, boolean_value{true}});
    ASSERT_EQ(v1, v1);

    map_value v1_copy;
    v1_copy.kvs.emplace_back(kv_value{int_value{0}, boolean_value{false}});
    v1_copy.kvs.emplace_back(kv_value{int_value{1}, boolean_value{true}});
    ASSERT_EQ(v1, v1_copy);

    map_value v2;
    v2.kvs.emplace_back(kv_value{int_value{0}, boolean_value{false}});
    v2.kvs.emplace_back(kv_value{int_value{2}, boolean_value{true}});
    ASSERT_NE(v1, v2);

    map_value v3;
    v3.kvs.emplace_back(kv_value{int_value{0}, boolean_value{false}});
    v3.kvs.emplace_back(kv_value{int_value{1}, boolean_value{false}});
    ASSERT_NE(v1, v3);

    map_value v4;
    v4.kvs.emplace_back(kv_value{int_value{1}, boolean_value{false}});
    v4.kvs.emplace_back(kv_value{int_value{1}, boolean_value{true}});
    ASSERT_NE(v1, v4);

    map_value v5;
    v5.kvs.emplace_back(kv_value{int_value{0}, boolean_value{false}});
    ASSERT_NE(v1, v5);

    map_value v6;
    v6.kvs.emplace_back(kv_value{int_value{0}, std::nullopt});
    v6.kvs.emplace_back(kv_value{int_value{1}, boolean_value{true}});
    ASSERT_NE(v1, v6);

    map_value v1_nested;
    v1_nested.kvs.emplace_back(
      kv_value{std::make_unique<map_value>(std::move(v1_copy)), std::nullopt});
    ASSERT_NE(v1, v1_nested);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    ASSERT_NE(v1, v1_copy);

    map_value v_null_1;
    v_null_1.kvs.emplace_back(kv_value{int_value{0}, std::nullopt});
    map_value v_null_2;
    v_null_2.kvs.emplace_back(kv_value{int_value{0}, std::nullopt});
    ASSERT_EQ(v_null_1, v_null_2);

    map_value v_empty;
    ASSERT_EQ(v_empty, v_empty);
    ASSERT_EQ(v_empty, map_value{});
    ASSERT_NE(v_empty, v_null_1);
    ASSERT_NE(v_empty, v1);
}

TEST(ValuesTest, TestNestedValueEquality) {
    value v1{std::make_unique<struct_value>()};
    value v1_copy{std::make_unique<struct_value>()};
    value v2{std::make_unique<list_value>()};
    value v2_copy{std::make_unique<list_value>()};
    value v3{std::make_unique<map_value>()};
    value v3_copy{std::make_unique<map_value>()};
    ASSERT_EQ(v1, v1_copy);
    ASSERT_EQ(v2, v2_copy);
    ASSERT_EQ(v3, v3_copy);
    ASSERT_NE(v1, v2);
    ASSERT_NE(v1, v3);
    ASSERT_NE(v2, v3);
}

TEST(ValuesTest, TestPrimitiveTypesOStream) {
    EXPECT_STREQ("boolean(true)", fmt::to_string(boolean_value{true}).c_str());
    EXPECT_STREQ(
      "boolean(false)", fmt::to_string(boolean_value{false}).c_str());

    EXPECT_STREQ(
      "int(-2147483648)",
      fmt::to_string(int_value{std::numeric_limits<int32_t>::min()}).c_str());
    EXPECT_STREQ(
      "int(2147483647)",
      fmt::to_string(int_value{std::numeric_limits<int32_t>::max()}).c_str());

    EXPECT_STREQ(
      "long(-9223372036854775808)",
      fmt::to_string(long_value{std::numeric_limits<long>::min()}).c_str());
    EXPECT_STREQ(
      "long(9223372036854775807)",
      fmt::to_string(long_value{std::numeric_limits<long>::max()}).c_str());

    EXPECT_STREQ(
      "double(-1.7976931348623157e+308)",
      fmt::to_string(double_value{-std::numeric_limits<double>::max()})
        .c_str());
    EXPECT_STREQ(
      "double(2.2250738585072014e-308)",
      fmt::to_string(double_value{std::numeric_limits<double>::min()}).c_str());
    EXPECT_STREQ(
      "double(1.7976931348623157e+308)",
      fmt::to_string(double_value{std::numeric_limits<double>::max()}).c_str());

    EXPECT_STREQ(
      "decimal(-170141183460469231731687303715884105728)",
      fmt::to_string(decimal_value{std::numeric_limits<absl::int128>::min()})
        .c_str());
    EXPECT_STREQ(
      "decimal(170141183460469231731687303715884105727)",
      fmt::to_string(decimal_value{std::numeric_limits<absl::int128>::max()})
        .c_str());

    EXPECT_STREQ(
      "date(-2147483648)",
      fmt::to_string(date_value{std::numeric_limits<int32_t>::min()}).c_str());
    EXPECT_STREQ(
      "date(2147483647)",
      fmt::to_string(date_value{std::numeric_limits<int32_t>::max()}).c_str());

    EXPECT_STREQ(
      "time(-9223372036854775808)",
      fmt::to_string(time_value{std::numeric_limits<long>::min()}).c_str());
    EXPECT_STREQ(
      "time(9223372036854775807)",
      fmt::to_string(time_value{std::numeric_limits<long>::max()}).c_str());
    EXPECT_STREQ(
      "timestamp(-9223372036854775808)",
      fmt::to_string(timestamp_value{std::numeric_limits<long>::min()})
        .c_str());
    EXPECT_STREQ(
      "timestamp(9223372036854775807)",
      fmt::to_string(timestamp_value{std::numeric_limits<long>::max()})
        .c_str());
    EXPECT_STREQ(
      "timestamptz(-9223372036854775808)",
      fmt::to_string(timestamptz_value{std::numeric_limits<long>::min()})
        .c_str());
    EXPECT_STREQ(
      "timestamptz(9223372036854775807)",
      fmt::to_string(timestamptz_value{std::numeric_limits<long>::max()})
        .c_str());

    EXPECT_STREQ("string(\"\")", fmt::to_string(string_value{}).c_str());
    EXPECT_STREQ(
      "string(\"0000000000000000\")",
      fmt::to_string(string_value{iobuf::from("0000000000000000")}).c_str());
    EXPECT_STREQ(
      "string(\"0000000000000000...\")",
      fmt::to_string(string_value{iobuf::from("00000000000000000")}).c_str());

    EXPECT_STREQ(
      "uuid(00000000-0000-0000-0000-000000000000)",
      fmt::to_string(uuid_value{}).c_str());
    EXPECT_STREQ(
      "uuid(deadbeef-0000-0000-0000-000000000000)",
      fmt::to_string(
        uuid_value{uuid_t::from_string("deadbeef-0000-0000-0000-000000000000")})
        .c_str());

    EXPECT_STREQ(
      "binary(size_bytes=0)", fmt::to_string(binary_value{}).c_str());
    EXPECT_STREQ(
      "binary(size_bytes=16)",
      fmt::to_string(binary_value{iobuf::from("0000000000000000")}).c_str());

    EXPECT_STREQ("fixed(size_bytes=0)", fmt::to_string(fixed_value{}).c_str());
    EXPECT_STREQ(
      "fixed(size_bytes=16)",
      fmt::to_string(fixed_value{iobuf::from("0000000000000000")}).c_str());
}

TEST(ValuesTest, TestStructOStream) {
    struct_value val;
    EXPECT_STREQ("struct{}", fmt::to_string(val).c_str());
    val.fields.emplace_back(std::nullopt);
    EXPECT_STREQ("struct{none, }", fmt::to_string(val).c_str());
    val.fields.emplace_back(int_value{1});
    EXPECT_STREQ("struct{none, int(1), }", fmt::to_string(val).c_str());
    val.fields.emplace_back(long_value{1});
    val.fields.emplace_back(long_value{1});
    val.fields.emplace_back(long_value{1});
    EXPECT_STREQ(
      "struct{none, int(1), long(1), long(1), long(1), }",
      fmt::to_string(val).c_str());
    val.fields.emplace_back(int_value{1});
    EXPECT_STREQ(
      "struct{none, int(1), long(1), long(1), long(1), ...}",
      fmt::to_string(val).c_str());
}

TEST(ValuesTest, TestListOStream) {
    list_value val;
    EXPECT_STREQ("list{}", fmt::to_string(val).c_str());
    val.elements.emplace_back(std::nullopt);
    EXPECT_STREQ("list{none, }", fmt::to_string(val).c_str());
    val.elements.emplace_back(int_value{1});
    EXPECT_STREQ("list{none, int(1), }", fmt::to_string(val).c_str());
    val.elements.emplace_back(int_value{1});
    val.elements.emplace_back(int_value{1});
    val.elements.emplace_back(int_value{1});
    EXPECT_STREQ(
      "list{none, int(1), int(1), int(1), int(1), }",
      fmt::to_string(val).c_str());
    val.elements.emplace_back(int_value{1});
    EXPECT_STREQ(
      "list{none, int(1), int(1), int(1), int(1), ...}",
      fmt::to_string(val).c_str());
}

TEST(ValuesTest, TestMapOStream) {
    map_value val;
    EXPECT_STREQ("map{}", fmt::to_string(val).c_str());
    val.kvs.emplace_back(kv_value{int_value{0}, std::nullopt});
    EXPECT_STREQ("map{(k=int(0), v=none), }", fmt::to_string(val).c_str());
    val.kvs.emplace_back(kv_value{int_value{0}, int_value{1}});
    EXPECT_STREQ(
      "map{(k=int(0), v=none), (k=int(0), v=int(1)), }",
      fmt::to_string(val).c_str());
    val.kvs.emplace_back(kv_value{int_value{0}, std::nullopt});
    val.kvs.emplace_back(kv_value{int_value{0}, std::nullopt});
    val.kvs.emplace_back(kv_value{int_value{0}, std::nullopt});
    EXPECT_STREQ(
      "map{(k=int(0), v=none), (k=int(0), v=int(1)), (k=int(0), v=none), "
      "(k=int(0), v=none), (k=int(0), v=none), }",
      fmt::to_string(val).c_str());
    val.kvs.emplace_back(kv_value{int_value{0}, std::nullopt});
    EXPECT_STREQ(
      "map{(k=int(0), v=none), (k=int(0), v=int(1)), (k=int(0), v=none), "
      "(k=int(0), v=none), (k=int(0), v=none), ...}",
      fmt::to_string(val).c_str());
}

TEST(ValuesTest, TestValueOStream) {
    auto schema_field_type = test_nested_schema_type();
    auto zero_val = tests::make_value({.max_elements = 1}, schema_field_type);
    EXPECT_STREQ(
      "struct{string(\"\"), int(0), boolean(false), list{string(\"\"), }, "
      "map{(k=string(\"\"), v=map{(k=string(\"\"), v=int(0)), }), }, ...}",
      fmt::to_string(zero_val).c_str());
}

TEST(ValuesTest, TestHashContainerStructs) {
    // Test that we can also keep a container of structs, which will roughly be
    // equivalent to a Kafka record.
    chunked_hash_set<struct_value> vs;
    auto unique_vals = unique_values_primitive_types();
    struct_value val;
    for (auto& v : unique_vals) {
        val.fields.emplace_back(std::move(v));
    }
    vs.emplace(std::move(val));
    ASSERT_EQ(1, vs.size());
}

TEST(ValuesTest, TestHashContainer) {
    chunked_hash_set<value> vs;
    auto unique_vals = unique_values_primitive_types();
    auto expected_size = unique_vals.size();
    for (auto& v : unique_vals) {
        vs.emplace(value(std::move(v)));
    }
    ASSERT_EQ(expected_size, vs.size());
}

TEST(ValuesTest, TestPrimitivesCopy) {
    auto vals = unique_values_primitive_types();
    for (const auto& v : vals) {
        auto v_copy = make_copy(std::get<primitive_value>(v));
        ASSERT_EQ(v, value{std::move(v_copy)});
    }
}

TEST(ValuesTest, TestLessThan) {
    auto vals = unique_values_primitive_types();
    for (const auto& v : vals) {
        ASSERT_FALSE(v < v);
    }
    ASSERT_LT(value{boolean_value{false}}, value{boolean_value{true}});
    ASSERT_LT(value{int_value{0}}, value{int_value{1}});
    ASSERT_LT(value{long_value{0}}, value{long_value{1}});
    ASSERT_LT(value{float_value{0.0}}, value{float_value{1.0}});
    ASSERT_LT(value{double_value{0.0}}, value{double_value{1.0}});
    ASSERT_LT(value{decimal_value{0}}, value{decimal_value{1}});
    ASSERT_LT(value{date_value{0}}, value{date_value{1}});
    ASSERT_LT(value{time_value{0}}, value{time_value{1}});
    ASSERT_LT(value{timestamp_value{0}}, value{timestamp_value{1}});
    ASSERT_LT(value{timestamptz_value{0}}, value{timestamptz_value{1}});
    ASSERT_LT(
      value{string_value{iobuf::from("aaa")}},
      value{string_value{iobuf::from("aab")}});
    ASSERT_LT(
      value{binary_value{iobuf::from("aaa")}},
      value{binary_value{iobuf::from("aab")}});
    ASSERT_LT(value{uuid_value{uuid_t{}}}, value{uuid_value{uuid_t::create()}});
    ASSERT_LT(
      value{fixed_value{iobuf::from("aaa")}},
      value{fixed_value{iobuf::from("aab")}});
}
