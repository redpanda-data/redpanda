// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/datatypes.h"

#include <gtest/gtest.h>

#include <variant>

using namespace iceberg;

chunked_vector<field_type> all_types() {
    chunked_vector<field_type> all_types;

    // Primitive types.
    all_types.emplace_back(boolean_type{});
    all_types.emplace_back(int_type{});
    all_types.emplace_back(long_type{});
    all_types.emplace_back(float_type{});
    all_types.emplace_back(double_type{});
    all_types.emplace_back(decimal_type{.precision = 123, .scale = 456});
    all_types.emplace_back(date_type{});
    all_types.emplace_back(time_type{});
    all_types.emplace_back(timestamp_type{});
    all_types.emplace_back(timestamptz_type{});
    all_types.emplace_back(string_type{});
    all_types.emplace_back(uuid_type{});
    all_types.emplace_back(fixed_type{
      .length = 789,
    });
    all_types.emplace_back(binary_type{});

    // Complex types.
    all_types.emplace_back(
      list_type::create(1, field_required::yes, string_type{}));
    chunked_vector<nested_field_ptr> struct_fields;
    struct_fields.emplace_back(
      nested_field::create(1, "foo", field_required::yes, string_type{}));
    all_types.emplace_back(struct_type{
      .fields = std::move(struct_fields),
    });
    all_types.emplace_back(map_type::create(
      0, string_type{}, 1, field_required::yes, string_type{}));
    return all_types;
};

// Use the equality operator to check that the given type only exists once in
// the given list.
void check_single_type_exists(
  const field_type& expected_type,
  const chunked_vector<field_type>& all_types) {
    size_t num_eq = 0;
    size_t num_ne = 0;
    for (const auto& t : all_types) {
        if (t == expected_type) {
            ++num_eq;
        }
        if (t != expected_type) {
            ++num_ne;
        }
    }
    ASSERT_EQ(num_eq, 1);
    ASSERT_EQ(num_ne, all_types.size() - 1);
}

TEST(DatatypeTest, TestTypesEquality) {
    auto types = all_types();
    for (const auto& t : types) {
        ASSERT_NO_FATAL_FAILURE(check_single_type_exists(t, types));
    }
}
TEST(DatatypeTest, TestBoolean) {
    ASSERT_EQ(field_type{boolean_type{}}, field_type{boolean_type{}});
    ASSERT_EQ("boolean", fmt::format("{}", boolean_type{}));
    ASSERT_EQ("boolean", fmt::format("{}", field_type{boolean_type{}}));
}
TEST(DatatypeTest, TestInt) {
    ASSERT_EQ(field_type{int_type{}}, field_type{int_type{}});
    ASSERT_EQ("int", fmt::format("{}", int_type{}));
    ASSERT_EQ("int", fmt::format("{}", field_type{int_type{}}));
}
TEST(DatatypeTest, TestLong) {
    ASSERT_EQ(field_type{long_type{}}, field_type{long_type{}});
    ASSERT_EQ("long", fmt::format("{}", long_type{}));
    ASSERT_EQ("long", fmt::format("{}", field_type{long_type{}}));
}
TEST(DatatypeTest, TestFloat) {
    ASSERT_EQ(field_type{float_type{}}, field_type{float_type{}});
    ASSERT_EQ("float", fmt::format("{}", float_type{}));
    ASSERT_EQ("float", fmt::format("{}", field_type{float_type{}}));
}
TEST(DatatypeTest, TestDouble) {
    ASSERT_EQ(field_type{double_type{}}, field_type{double_type{}});
    ASSERT_EQ("double", fmt::format("{}", double_type{}));
    ASSERT_EQ("double", fmt::format("{}", field_type{double_type{}}));
}
TEST(DatatypeTest, TestDecimal) {
    decimal_type t1{
      .precision = 123,
      .scale = 456,
    };
    auto t1_copy = t1;
    auto t2 = t1;
    t2.scale = 0;
    auto t3 = t1;
    t3.precision = 0;
    ASSERT_EQ(field_type{t1}, field_type{t1});
    ASSERT_EQ(field_type{t1}, field_type{t1_copy});
    ASSERT_NE(field_type{t1}, field_type{t2});
    ASSERT_NE(field_type{t1}, field_type{t3});
    ASSERT_EQ("decimal(123, 456)", fmt::format("{}", t1));
    ASSERT_EQ("decimal(123, 456)", fmt::format("{}", t1_copy));
    ASSERT_EQ("decimal(123, 0)", fmt::format("{}", t2));
    ASSERT_EQ("decimal(0, 456)", fmt::format("{}", t3));
    ASSERT_EQ("decimal(123, 456)", fmt::format("{}", field_type{t1}));
    ASSERT_EQ("decimal(123, 456)", fmt::format("{}", field_type{t1_copy}));
    ASSERT_EQ("decimal(123, 0)", fmt::format("{}", field_type{t2}));
    ASSERT_EQ("decimal(0, 456)", fmt::format("{}", field_type{t3}));
}
TEST(DatatypeTest, TestDate) {
    ASSERT_EQ(field_type{date_type{}}, field_type{date_type{}});
    ASSERT_EQ("date", fmt::format("{}", date_type{}));
    ASSERT_EQ("date", fmt::format("{}", field_type{date_type{}}));
}
TEST(DatatypeTest, TestTime) {
    ASSERT_EQ(field_type{time_type{}}, field_type{time_type{}});
    ASSERT_EQ("time", fmt::format("{}", time_type{}));
    ASSERT_EQ("time", fmt::format("{}", field_type{time_type{}}));
}
TEST(DatatypeTest, TestTimestamp) {
    ASSERT_EQ(field_type{timestamp_type{}}, field_type{timestamp_type{}});
    ASSERT_EQ("timestamp", fmt::format("{}", timestamp_type{}));
    ASSERT_EQ("timestamp", fmt::format("{}", field_type{timestamp_type{}}));
}
TEST(DatatypeTest, TestTimestamptz) {
    ASSERT_EQ(field_type{timestamptz_type{}}, field_type{timestamptz_type{}});
    ASSERT_EQ("timestamptz", fmt::format("{}", timestamptz_type{}));
    ASSERT_EQ("timestamptz", fmt::format("{}", field_type{timestamptz_type{}}));
}
TEST(DatatypeTest, TestString) {
    ASSERT_EQ(field_type{string_type{}}, field_type{string_type{}});
    ASSERT_EQ("string", fmt::format("{}", string_type{}));
    ASSERT_EQ("string", fmt::format("{}", field_type{string_type{}}));
}
TEST(DatatypeTest, TestUuid) {
    ASSERT_EQ(field_type{uuid_type{}}, field_type{uuid_type{}});
    ASSERT_EQ("uuid", fmt::format("{}", uuid_type{}));
    ASSERT_EQ("uuid", fmt::format("{}", field_type{uuid_type{}}));
}
TEST(DatatypeTest, TestFixed) {
    fixed_type t1{
      .length = 12345,
    };
    auto t1_copy = t1;
    fixed_type t2{
      .length = 54321,
    };
    ASSERT_EQ(field_type{t1}, field_type{t1});
    ASSERT_EQ(field_type{t1}, field_type{t1_copy});
    ASSERT_NE(field_type{t1}, field_type{t2});
    ASSERT_EQ("fixed[12345]", fmt::format("{}", t1));
    ASSERT_EQ("fixed[12345]", fmt::format("{}", t1_copy));
    ASSERT_EQ("fixed[54321]", fmt::format("{}", t2));
    ASSERT_EQ("fixed[12345]", fmt::format("{}", field_type{t1}));
    ASSERT_EQ("fixed[12345]", fmt::format("{}", field_type{t1_copy}));
    ASSERT_EQ("fixed[54321]", fmt::format("{}", field_type{t2}));
}
TEST(DatatypeTest, TestBinary) {
    ASSERT_EQ(field_type{binary_type{}}, field_type{binary_type{}});
    ASSERT_EQ("binary", fmt::format("{}", binary_type{}));
    ASSERT_EQ("binary", fmt::format("{}", field_type{binary_type{}}));
}
TEST(DatatypeTest, TestList) {
    auto t1 = field_type{
      list_type::create(1, field_required::yes, boolean_type{})};
    auto t1_dup = field_type{
      list_type::create(1, field_required::yes, boolean_type{})};
    ASSERT_EQ(t1, t1);
    ASSERT_EQ(t1, t1_dup);

    auto t2 = field_type{
      list_type::create(2, field_required::yes, boolean_type{})};
    auto t3 = field_type{
      list_type::create(1, field_required::no, boolean_type{})};
    auto t4 = field_type{
      list_type::create(1, field_required::yes, string_type{})};
    ASSERT_NE(t1, t2);
    ASSERT_NE(t1, t3);
    ASSERT_NE(t1, t4);

    // Moving the type will empty it.
    auto t1_move = std::move(t1);
    // NOLINTBEGIN(bugprone-use-after-move)
    ASSERT_TRUE(std::holds_alternative<list_type>(t1));
    ASSERT_TRUE(std::get<list_type>(t1).element_field == nullptr);
    ASSERT_NE(t1_move, t1);
    ASSERT_EQ("list", fmt::format("{}", t1));
    // NOLINTEND(bugprone-use-after-move)

    ASSERT_EQ(t1_move, t1_dup);
    ASSERT_EQ("list", fmt::format("{}", t2));
    ASSERT_EQ("list", fmt::format("{}", t3));
    ASSERT_EQ("list", fmt::format("{}", t4));
    ASSERT_EQ("list", fmt::format("{}", t1_dup));
    ASSERT_EQ("list", fmt::format("{}", t1_move));
}
TEST(DatatypeTest, TestMap) {
    auto t1 = field_type{map_type::create(
      0, string_type{}, 1, field_required::yes, string_type{})};
    auto t1_dup = field_type{map_type::create(
      0, string_type{}, 1, field_required::yes, string_type{})};
    ASSERT_EQ(t1, t1);
    ASSERT_EQ(t1, t1_dup);

    auto t2 = field_type{map_type::create(
      1, string_type{}, 1, field_required::yes, string_type{})};
    auto t3 = field_type{map_type::create(
      0, boolean_type{}, 1, field_required::yes, string_type{})};
    auto t4 = field_type{
      map_type::create(0, string_type{}, 2, field_required::no, string_type{})};
    auto t5 = field_type{map_type::create(
      0, string_type{}, 1, field_required::yes, boolean_type{})};
    ASSERT_NE(t1, t2);
    ASSERT_NE(t1, t3);
    ASSERT_NE(t1, t4);
    ASSERT_NE(t1, t5);

    // Moving the type will empty it.
    auto t1_move = std::move(t1);
    // NOLINTBEGIN(bugprone-use-after-move)
    ASSERT_TRUE(std::holds_alternative<map_type>(t1));
    ASSERT_TRUE(std::get<map_type>(t1).key_field == nullptr);
    ASSERT_TRUE(std::get<map_type>(t1).value_field == nullptr);
    ASSERT_NE(t1_move, t1);
    ASSERT_EQ("map", fmt::format("{}", t1));
    // NOLINTEND(bugprone-use-after-move)

    ASSERT_EQ(t1_move, t1_dup);
    ASSERT_EQ("map", fmt::format("{}", t2));
    ASSERT_EQ("map", fmt::format("{}", t3));
    ASSERT_EQ("map", fmt::format("{}", t4));
    ASSERT_EQ("map", fmt::format("{}", t1_dup));
    ASSERT_EQ("map", fmt::format("{}", t1_move));

    // Regression test that would cause previous impl to crash if both types
    // are the same but keys are null.
    std::get<map_type>(t1_move).key_field.reset();
    std::get<map_type>(t1_dup).key_field.reset();
    ASSERT_EQ(t1_move, t1_dup);
}
TEST(DatatypeTest, TestStruct) {
    // Constructs a struct_type with a single field.
    auto struct_single =
      [](int32_t i, const ss::sstring& name, field_required req, field_type t) {
          chunked_vector<nested_field_ptr> struct_fields;
          struct_fields.emplace_back(
            nested_field::create(i, name, req, std::move(t)));
          return field_type{struct_type{
            .fields = std::move(struct_fields),
          }};
      };
    auto t1 = struct_single(0, "foo", field_required::yes, string_type{});
    auto t1_dup = struct_single(0, "foo", field_required::yes, string_type{});
    auto t2 = struct_single(1, "foo", field_required::yes, string_type{});
    auto t3 = struct_single(0, "food", field_required::yes, string_type{});
    auto t4 = struct_single(0, "foo", field_required::no, string_type{});
    auto t5 = struct_single(0, "foo", field_required::yes, boolean_type{});
    ASSERT_EQ(t1, t1);
    ASSERT_EQ(t1, t1_dup);
    ASSERT_NE(t1, t2);
    ASSERT_NE(t1, t3);
    ASSERT_NE(t1, t4);
    ASSERT_NE(t1, t5);

    // Moving the type will empty it.
    auto t1_move = std::move(t1);
    // NOLINTBEGIN(bugprone-use-after-move)
    ASSERT_TRUE(std::holds_alternative<struct_type>(t1));
    ASSERT_TRUE(std::get<struct_type>(t1).fields.empty());
    ASSERT_NE(t1_move, t1);
    ASSERT_EQ("struct", fmt::format("{}", t1));
    // NOLINTEND(bugprone-use-after-move)

    ASSERT_EQ(t1_move, t1_dup);
    ASSERT_EQ("struct", fmt::format("{}", t2));
    ASSERT_EQ("struct", fmt::format("{}", t3));
    ASSERT_EQ("struct", fmt::format("{}", t4));
    ASSERT_EQ("struct", fmt::format("{}", t5));
    ASSERT_EQ("struct", fmt::format("{}", t1_dup));
    ASSERT_EQ("struct", fmt::format("{}", t1_move));
}

TEST(DatatypeTest, TestBasicCopy) {
    auto types = all_types();
    chunked_vector<field_type> types_copy;
    for (const auto& t : types) {
        types_copy.emplace_back(make_copy(t));
    }
    ASSERT_EQ(types, types_copy);

    types.clear();
    ASSERT_NE(types, types_copy);
}

namespace {

field_type make_nested_struct(int depth) {
    struct_type ret;
    if (depth == 0) {
        ret.fields.emplace_back(nested_field::create(
          0, "d0", field_required::yes, primitive_type{int_type{}}));
        return ret;
    }
    ret.fields.emplace_back(nested_field::create(
      depth,
      fmt::format("d{}", depth),
      field_required::yes,
      make_nested_struct(depth - 1)));
    return ret;
}

} // namespace

TEST(DatatypeTest, TestNestedCopyStruct) {
    auto nested_struct = make_nested_struct(3);
    auto nested_copy = make_copy(nested_struct);
    ASSERT_TRUE(std::holds_alternative<struct_type>(nested_copy));
    const auto& d3 = std::get<struct_type>(nested_copy);
    ASSERT_EQ(1, d3.fields.size());
    ASSERT_EQ(d3.fields[0]->id(), 3);
    ASSERT_STREQ(d3.fields[0]->name.c_str(), "d3");
    ASSERT_TRUE(std::holds_alternative<struct_type>(d3.fields[0]->type));

    const auto& d2 = std::get<struct_type>(d3.fields[0]->type);
    ASSERT_EQ(1, d2.fields.size());
    ASSERT_EQ(d2.fields[0]->id(), 2);
    ASSERT_STREQ(d2.fields[0]->name.c_str(), "d2");
    ASSERT_TRUE(std::holds_alternative<struct_type>(d2.fields[0]->type));

    const auto& d1 = std::get<struct_type>(d2.fields[0]->type);
    ASSERT_EQ(1, d1.fields.size());
    ASSERT_EQ(d1.fields[0]->id(), 1);
    ASSERT_STREQ(d1.fields[0]->name.c_str(), "d1");
    ASSERT_TRUE(std::holds_alternative<struct_type>(d1.fields[0]->type));

    const auto& d0 = std::get<struct_type>(d1.fields[0]->type);
    ASSERT_EQ(1, d0.fields.size());
    ASSERT_EQ(d0.fields[0]->id(), 0);
    ASSERT_STREQ(d0.fields[0]->name.c_str(), "d0");
    ASSERT_TRUE(std::holds_alternative<int_type>(
      std::get<primitive_type>(d0.fields[0]->type)));
}

TEST(DatatypeTest, TestNestedCopyList) {
    field_type parent{list_type::create(
      0,
      field_required::yes,
      field_type{
        list_type::create(1, field_required::no, field_type{int_type{}})})};
    auto nested_copy = make_copy(parent);
    ASSERT_TRUE(std::holds_alternative<list_type>(nested_copy));
    const auto& type_copy = std::get<list_type>(nested_copy);
    ASSERT_EQ(0, type_copy.element_field->id());
    ASSERT_TRUE(type_copy.element_field->required);

    ASSERT_TRUE(
      std::holds_alternative<list_type>(type_copy.element_field->type));
    const auto& child_copy = std::get<list_type>(type_copy.element_field->type);
    ASSERT_EQ(1, child_copy.element_field->id());
    ASSERT_FALSE(child_copy.element_field->required);
    ASSERT_TRUE(
      std::holds_alternative<primitive_type>(child_copy.element_field->type));
    ASSERT_TRUE(std::holds_alternative<int_type>(
      std::get<primitive_type>(child_copy.element_field->type)));
}

TEST(DatatypeTest, TestNestedCopyMap) {
    field_type parent{map_type::create(
      0,
      field_type{
        list_type::create(1, field_required::no, field_type{int_type{}})},
      2,
      field_required::yes,
      field_type{
        list_type::create(3, field_required::no, field_type{int_type{}})})};
    auto nested_copy = make_copy(parent);
    ASSERT_TRUE(std::holds_alternative<map_type>(nested_copy));
    const auto& type_copy = std::get<map_type>(nested_copy);
    ASSERT_EQ(0, type_copy.key_field->id());
    ASSERT_EQ(2, type_copy.value_field->id());

    ASSERT_TRUE(std::holds_alternative<list_type>(type_copy.key_field->type));
    const auto& key_copy = std::get<list_type>(type_copy.key_field->type);
    ASSERT_EQ(1, key_copy.element_field->id());
    ASSERT_FALSE(key_copy.element_field->required);
    ASSERT_TRUE(
      std::holds_alternative<primitive_type>(key_copy.element_field->type));
    ASSERT_TRUE(std::holds_alternative<int_type>(
      std::get<primitive_type>(key_copy.element_field->type)));

    ASSERT_TRUE(std::holds_alternative<list_type>(type_copy.value_field->type));
    const auto& value_copy = std::get<list_type>(type_copy.value_field->type);
    ASSERT_EQ(3, value_copy.element_field->id());
    ASSERT_FALSE(value_copy.element_field->required);
    ASSERT_TRUE(
      std::holds_alternative<primitive_type>(value_copy.element_field->type));
    ASSERT_TRUE(std::holds_alternative<int_type>(
      std::get<primitive_type>(value_copy.element_field->type)));
}
