/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf_parser.h"
#include "datalake/schema_parquet.h"
#include "datalake/values_parquet.h"
#include "gtest/gtest.h"
#include "iceberg/tests/value_generator.h"
#include "test_utils/randoms.h"
using namespace testing;
namespace {
iceberg::struct_type primitive_types() {
    iceberg::struct_type primitives;
    // all possible iceberg primitives

    primitives.fields.push_back(iceberg::nested_field::create(
      0,
      "boolean_type_field_0",
      iceberg::field_required::yes,
      iceberg::boolean_type{}));
    primitives.fields.push_back(iceberg::nested_field::create(
      1,
      "int_type_field_1",
      iceberg::field_required::yes,
      iceberg::int_type{}));
    primitives.fields.push_back(iceberg::nested_field::create(
      2,
      "long_type_field_2",
      iceberg::field_required::yes,
      iceberg::long_type{}));
    primitives.fields.push_back(iceberg::nested_field::create(
      3,
      "float_type_field_3",
      iceberg::field_required::yes,
      iceberg::float_type{}));
    primitives.fields.push_back(iceberg::nested_field::create(
      4,
      "double_type_field_4",
      iceberg::field_required::yes,
      iceberg::double_type{}));
    primitives.fields.push_back(iceberg::nested_field::create(
      5,
      "decimal_type_field_5",
      iceberg::field_required::yes,
      iceberg::decimal_type{.precision = 10, .scale = 2}));
    primitives.fields.push_back(iceberg::nested_field::create(
      6,
      "date_type_field_6",
      iceberg::field_required::yes,
      iceberg::date_type{}));
    primitives.fields.push_back(iceberg::nested_field::create(
      7,
      "time_type_field_7",
      iceberg::field_required::yes,
      iceberg::time_type{}));
    primitives.fields.push_back(iceberg::nested_field::create(
      8,
      "timestamp_type_field_8",
      iceberg::field_required::yes,
      iceberg::timestamp_type{}));
    primitives.fields.push_back(iceberg::nested_field::create(
      9,
      "timestamptz_type_field_9",
      iceberg::field_required::yes,
      iceberg::timestamptz_type{}));
    primitives.fields.push_back(iceberg::nested_field::create(
      10,
      "string_type_field_10",
      iceberg::field_required::yes,
      iceberg::string_type{}));
    primitives.fields.push_back(iceberg::nested_field::create(
      11,
      "uuid_type_field_11",
      iceberg::field_required::yes,
      iceberg::uuid_type{}));
    primitives.fields.push_back(iceberg::nested_field::create(
      12,
      "fixed_type_field_12",
      iceberg::field_required::yes,
      iceberg::fixed_type{.length = 12}));
    primitives.fields.push_back(iceberg::nested_field::create(
      13,
      "binary_type_field_13",
      iceberg::field_required::yes,
      iceberg::binary_type{}));

    return primitives;
}

/**
 * Map of type expectations for the primitive types from the schema above
 */
std::unordered_map<
  int,
  std::tuple<serde::parquet::physical_type, serde::parquet::logical_type>>
  primitive_type_expectations = {
    {0, {serde::parquet::bool_type{}, serde::parquet::logical_type{}}},
    {1,
     {serde::parquet::i32_type{},
      serde::parquet::int_type{.bit_width = 32, .is_signed = true}}},
    {2,
     {serde::parquet::i64_type{},
      serde::parquet::int_type{.bit_width = 64, .is_signed = true}}},
    {3, {serde::parquet::f32_type{}, serde::parquet::logical_type{}}},
    {4, {serde::parquet::f64_type{}, serde::parquet::logical_type{}}},
    {5,
     {serde::parquet::byte_array_type{.fixed_length = 16},
      serde::parquet::decimal_type{.scale = 2, .precision = 10}}},
    {6, {serde::parquet::i32_type{}, serde::parquet::date_type{}}},
    {7,
     {serde::parquet::i64_type{},
      serde::parquet::time_type{
        .is_adjusted_to_utc = false,
        .unit = serde::parquet::time_unit::micros}}},
    {8,
     {serde::parquet::i64_type{},
      serde::parquet::timestamp_type{
        .is_adjusted_to_utc = false,
        .unit = serde::parquet::time_unit::micros}}},
    {9,
     {serde::parquet::i64_type{},
      serde::parquet::timestamp_type{
        .is_adjusted_to_utc = true,
        .unit = serde::parquet::time_unit::micros}}},
    {10, {serde::parquet::byte_array_type{}, serde::parquet::string_type{}}},
    {11,
     {serde::parquet::byte_array_type{.fixed_length = 16},
      serde::parquet::uuid_type{}}},
    {12,
     {serde::parquet::byte_array_type{.fixed_length = 12},
      serde::parquet::logical_type{}}},
    {13, {serde::parquet::byte_array_type{}, serde::parquet::logical_type{}}}

};

AssertionResult validate_basics(
  const serde::parquet::schema_element& element,
  std::string_view expected_name,
  serde::parquet::field_repetition_type expected_repetition,
  int expected_field_id) {
    if (element.name() != expected_name) {
        return AssertionFailure() << fmt::format(
                 "Expected name {}, got {}", expected_name, element.name());
    }
    if (expected_repetition != element.repetition_type) {
        return AssertionFailure() << fmt::format(
                 "Expected repetition type {}, got {}, field name: {}",
                 static_cast<int>(expected_repetition),
                 static_cast<int>(element.repetition_type),
                 element.name());
    }
    if (expected_field_id != element.field_id) {
        return AssertionFailure() << fmt::format(
                 "Expected field id {}, got {}, field name: {}",
                 expected_field_id,
                 element.field_id.value_or(-1),
                 element.name());
    }

    return AssertionSuccess();
}

AssertionResult validate_primitive_element(
  const serde::parquet::schema_element& element,
  std::string_view expected_name,
  serde::parquet::field_repetition_type expected_repetition,
  int expected_field_id,
  serde::parquet::physical_type expected_type,
  serde::parquet::logical_type expected_logical) {
    auto res = validate_basics(
      element, expected_name, expected_repetition, expected_field_id);
    if (!res) {
        return res;
    }

    if (!element.children.empty()) {
        return AssertionFailure() << "Expected leaf node, got group";
    }
    if (expected_type != element.type) {
        return AssertionFailure() << fmt::format(
                 "Expected physical type {}, got {}, field name: {}",
                 expected_type.index(),
                 element.type.index(),
                 element.name());
    }
    if (expected_logical != element.logical_type) {
        return AssertionFailure() << fmt::format(
                 "Expected logical type {}, got {}, field name: {}",
                 expected_logical.index(),
                 element.logical_type.index(),
                 element.name());
    }

    return AssertionSuccess();
}

AssertionResult primitive_schema_matches(
  const serde::parquet::schema_element& element,
  const iceberg::struct_type& primitive_schema) {
    if (element.children.size() != 14) {
        return AssertionFailure()
               << "Expected 14 children, got " << element.children.size();
    }
    const auto match_primitive_field = [&](
                                         int id,
                                         serde::parquet::physical_type p_type,
                                         serde::parquet::logical_type l_type) {
        return validate_primitive_element(
          element.children[id],
          primitive_schema.fields[id]->name,
          primitive_schema.fields[id]->required
            ? serde::parquet::field_repetition_type::required
            : serde::parquet::field_repetition_type::optional,
          primitive_schema.fields[id]->id,
          p_type,
          l_type);
    };
    for (auto i = 0; i < 14; i++) {
        auto& [p_type, l_type] = primitive_type_expectations[i];
        auto res = match_primitive_field(i, p_type, l_type);
        if (!res) {
            return AssertionFailure() << res.failure_message();
        }
    }

    return AssertionSuccess();
}

iceberg::struct_type list_types() {
    iceberg::struct_type lists;

    lists.fields.push_back(iceberg::nested_field::create(
      0,
      "required_booleans",
      iceberg::field_required::yes,
      iceberg::list_type{
        .element_field = iceberg::nested_field::create(
          1,
          "element_field",
          iceberg::field_required::yes,
          iceberg::boolean_type{})}));
    lists.fields.push_back(iceberg::nested_field::create(
      1,
      "optional_longs",
      iceberg::field_required::yes,
      iceberg::list_type{
        .element_field = iceberg::nested_field::create(
          1,
          "element_field",
          iceberg::field_required::no,
          iceberg::long_type{})}));
    lists.fields.push_back(iceberg::nested_field::create(
      2,
      "optional_top_level_decimals",
      iceberg::field_required::no,
      iceberg::list_type{
        .element_field = iceberg::nested_field::create(
          1,
          "element_field",
          iceberg::field_required::yes,
          iceberg::decimal_type{.precision = 4, .scale = 12})}));

    lists.fields.push_back(iceberg::nested_field::create(
      3,
      "structs",
      iceberg::field_required::yes,
      iceberg::list_type{
        .element_field = iceberg::nested_field::create(
          1,
          "element_field",
          iceberg::field_required::yes,
          primitive_types())}));
    return lists;
}

AssertionResult validate_list(
  const serde::parquet::schema_element& element,
  const iceberg::nested_field_ptr& list_schema,
  serde::parquet::physical_type expected_type,
  serde::parquet::logical_type expected_logical) {
    if (element.children.size() != 1) {
        return AssertionFailure() << "List is expected to have exactly 1 child "
                                     "schema, current have: "
                                  << element.children.size();
    }
    if (element.name() != list_schema->name) {
        return AssertionFailure() << fmt::format(
                 "Expected list name {}, got {}",
                 list_schema->name,
                 element.name());
    }
    if (element.field_id != list_schema->id) {
        return AssertionFailure() << fmt::format(
                 "Expected field id {}, got {}",
                 list_schema->id,
                 element.field_id.value_or(-1));
    }
    if (list_schema->required) {
        if (
          element.repetition_type
          != serde::parquet::field_repetition_type::required) {
            return AssertionFailure() << fmt::format(
                     "Expected required repetition type, got {}",
                     static_cast<int>(element.repetition_type));
        }
    }

    if (!std::holds_alternative<serde::parquet::list_type>(
          element.logical_type)) {
        return AssertionFailure() << fmt::format(
                 "Expected top level list element to have list type, got {}",
                 element.logical_type.index());
    }
    auto& container = element.children[0];
    if (container.name() != "list") {
        return AssertionFailure() << fmt::format(
                 "Expected list container name to be 'list', got '{}'",
                 container.name());
    }
    if (
      container.repetition_type
      != serde::parquet::field_repetition_type::repeated) {
        return AssertionFailure() << fmt::format(
                 "Expected list container repetition type to be repeated, got "
                 "'{}'",
                 static_cast<int>(container.repetition_type));
    }
    if (!std::holds_alternative<std::monostate>(container.type)) {
        return AssertionFailure() << fmt::format(
                 "Expected list container type to be empty, got '{}'",
                 container.type.index());
    }
    if (!std::holds_alternative<std::monostate>(container.logical_type)) {
        return AssertionFailure() << fmt::format(
                 "Expected list container logical_type to be empty, got '{}'",
                 container.logical_type.index());
    }
    if (container.children.size() != 1) {
        return AssertionFailure() << fmt::format(
                 "Expected list container to have 1 child, got {}",
                 container.children.size());
    }
    auto& list_element = container.children[0];

    return validate_primitive_element(
      list_element,
      "element",
      std::get<iceberg::list_type>(list_schema->type).element_field->required
        ? serde::parquet::field_repetition_type::required
        : serde::parquet::field_repetition_type::optional,
      1,
      expected_type,
      expected_logical);
}
iceberg::struct_type map_types() {
    iceberg::struct_type maps;
    auto primitive = primitive_types();

    for (auto& f : primitive.fields) {
        auto value_type_idx = random_generators::get_int(
          primitive.fields.size() - 1);

        auto map = iceberg::map_type::create(
          f->id,
          iceberg::make_copy(f->type),
          value_type_idx,
          tests::random_bool() ? iceberg::field_required::yes
                               : iceberg::field_required::no,
          iceberg::make_copy(primitive.fields[value_type_idx]->type));
        maps.fields.push_back(iceberg::nested_field::create(
          f->id, f->name, f->required, std::move(map)));
    }
    auto key_type_idx = random_generators::get_int(primitive.fields.size() - 1);

    auto map = iceberg::map_type::create(
      key_type_idx,
      iceberg::make_copy(primitive.fields[key_type_idx]->type),
      0,
      iceberg::field_required::yes,
      primitive_types());

    maps.fields.push_back(iceberg::nested_field::create(
      15, "nested_struct_map", iceberg::field_required::yes, std::move(map)));

    return maps;
}
} // namespace

TEST(DatalakeParquetSchema, EmptySchemaConversionTest) {
    iceberg::struct_type schema;
    auto parquet_schema = datalake::schema_to_parquet(schema);
    ASSERT_EQ(parquet_schema.name(), "root");
    ASSERT_TRUE(parquet_schema.children.empty());
    ASSERT_EQ(
      parquet_schema.repetition_type,
      serde::parquet::field_repetition_type::required);
}

TEST(DatalakeParquetSchema, PrimitiveTypes) {
    auto schema = primitive_types();
    // make random fields optional
    for (auto& f : schema.fields) {
        if (tests::random_bool()) {
            f->required = iceberg::field_required::no;
        }
    }
    auto parquet_schema = datalake::schema_to_parquet(schema);
    ASSERT_EQ(parquet_schema.name(), "root");
    ASSERT_EQ(parquet_schema.children.size(), 14);
    ASSERT_TRUE(primitive_schema_matches(parquet_schema, schema));
}

TEST(DatalakeParquetSchema, Lists) {
    auto schema = list_types();
    auto parquet_schema = datalake::schema_to_parquet(schema);
    ASSERT_EQ(parquet_schema.name(), "root");
    ASSERT_EQ(parquet_schema.children.size(), 4);

    ASSERT_TRUE(validate_list(
      parquet_schema.children[0],
      schema.fields[0],
      serde::parquet::bool_type{},
      serde::parquet::logical_type{}));

    ASSERT_TRUE(validate_list(
      parquet_schema.children[1],
      schema.fields[1],
      serde::parquet::i64_type{},
      serde::parquet::int_type{.bit_width = 64, .is_signed = true}));

    ASSERT_TRUE(validate_list(
      parquet_schema.children[2],
      schema.fields[2],
      serde::parquet::byte_array_type{.fixed_length = 16},
      serde::parquet::decimal_type{
        .scale = 12,
        .precision = 4,
      }));

    auto& nested_struct_list = parquet_schema.children[3];
    ASSERT_TRUE(validate_basics(
      nested_struct_list,
      schema.fields[3]->name,
      serde::parquet::field_repetition_type::required,
      schema.fields[3]->id));
    ASSERT_EQ(nested_struct_list.children.size(), 1);
    ASSERT_EQ(nested_struct_list.children[0].name(), "list");
    ASSERT_EQ(
      nested_struct_list.children[0].repetition_type,
      serde::parquet::field_repetition_type::repeated);
    auto& element_schema = nested_struct_list.children[0].children[0];
    ASSERT_TRUE(primitive_schema_matches(element_schema, primitive_types()));
}

TEST(DatalakeParquetSchema, Maps) {
    for (int i = 0; i < 2000; ++i) {
        auto schema = map_types();
        auto parquet_schema = datalake::schema_to_parquet(schema);
        ASSERT_EQ(parquet_schema.name(), "root");
        ASSERT_EQ(parquet_schema.children.size(), 15);
        for (int i = 0; i < 15; i++) {
            auto& map = parquet_schema.children[i];
            ASSERT_TRUE(validate_basics(
              map,
              schema.fields[i]->name,
              schema.fields[i]->required
                ? serde::parquet::field_repetition_type::required
                : serde::parquet::field_repetition_type::optional,
              schema.fields[i]->id));
            // check map key_value wrapper
            ASSERT_EQ(map.children.size(), 1);
            ASSERT_EQ(map.children[0].name(), "key_value");
            ASSERT_EQ(
              map.children[0].repetition_type,
              serde::parquet::field_repetition_type::repeated);
            ASSERT_EQ(map.children[0].children.size(), 2);
            // validation of nested struct map is handled separately
            if (i >= 14) {
                continue;
            }
            auto& key_element = map.children[0].children[0];
            auto& value_element = map.children[0].children[1];
            auto& [key_physical_type, key_logical_type]
              = primitive_type_expectations[schema.fields[i]->id];
            // when preparing test data we set a value field id to match the
            // primitive type used as a value
            auto value_idx = std::get<iceberg::map_type>(schema.fields[i]->type)
                               .value_field->id;
            auto value_required = std::get<iceberg::map_type>(
                                    schema.fields[i]->type)
                                    .value_field->required;
            auto& [value_physical_type, value_logical_type]
              = primitive_type_expectations[value_idx];

            ASSERT_TRUE(validate_primitive_element(
              key_element,
              "key",
              serde::parquet::field_repetition_type::required,
              i,
              key_physical_type,
              key_logical_type));

            ASSERT_TRUE(validate_primitive_element(
              value_element,
              "value",
              value_required ? serde::parquet::field_repetition_type::required
                             : serde::parquet::field_repetition_type::optional,
              value_idx,
              value_physical_type,
              value_logical_type));
        }
    }
}

TEST(DatalakeParquetSchema, NestedStruct) {
    iceberg::struct_type schema;
    auto nested = primitive_types();
    schema.fields.push_back(iceberg::nested_field::create(
      0,
      "nested_struct",
      iceberg::field_required::yes,
      iceberg::make_copy(primitive_types())));

    auto parquet_schema = datalake::schema_to_parquet(schema);
    ASSERT_EQ(parquet_schema.name(), "root");
    ASSERT_EQ(parquet_schema.children.size(), 1);
    ASSERT_EQ(parquet_schema.children[0].name(), "nested_struct");
    ASSERT_TRUE(
      primitive_schema_matches(parquet_schema.children[0], primitive_types()));
}

/**
 * Helper to determine parquet physical type value for an iceberg value
 */
template<typename ParquetT>
struct type_wrapper {
    using type = ParquetT;
};

template<typename T>
static constexpr auto parquet_type(const T&) {
    if constexpr (std::is_same_v<T, iceberg::boolean_value>) {
        return type_wrapper<serde::parquet::boolean_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::int_value>) {
        return type_wrapper<serde::parquet::int32_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::long_value>) {
        return type_wrapper<serde::parquet::int64_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::float_value>) {
        return type_wrapper<serde::parquet::float32_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::double_value>) {
        return type_wrapper<serde::parquet::float64_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::decimal_value>) {
        return type_wrapper<serde::parquet::fixed_byte_array_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::date_value>) {
        return type_wrapper<serde::parquet::int32_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::time_value>) {
        return type_wrapper<serde::parquet::int64_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::timestamp_value>) {
        return type_wrapper<serde::parquet::int64_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::timestamptz_value>) {
        return type_wrapper<serde::parquet::int64_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::string_value>) {
        return type_wrapper<serde::parquet::byte_array_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::uuid_value>) {
        return type_wrapper<serde::parquet::fixed_byte_array_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::fixed_value>) {
        return type_wrapper<serde::parquet::fixed_byte_array_value>{};
    }
    if constexpr (std::is_same_v<T, iceberg::binary_value>) {
        return type_wrapper<serde::parquet::byte_array_value>{};
    }
    __builtin_unreachable();
}
const auto values_equal = [](const auto& expected, const auto& current) {
    return expected.val == current.val;
};

template<typename IcebergPrimitive, typename Predicate>
AssertionResult validate_parquet_primitive_value(
  const iceberg::value& expected_value,
  const serde::parquet::value& current_value,
  const Predicate& value_matches = values_equal) {
    using ParquetPrimitive = decltype(parquet_type(IcebergPrimitive{}))::type;

    auto& expected_primitive = std::get<IcebergPrimitive>(
      std::get<iceberg::primitive_value>(expected_value));
    if (!std::holds_alternative<ParquetPrimitive>(current_value)) {
        return AssertionFailure() << fmt::format(
                 "Error validating {} Expected parquet primitive value of type "
                 "{}, got {}",
                 typeid(IcebergPrimitive).name(),
                 typeid(ParquetPrimitive).name(),
                 current_value.index());
    }
    auto& parquet_primitive = std::get<ParquetPrimitive>(current_value);
    if (!value_matches(expected_primitive, parquet_primitive)) {
        return AssertionFailure() << fmt::format(
                 "Error validating {} expected primitive value {}, got {}",
                 typeid(IcebergPrimitive).name(),
                 expected_primitive.val,
                 parquet_primitive.val);
    }
    return AssertionSuccess();
}

template<typename IcebergPrimitive, typename Predicate>
AssertionResult validate_parquet_optional_primitive_value(
  const std::optional<iceberg::value>& expected_value,
  const serde::parquet::value& current_value,
  const Predicate& value_matches) {
    if (!expected_value.has_value()) {
        if (!std::holds_alternative<serde::parquet::null_value>(
              current_value)) {
            return AssertionFailure()
                   << "Empty optional must be mapped to parquet null";
        }
        return AssertionSuccess();
    }

    return validate_parquet_primitive_value<IcebergPrimitive>(
      expected_value.value(), current_value, value_matches);
}

bool decimals_equal(
  const iceberg::decimal_value& expected,
  const serde::parquet::fixed_byte_array_value& current) {
    iobuf_parser parser(current.val.copy());
    auto high_part = ss::be_to_cpu(parser.consume_type<int64_t>());
    auto low_part = ss::be_to_cpu(parser.consume_type<uint64_t>());
    return expected.val == absl::MakeInt128(high_part, low_part);
}

bool uuids_equal(
  const iceberg::uuid_value& expected,
  const serde::parquet::fixed_byte_array_value& current_bytes) {
    iobuf_parser parser(current_bytes.val.copy());
    auto bytes = parser.read_bytes(16);
    std::vector<uint8_t> uuid_bytes(bytes.begin(), bytes.end());
    uuid_t current_uuid(uuid_bytes);
    return expected.val == current_uuid;
}

struct primitive_validating_visitor {
    template<typename T>
    AssertionResult operator()(T expected) {
        return validate_parquet_primitive_value<decltype(expected)>(
          iceberg::primitive_value{std::move(expected)}, current, values_equal);
    }

    AssertionResult operator()(iceberg::decimal_value expected) {
        return validate_parquet_primitive_value<iceberg::decimal_value>(
          iceberg::primitive_value{std::move(expected)},
          current,
          decimals_equal);
    }
    AssertionResult operator()(iceberg::uuid_value expected) {
        return validate_parquet_primitive_value<iceberg::uuid_value>(
          iceberg::primitive_value{std::move(expected)}, current, uuids_equal);
    }

    const serde::parquet::value& current;
};

AssertionResult validate_optional_value(
  const std::optional<iceberg::value>& optional_value,
  const serde::parquet::value& current);
AssertionResult validate_value(
  const iceberg::value& optional_value, const serde::parquet::value& current);
AssertionResult validate_list_element(
  const std::optional<iceberg::value>& optional_value,
  const serde::parquet::value& current);
struct value_validating_visitor {
    AssertionResult operator()(const iceberg::primitive_value& expected) {
        return std::visit(
          primitive_validating_visitor{current}, iceberg::make_copy(expected));
    }

    AssertionResult
    operator()(const std::unique_ptr<iceberg::struct_value>& s_value) {
        if (!std::holds_alternative<serde::parquet::group_value>(current)) {
            return AssertionFailure()
                   << "Struct must be represented as group, got "
                   << current.index();
        }
        auto& group_value = std::get<serde::parquet::group_value>(current);
        auto idx = 0;
        for (auto& field : s_value->fields) {
            auto res = validate_optional_value(field, group_value[idx++].field);
            if (!res) {
                return res;
            }
        }
        return AssertionSuccess();
    }

    AssertionResult
    operator()(const std::unique_ptr<iceberg::list_value>& expected_list) {
        if (!std::holds_alternative<serde::parquet::group_value>(current)) {
            return AssertionFailure()
                   << "List must be wrapped with a group value, got "
                   << current.index();
        }
        auto& wrapper = std::get<serde::parquet::group_value>(current);
        if (!std::holds_alternative<serde::parquet::repeated_value>(
              wrapper[0].field)) {
            return AssertionFailure()
                   << "List must be represented as repeated_value, got "
                   << current.index();
        }

        auto& repeated = std::get<serde::parquet::repeated_value>(
          wrapper[0].field);

        if (repeated.size() != expected_list->elements.size()) {
            return AssertionFailure() << fmt::format(
                     "Expected {} elements, got {}",
                     expected_list->elements.size(),
                     repeated.size());
        }
        for (size_t i = 0; i < repeated.size(); i++) {
            auto res = validate_list_element(
              expected_list->elements[i], repeated[i].element);
            if (!res) {
                return AssertionFailure() << res.failure_message();
            }
        }
        return AssertionSuccess();
    }

    AssertionResult
    operator()(const std::unique_ptr<iceberg::map_value>& expected_map) {
        if (!std::holds_alternative<serde::parquet::group_value>(current)) {
            return AssertionFailure()
                   << "Map must be wrapped with a group value, got "
                   << current.index();
        }
        auto& wrapper = std::get<serde::parquet::group_value>(current);
        if (!std::holds_alternative<serde::parquet::repeated_value>(
              wrapper[0].field)) {
            return AssertionFailure()
                   << "Map must be represented as repeated_value, got "
                   << current.index();
        }
        auto& repeated = std::get<serde::parquet::repeated_value>(
          wrapper[0].field);

        if (repeated.size() != expected_map->kvs.size()) {
            return AssertionFailure() << fmt::format(
                     "Expected {} elements, got {}",
                     expected_map->kvs.size(),
                     repeated.size());
        }
        for (size_t i = 0; i < repeated.size(); i++) {
            if (!std::holds_alternative<serde::parquet::group_value>(
                  repeated[i].element)) {
                return AssertionFailure()
                       << "Map element must be represented as group, got "
                       << current.index();
            }
            auto& kv_group = std::get<serde::parquet::group_value>(
              repeated[i].element);
            auto res = validate_value(
              expected_map->kvs[i].key, kv_group[0].field);
            if (!res) {
                return AssertionFailure()
                       << "Map key validation error" << res.failure_message();
            }
            res = validate_optional_value(
              expected_map->kvs[i].val, kv_group[1].field);
            if (!res) {
                return AssertionFailure()
                       << "Map value validation error" << res.failure_message();
            }
        }
        return AssertionSuccess();
    }

    const serde::parquet::value& current;
};

AssertionResult validate_list_element(
  const std::optional<iceberg::value>& optional_value,
  const serde::parquet::value& current) {
    if (!std::holds_alternative<serde::parquet::group_value>(current)) {
        return AssertionFailure()
               << "List element must be wrapped with a group value, got "
               << current.index();
    }

    auto& wrapper_group = std::get<serde::parquet::group_value>(current);
    if (wrapper_group.size() != 1) {
        return AssertionFailure()
               << "List element must have exactly 1 child, got "
               << wrapper_group.size();
    }

    return validate_optional_value(optional_value, wrapper_group[0].field);
}

AssertionResult validate_value(
  const iceberg::value& value, const serde::parquet::value& current) {
    return std::visit(
      value_validating_visitor{current}, iceberg::make_copy(value));
}

AssertionResult validate_optional_value(
  const std::optional<iceberg::value>& optional_value,
  const serde::parquet::value& current) {
    if (!optional_value.has_value()) {
        if (!std::holds_alternative<serde::parquet::null_value>(current)) {
            return AssertionFailure()
                   << "Empty field must be represented as parquet null, got "
                   << current.index();
        }
        return AssertionSuccess();
    }
    return validate_value(optional_value.value(), current);
}

auto prepare_test_data(iceberg::field_type schema) {
    auto schema_field = iceberg::field_type{std::move(schema)};
    auto test_value = iceberg::tests::make_value(
      iceberg::tests::value_spec{}, iceberg::make_copy(schema_field));
    return std::make_tuple(
      datalake::to_parquet_value(iceberg::make_copy(test_value)).get(),
      std::move(test_value));
}

TEST(DataParquetValues, TestPrimitiveValues) {
    auto schema = primitive_types();
    for (auto& f : schema.fields) {
        if (tests::random_bool()) {
            f->required = iceberg::field_required::no;
        }
    }
    auto schema_field = iceberg::field_type{std::move(schema)};
    auto [result, test_value] = prepare_test_data(
      iceberg::make_copy(schema_field));
    ASSERT_FALSE(result.has_error());

    auto& parquet_value = result.value();

    ASSERT_TRUE(std::visit(
      value_validating_visitor{parquet_value}, std::move(test_value)));
}

TEST(DataParquetValues, TestLists) {
    auto schema = list_types();
    auto schema_field = iceberg::field_type{std::move(schema)};
    auto [result, test_value] = prepare_test_data(
      iceberg::make_copy(schema_field));
    ASSERT_FALSE(result.has_error());

    ASSERT_TRUE(validate_value(test_value, result.value()));
}

TEST(DataParquetValues, TestMaps) {
    auto schema = map_types();
    auto schema_field = iceberg::field_type{std::move(schema)};
    auto [result, test_value] = prepare_test_data(
      iceberg::make_copy(schema_field));
    ASSERT_FALSE(result.has_error());
    ASSERT_TRUE(validate_value(test_value, result.value()));
}

TEST(DataParquetValues, TestNestedStruct) {
    iceberg::struct_type schema;
    schema.fields.push_back(iceberg::nested_field::create(
      0,
      "nested_struct",
      iceberg::field_required(tests::random_bool()),
      primitive_types()));
    schema.fields.push_back(iceberg::nested_field::create(
      1,
      "nested_lists",
      iceberg::field_required(tests::random_bool()),
      list_types()));
    schema.fields.push_back(iceberg::nested_field::create(
      2,
      "nested_maps",
      iceberg::field_required(tests::random_bool()),
      map_types()));
    auto schema_field = iceberg::field_type{std::move(schema)};
    auto [result, test_value] = prepare_test_data(
      iceberg::make_copy(schema_field));
    ASSERT_FALSE(result.has_error());
    ASSERT_TRUE(validate_value(test_value, result.value()));
}
