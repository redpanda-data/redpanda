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
#include "datalake/schema_avro.h"
#include "datalake/values_avro.h"
#include "gtest/gtest.h"
#include "iceberg/avro_decimal.h"
#include "iceberg/datatypes.h"
#include "serde/avro/tests/data_generator.h"

#include <seastar/core/sstring.hh>

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/ValidSchema.hh>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace testing;
static constexpr auto tl_string = R"J({"type": "string"})J";
static constexpr auto tl_int = R"J({"type": "int"})J";
static constexpr auto tl_long = R"J({"type": "long"})J";
static constexpr auto tl_double = R"J({"type": "double"})J";
static constexpr auto tl_array = R"J({ "type" : "array", "items" : "int" })J";

static constexpr auto tl_map = R"J({"type": "map","values": {"type":"int"}})J";
static constexpr auto tl_union = R"J([ "int" , "long" , "float" ])J";
avro::NodePtr load_json_schema(std::string_view sch) {
    return avro::compileJsonSchemaFromString(std::string(sch)).root();
}

struct TypeEqMatcher : MatcherInterface<const iceberg::field_type&> {
    explicit TypeEqMatcher(const iceberg::field_type& ft)
      : ft(ft) {}
    bool MatchAndExplain(
      const iceberg::field_type& x, MatchResultListener*) const final {
        return ft == x;
    }
    void DescribeTo(::std::ostream* os) const final {
        *os << " is equal to " << ft;
    }

    const iceberg::field_type& ft;
};

auto TypeEq(const iceberg::field_type& ft) {
    return Matcher<const iceberg::field_type&>(new TypeEqMatcher(ft));
}

auto match_field(const ss::sstring& name, const iceberg::field_type& ft) {
    return Pointee(AllOf(
      Field(&iceberg::nested_field::id, Eq(0)),
      Field(&iceberg::nested_field::name, Eq(name)),
      Field(&iceberg::nested_field::type, TypeEq(ft))));
}

TEST(AvroSchema, TestNonRecordSchema) {
    std::vector<std::tuple<ss::sstring, std::string_view, iceberg::field_type>>
      tl_type_expectations;

    tl_type_expectations.emplace_back(
      "root", tl_string, iceberg::string_type{});
    tl_type_expectations.emplace_back("root", tl_int, iceberg::int_type{});
    tl_type_expectations.emplace_back(
      "root", tl_double, iceberg::double_type{});
    tl_type_expectations.emplace_back("root", tl_long, iceberg::long_type{});
    tl_type_expectations.emplace_back(
      "root",
      tl_array,
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::int_type{}));
    tl_type_expectations.emplace_back(
      "root",
      tl_map,
      iceberg::map_type::create(
        0,
        iceberg::string_type{},
        0,
        iceberg::field_required::yes,
        iceberg::int_type{}));

    iceberg::struct_type union_st;
    union_st.fields.push_back(iceberg::nested_field::create(
      0, "union_opt_0", iceberg::field_required::no, iceberg::int_type{}));
    union_st.fields.push_back(iceberg::nested_field::create(
      0, "union_opt_1", iceberg::field_required::no, iceberg::long_type{}));
    union_st.fields.push_back(iceberg::nested_field::create(
      0, "union_opt_2", iceberg::field_required::no, iceberg::float_type{}));

    tl_type_expectations.emplace_back("root", tl_union, std::move(union_st));

    for (auto& [name, schema, type] : tl_type_expectations) {
        auto root = load_json_schema(schema);
        auto iceberg_struct_res = datalake::type_to_iceberg(root);

        ASSERT_TRUE(iceberg_struct_res.has_value());

        ASSERT_THAT(
          iceberg_struct_res.value().fields,
          testing::ElementsAre(match_field(name, type)));
    }
}

constexpr auto big_record = R"J(
{
    "type": "record",
    "doc": "Top level Doc.",
    "name": "RootRecord",
    "fields": [
        {
            "name": "mylong",
            "doc": "mylong field doc.",
            "type": "long"
        },
        {
            "name": "nestedrecord",
            "type": {
                "type": "record",
                "name": "Nested",
                "fields": [
                    {
                        "name": "inval1",
                        "type": "double"
                    },
                    {
                        "name": "inval2",
                        "type": "string"
                    },
                    {
                        "name": "inval3",
                        "type": "int"
                    }
                ]
            }
        },
        {
            "name": "mymap",
            "type": {
                "type": "map",
                "values": "int"
            }
        },
        {
            "name": "recordmap",
            "type": {
                "type": "map",
                "values": "Nested"
            }
        },
        {
            "name": "myarray",
            "type": {
                "type": "array",
                "items": "double"
            }
        },
        {
            "name": "myenum",
            "type": {
                "type": "enum",
                "name": "ExampleEnum",
                "symbols": [
                    "zero",
                    "one",
                    "two",
                    "three"
                ]
            }
        },
        {
            "name": "myunion",
            "type": [
                "null",
                {
                    "type": "map",
                    "values": "int"
                },
                "float"
            ]
        },
        {
            "name": "anotherunion",
            "type": [
                "bytes",
                "null"
            ]
        },
        {
            "name": "mybool",
            "type": "boolean"
        },
        {
            "name": "anothernested",
            "type": "Nested"
        },
        {
            "name": "myfixed",
            "type": {
                "type": "fixed",
                "size": 16,
                "name": "md5"
            }
        },
        {
            "name": "anotherint",
            "type": "int"
        },
        {
            "name": "bytes",
            "type": "bytes"
        },
		    {
            "name": "null",
            "type": "null"
        }
    ]
}
)J";

iceberg::struct_type nested_record() {
    iceberg::struct_type st;
    st.fields.push_back(iceberg::nested_field::create(
      0, "inval1", iceberg::field_required::yes, iceberg::double_type{}));
    st.fields.push_back(iceberg::nested_field::create(
      0, "inval2", iceberg::field_required::yes, iceberg::string_type{}));
    st.fields.push_back(iceberg::nested_field::create(
      0, "inval3", iceberg::field_required::yes, iceberg::int_type{}));
    return st;
}

AssertionResult field_matches(
  const iceberg::nested_field_ptr& field,
  const ss::sstring& name,
  const iceberg::field_type& ft) {
    if (field->id != 0 || field->name != name || field->type != ft) {
        return AssertionFailure() << fmt::format(
                 "\nexpected: (id: 0, name: {}, type: {})\n"
                 "actual  : (id: {}, name: {}, type: {})",
                 name,
                 ft,
                 field->id,
                 field->name,
                 field->type);
    }
    return AssertionSuccess();
}

TEST(AvroSchema, TestRecordType) {
    auto root = load_json_schema(big_record);
    auto iceberg_struct_res = datalake::type_to_iceberg(root);

    ASSERT_TRUE(iceberg_struct_res.has_value());
    auto struct_t = std::move(iceberg_struct_res.value());
    ASSERT_EQ(struct_t.fields.size(), 13);

    ASSERT_TRUE(
      field_matches(struct_t.fields[0], "mylong", iceberg::long_type{}));

    ASSERT_TRUE(
      field_matches(struct_t.fields[1], "nestedrecord", nested_record()));

    ASSERT_TRUE(field_matches(
      struct_t.fields[2],

      "mymap",
      iceberg::map_type::create(
        0,
        iceberg::string_type{},
        0,
        iceberg::field_required::yes,
        iceberg::int_type{})));

    ASSERT_TRUE(field_matches(
      struct_t.fields[3],
      "recordmap",
      iceberg::map_type::create(
        0,
        iceberg::string_type{},
        0,
        iceberg::field_required::yes,
        nested_record())));

    ASSERT_TRUE(field_matches(
      struct_t.fields[4],
      "myarray",
      iceberg::list_type::create(
        0, iceberg::field_required::yes, iceberg::double_type{})));

    ASSERT_TRUE(
      field_matches(struct_t.fields[5], "myenum", iceberg::long_type{}));

    iceberg::struct_type union_struct;
    // starts from union_1 as the union_0 is null type which is not represented
    // here
    union_struct.fields.push_back(iceberg::nested_field::create(
      0,
      "union_opt_1",
      iceberg::field_required::no,
      iceberg::map_type::create(
        0,
        iceberg::string_type{},
        0,
        iceberg::field_required::yes,
        iceberg::int_type{})));
    union_struct.fields.push_back(iceberg::nested_field::create(
      0, "union_opt_2", iceberg::field_required::no, iceberg::float_type{}));

    ASSERT_TRUE(
      field_matches(struct_t.fields[6], "myunion", std::move(union_struct)));
    iceberg::struct_type another_union_struct;

    another_union_struct.fields.push_back(iceberg::nested_field::create(
      0, "union_opt_0", iceberg::field_required::no, iceberg::binary_type{}));

    ASSERT_TRUE(field_matches(
      struct_t.fields[7], "anotherunion", std::move(another_union_struct)));

    ASSERT_TRUE(
      field_matches(struct_t.fields[8], "mybool", iceberg::boolean_type{}));
    ASSERT_TRUE(
      field_matches(struct_t.fields[9], "anothernested", nested_record()));
    ASSERT_TRUE(field_matches(
      struct_t.fields[10], "myfixed", iceberg::fixed_type{.length = 16}));
    ASSERT_TRUE(
      field_matches(struct_t.fields[11], "anotherint", iceberg::int_type{}));
    ASSERT_TRUE(
      field_matches(struct_t.fields[12], "bytes", iceberg::binary_type{}));
    // There is a NULL type in schema, but it is skiped when translating to
    // iceberg
}

constexpr auto tree_node = R"J(
{
  "name": "Node",
  "type": "record",
  "fields": [
    {
      "name": "payload",
      "type": "int",
      "default": 0
    },
    {
      "name": "edges",
      "type": {
        "type": "map",
        "values": "Node"
      }
    }
  ]
}
)J";

TEST(AvroSchema, TestRecursiveType) {
    auto root = load_json_schema(tree_node);
    auto iceberg_struct_res = datalake::type_to_iceberg(root);

    ASSERT_TRUE(iceberg_struct_res.has_error());
}

constexpr auto logical_types = R"J(
{
    "name": "LogicalTypesRecord",
    "type": "record",
    "fields": [
        {
            "name": "uuid_string",
            "type": {
                "type": "string",
                "logicalType": "uuid"
            }
        },
        {
            "name": "date_int",
            "type": {
                "type": "int",
                "logicalType": "date"
            }
        },
        {
            "name": "bytes_decimal",
            "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 38,
                "scale": 9
            }
        },
        {
            "name": "fixed_decimal",
            "type": {
                "name": "fixed_16",
                "size": 16,
                "type": "fixed",
                "logicalType": "decimal",
                "precision": 20,
                "scale": 9
            }
        },
        {
            "name": "int_time_ms",
            "type": {
                "type": "int",
                "logicalType": "time-millis"
            }
        },
        {
            "name": "long_time_micro",
            "type": {
                "type": "long",
                "logicalType": "time-micros"
            }
        },
        {
            "name": "long_ts_ms",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "long_ts_micro",
            "type": {
                "type": "long",
                "logicalType": "timestamp-micros"
            }
        },
        {
            "name": "duration_fixed",
            "type": {
                "type": "fixed",
                "name": "fixed_12",
                "size": 12,
                "logicalType": "duration"
            }
        }
    ]
}
)J";

TEST(AvroSchema, TestLogicalTypes) {
    auto root = load_json_schema(logical_types);
    auto iceberg_struct_res = datalake::type_to_iceberg(root);

    ASSERT_FALSE(iceberg_struct_res.has_error());
    auto struct_t = std::move(iceberg_struct_res.value());
    ASSERT_EQ(struct_t.fields.size(), 9);
    EXPECT_TRUE(
      field_matches(struct_t.fields[0], "uuid_string", iceberg::uuid_type{}));
    EXPECT_TRUE(
      field_matches(struct_t.fields[1], "date_int", iceberg::date_type{}));
    EXPECT_TRUE(field_matches(
      struct_t.fields[2],
      "bytes_decimal",
      iceberg::decimal_type{.precision = 38, .scale = 9}));
    EXPECT_TRUE(field_matches(
      struct_t.fields[3],
      "fixed_decimal",
      iceberg::decimal_type{.precision = 20, .scale = 9}));
    EXPECT_TRUE(
      field_matches(struct_t.fields[4], "int_time_ms", iceberg::time_type{}));
    EXPECT_TRUE(field_matches(
      struct_t.fields[5], "long_time_micro", iceberg::time_type{}));
    EXPECT_TRUE(field_matches(
      struct_t.fields[6], "long_ts_ms", iceberg::timestamp_type{}));
    EXPECT_TRUE(field_matches(
      struct_t.fields[7], "long_ts_micro", iceberg::timestamp_type{}));
    // Duration logical type is not intrepreted in as a separate iceberg type
    EXPECT_TRUE(field_matches(
      struct_t.fields[8], "duration_fixed", iceberg::fixed_type{12}));
}

iobuf serialize_with_avro(
  const ::avro::GenericDatum& datum, const ::avro::ValidSchema& schema) {
    std::unique_ptr<::avro::OutputStream> out = ::avro::memoryOutputStream();
    auto e = avro::binaryEncoder();
    e->init(*out);
    avro::GenericWriter writer(schema, e);
    writer.write(datum);
    e->flush();

    auto data = ::avro::snapshot(*out);
    iobuf buffer;
    buffer.append(data->data(), e->byteCount());
    return buffer;
}

/**
 * Helper function to prepare test data for avro serialization and
 * deserialization. This function takes avro schema as input, generates random
 * Avro GenericDatum serializes it using avro library and finally deserialize it
 * with datalake::deserialize_avro function. It returns a tuple containing
 * value, iceberg schema and test data in form of generic datum.
 */
std::tuple<
  datalake::value_outcome,
  datalake::conversion_outcome<iceberg::field_type>,
  avro::GenericDatum>
prepare_avro_test(std::string_view schema) {
    // Load schema
    auto valid_schema = avro::compileJsonSchemaFromString(std::string(schema));
    // Convert to iceberg schema
    auto iceberg_struct_res = datalake::type_to_iceberg(valid_schema.root());
    // Generate random generic datum
    avro_generator gen({});
    avro::GenericDatum datum = gen.generate_datum(valid_schema.root());

    // Serialize using avro library
    auto buffer = serialize_with_avro(datum, valid_schema);

    auto values = datalake::deserialize_avro(buffer.copy(), valid_schema).get();
    if (iceberg_struct_res.has_error()) {
        return {
          std::move(values),
          std::move(iceberg_struct_res.error()),
          std::move(datum)};
    }
    return {
      std::move(values),
      std::move(iceberg_struct_res.value()),
      std::move(datum)};
}

/**
 * Helpers to validate if iceberg schema matches iceberg value.
 */
struct schema_primitive_validating_visitor {
    AssertionResult operator()(const iceberg::boolean_type&) {
        if (std::holds_alternative<iceberg::boolean_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected boolean value, got: " << value;
    }
    AssertionResult operator()(const iceberg::int_type&) {
        if (std::holds_alternative<iceberg::int_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected int value, got " << value;
    }
    AssertionResult operator()(const iceberg::long_type&) {
        if (std::holds_alternative<iceberg::long_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected long value, got: " << value;
    }
    AssertionResult operator()(const iceberg::float_type&) {
        if (std::holds_alternative<iceberg::float_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected float value, got: " << value;
    }
    AssertionResult operator()(const iceberg::double_type&) {
        if (std::holds_alternative<iceberg::double_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected double value, got: " << value;
    }
    AssertionResult operator()(const iceberg::decimal_type&) {
        if (std::holds_alternative<iceberg::decimal_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected decimal value, got: " << value;
    }
    AssertionResult operator()(const iceberg::date_type&) {
        if (std::holds_alternative<iceberg::date_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected date value, got: " << value;
    }
    AssertionResult operator()(const iceberg::time_type&) {
        if (std::holds_alternative<iceberg::time_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected time value, got: " << value;
    }
    AssertionResult operator()(const iceberg::timestamp_type&) {
        if (std::holds_alternative<iceberg::timestamp_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected timestamp value, got: " << value;
    }
    AssertionResult operator()(const iceberg::timestamptz_type&) {
        if (std::holds_alternative<iceberg::timestamptz_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure()
               << "Expected timestamptz value, got: " << value;
    }
    AssertionResult operator()(const iceberg::string_type&) {
        if (std::holds_alternative<iceberg::string_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected string value, got: " << value;
    }
    AssertionResult operator()(const iceberg::uuid_type&) {
        if (std::holds_alternative<iceberg::uuid_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected uuid value, got: " << value;
    }
    AssertionResult operator()(const iceberg::fixed_type&) {
        if (std::holds_alternative<iceberg::fixed_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected fixed value, got: " << value;
    }
    AssertionResult operator()(const iceberg::binary_type&) {
        if (std::holds_alternative<iceberg::binary_value>(value)) {
            return AssertionSuccess();
        }
        return AssertionFailure() << "Expected binary value, got: " << value;
    }

    const iceberg::primitive_value& value;
};

struct schema_validating_visitor {
    AssertionResult operator()(const iceberg::primitive_type& t) {
        return std::visit(
          schema_primitive_validating_visitor{
            std::get<iceberg::primitive_value>(value)},
          t);
    }

    AssertionResult operator()(const iceberg::struct_type& st) {
        if (!std::holds_alternative<std::unique_ptr<iceberg::struct_value>>(
              value)) {
            return AssertionFailure()
                   << "Expected struct value, got: " << value;
        }
        auto idx = 0;
        const auto& sv = std::get<std::unique_ptr<iceberg::struct_value>>(
          value);
        for (const auto& field : st.fields) {
            if (!sv->fields[idx].has_value()) {
                continue;
            }
            auto res = std::visit(
              schema_validating_visitor{sv->fields[idx].value()}, field->type);
            if (!res) {
                return res;
            }
            idx++;
        }
        return AssertionSuccess();
    }
    AssertionResult operator()(const iceberg::list_type& lt) {
        if (!std::holds_alternative<std::unique_ptr<iceberg::list_value>>(
              value)) {
            return AssertionFailure() << "Expected list value, got: " << value;
        }
        for (const auto& v :
             std::get<std::unique_ptr<iceberg::list_value>>(value)->elements) {
            if (!v.has_value()) {
                continue;
            }
            if (!std::visit(
                  schema_validating_visitor{v.value()},
                  lt.element_field->type)) {
                return AssertionFailure() << "Element does not match schema";
            }
        }
        return AssertionSuccess();
    }
    AssertionResult operator()(const iceberg::map_type& mt) {
        if (!std::holds_alternative<std::unique_ptr<iceberg::map_value>>(
              value)) {
            return AssertionFailure() << "Expected map value, got: " << value;
        }
        for (auto& kv :
             std::get<std::unique_ptr<iceberg::map_value>>(value)->kvs) {
            if (!std::visit(
                  schema_validating_visitor{kv.key}, mt.key_field->type)) {
                return AssertionFailure() << "Key does not match schema";
            }
            if (!kv.val.has_value()) {
                continue;
            }
            if (!std::visit(
                  schema_validating_visitor{kv.val.value()},
                  mt.value_field->type)) {
                return AssertionFailure() << "Value does not match schema";
            }
        }
        return AssertionSuccess();
    }

    const iceberg::value& value;
};

/**
 * Helpers to validate if an avro value matches iceberg value
 */

std::vector<uint8_t> iobuf_to_vector(const iobuf& buf) {
    std::vector<uint8_t> ret(buf.size_bytes());
    iobuf::iterator_consumer it(buf.cbegin(), buf.cend());
    it.consume_to(buf.size_bytes(), ret.data());
    return ret;
}

template<typename IcebergValueT>
AssertionResult check_if_holds_primitive(const iceberg::value& current) {
    // make sure iceberg holds a requested primitive value
    if (!std::holds_alternative<iceberg::primitive_value>(current)) {
        return AssertionFailure()
               << fmt::format("Expected value {} to be a primitive", current);
    }

    auto& current_primitive = std::get<iceberg::primitive_value>(current);
    if (!std::holds_alternative<IcebergValueT>(current_primitive)) {
        return AssertionFailure() << fmt::format(
                 "Expected value {} to be of type {}",
                 current,
                 IcebergValueT{});
    }
    return AssertionSuccess();
}

template<typename IcebergT, typename Matcher>
AssertionResult primitive_equal(
  const avro::GenericDatum& expected,
  const iceberg::value& current,
  Matcher matcher_func) {
    // make sure iceberg holds a requested primitive value
    if (!std::holds_alternative<iceberg::primitive_value>(current)) {
        return AssertionFailure()
               << fmt::format("Expected value {} to be a primitive", current);
    }

    auto& current_primitive = std::get<iceberg::primitive_value>(current);
    if (!std::holds_alternative<IcebergT>(current_primitive)) {
        return AssertionFailure() << fmt::format(
                 "Expected value {} to be of type {}", current, IcebergT{});
    }

    const auto& current_value = std::get<IcebergT>(current_primitive);

    return matcher_func(current_value, expected);
}

AssertionResult value_matches(
  const avro::GenericDatum& datum, const iceberg::value& value, bool is_union);

AssertionResult value_matches(
  const avro::GenericDatum& datum,
  const std::optional<iceberg::value>& optional_val,
  bool is_union) {
    if (optional_val) {
        return value_matches(datum, optional_val.value(), is_union);
    }
    if (datum.type() != avro::AVRO_NULL) {
        return AssertionFailure() << "Empty optional must be represented as "
                                     "null in AVRO";
    }
    return AssertionSuccess();
}

// Helper macro to minimize boilerplate code
#define ASSERT_VALUES_EQUAL(current, expected)                                 \
    if (current != expected) {                                                 \
        return AssertionFailure() << fmt::format(                              \
                 "Expected value {} to be equal to {}", current, expected);    \
    }                                                                          \
    return AssertionSuccess();

// convert iceberg decimal to vector of bytes, big endian
std::vector<uint8_t> decimal_to_vector(absl::int128 decimal) {
    auto array = iceberg::encode_avro_decimal(decimal);
    return {array.begin(), array.end()};
}

/**
 * Check if avro generic datum matches iceberg value
 */
AssertionResult value_matches(
  const avro::GenericDatum& datum, const iceberg::value& value, bool is_union) {
    if (is_union) {
        if (!std::holds_alternative<std::unique_ptr<iceberg::struct_value>>(
              value)) {
            return AssertionFailure() << fmt::format(
                     "Expected value {} to be an union struct", value);
        }

        auto& struct_v = std::get<std::unique_ptr<iceberg::struct_value>>(
          value);
        if (datum.type() == avro::AVRO_NULL) {
            auto all_empty = std::ranges::all_of(
              struct_v->fields,
              [](const auto& field) { return !field.has_value(); });
            if (!all_empty) {
                return AssertionFailure()
                       << "Expected all fields to be empty in case of an union "
                          "branch that holds a NULL type";
            }
            return AssertionSuccess();
        }
        // find a branch that is not empty, we can not relay on the
        // GenericDatum::selectedBranch here as the indexing is shifted in
        // iceberg scheme as it does not include NULL type
        const auto& field = std::ranges::find_if(
          struct_v->fields, [](const std::optional<iceberg::value>& field) {
              return field.has_value();
          });
        return value_matches(datum, *field, false);
    }
    switch (datum.type()) {
    case avro::AVRO_STRING:
        if (datum.logicalType().type() == avro::LogicalType::UUID) {
            return primitive_equal<iceberg::uuid_value>(
              datum,
              value,
              [](
                const iceberg::uuid_value& current,
                const avro::GenericDatum& expected) {
                  ASSERT_VALUES_EQUAL(
                    fmt::to_string(current.val), expected.value<std::string>());
              });
        }
        return primitive_equal<iceberg::string_value>(
          datum,
          value,
          [](
            const iceberg::string_value& current,
            const avro::GenericDatum& expected) {
              iobuf_parser parser(current.val.copy());
              auto string_val = parser.read_string(current.val.size_bytes());
              ASSERT_VALUES_EQUAL(string_val, expected.value<std::string>());
          });
    case avro::AVRO_BYTES: {
        if (datum.logicalType().type() == avro::LogicalType::DECIMAL) {
            return primitive_equal<iceberg::decimal_value>(
              datum,
              value,
              [](
                const iceberg::decimal_value& decimal,
                const avro::GenericDatum& expected) {
                  auto decimal_bytes = decimal_to_vector(decimal.val);

                  ASSERT_VALUES_EQUAL(
                    decimal_bytes, expected.value<std::vector<uint8_t>>())
              });
        }
        return primitive_equal<iceberg::binary_value>(
          datum,
          value,
          [](
            const iceberg::binary_value& current,
            const avro::GenericDatum& expected) {
              ASSERT_VALUES_EQUAL(
                iobuf_to_vector(current.val),
                expected.value<std::vector<uint8_t>>());
          });
    }
    case avro::AVRO_INT:
        if (datum.logicalType().type() == avro::LogicalType::DATE) {
            return primitive_equal<iceberg::date_value>(
              datum,
              value,
              [](
                const iceberg::date_value& current,
                const avro::GenericDatum& expected) {
                  ASSERT_VALUES_EQUAL(current.val, expected.value<int32_t>());
              });
        }
        if (datum.logicalType().type() == avro::LogicalType::TIME_MILLIS) {
            return primitive_equal<iceberg::time_value>(
              datum,
              value,
              [](
                const iceberg::time_value& current,
                const avro::GenericDatum& expected) {
                  ASSERT_VALUES_EQUAL(
                    current.val / 1000, expected.value<int32_t>());
              });
        }
        return primitive_equal<iceberg::int_value>(
          datum,
          value,
          [](
            const iceberg::int_value& current,
            const avro::GenericDatum& expected) {
              ASSERT_VALUES_EQUAL(current.val, expected.value<int32_t>());
          });
    case avro::AVRO_LONG:
        if (datum.logicalType().type() == avro::LogicalType::TIME_MICROS) {
            return primitive_equal<iceberg::time_value>(
              datum,
              value,
              [](
                const iceberg::time_value& current,
                const avro::GenericDatum& expected) {
                  ASSERT_VALUES_EQUAL(current.val, expected.value<int64_t>());
              });
        }
        if (datum.logicalType().type() == avro::LogicalType::TIMESTAMP_MILLIS) {
            return primitive_equal<iceberg::timestamp_value>(
              datum,
              value,
              [](
                const iceberg::timestamp_value& current,
                const avro::GenericDatum& expected) {
                  ASSERT_VALUES_EQUAL(
                    current.val / 1000, expected.value<int64_t>());
              });
        }
        if (datum.logicalType().type() == avro::LogicalType::TIMESTAMP_MICROS) {
            return primitive_equal<iceberg::timestamp_value>(
              datum,
              value,
              [](
                const iceberg::timestamp_value& current,
                const avro::GenericDatum& expected) {
                  ASSERT_VALUES_EQUAL(current.val, expected.value<int64_t>());
              });
        }
        return primitive_equal<iceberg::long_value>(
          datum,
          value,
          [](
            const iceberg::long_value& current,
            const avro::GenericDatum& expected) {
              ASSERT_VALUES_EQUAL(current.val, expected.value<int64_t>());
          });
    case avro::AVRO_FLOAT:
        return primitive_equal<iceberg::float_value>(
          datum,
          value,
          [](
            const iceberg::float_value& current,
            const avro::GenericDatum& expected) {
              ASSERT_VALUES_EQUAL(current.val, expected.value<float>());
          });
    case avro::AVRO_DOUBLE:
        return primitive_equal<iceberg::double_value>(
          datum,
          value,
          [](
            const iceberg::double_value& current,
            const avro::GenericDatum& expected) {
              ASSERT_VALUES_EQUAL(current.val, expected.value<double>());
          });
    case avro::AVRO_BOOL:
        return primitive_equal<iceberg::boolean_value>(
          datum,
          value,
          [](
            const iceberg::boolean_value& current,
            const avro::GenericDatum& expected) {
              ASSERT_VALUES_EQUAL(current.val, expected.value<bool>());
          });
    case avro::AVRO_NULL:
        return AssertionSuccess();
    case avro::AVRO_RECORD: {
        const auto& record = datum.value<avro::GenericRecord>();
        if (!std::holds_alternative<std::unique_ptr<iceberg::struct_value>>(
              value)) {
            return AssertionFailure()
                   << fmt::format("Expected value {} to be a struct", value);
        }
        auto& record_v = std::get<std::unique_ptr<iceberg::struct_value>>(
          value);
        // since the nulls are skipped in iceberg schema the field count in
        // record_value is always greater than or equal to the field count in
        // avro record
        if (record.fieldCount() < record_v->fields.size()) {
            return AssertionFailure() << fmt::format(
                     "Record field count mismatch {} < {}",
                     record.fieldCount(),
                     record_v->fields.size());
        }
        size_t null_count = 0;
        for (size_t i = 0; i < record.fieldCount(); ++i) {
            auto& avro_field = record.fieldAt(i);
            if (avro_field.type() == avro::AVRO_NULL && !avro_field.isUnion()) {
                null_count++;
                continue;
            }
            auto r = value_matches(
              avro_field,
              record_v->fields.at(i - null_count),
              avro_field.isUnion());
            if (!r) {
                return AssertionFailure() << "Record field value mismatch at "
                                          << i << " - " << r.message();
            }
        }
        return AssertionSuccess();
    }
    case avro::AVRO_ENUM: {
        auto& generic_enum = datum.value<avro::GenericEnum>();
        if (!std::holds_alternative<iceberg::primitive_value>(value)) {
            return AssertionFailure()
                   << fmt::format("Expected value {} to be a primitive", value);
        }

        auto& actual_primitive = std::get<iceberg::primitive_value>(value);
        if (!std::holds_alternative<iceberg::long_value>(actual_primitive)) {
            return AssertionFailure() << fmt::format(
                     "Expected value {} to be of type iceberg::long_value",
                     value);
        }
        auto& value = std::get<iceberg::long_value>(actual_primitive);
        if (value.val != static_cast<int64_t>(generic_enum.value())) {
            return AssertionFailure() << fmt::format(
                     "Expected value {} to be equal to {}",
                     value,
                     generic_enum.value());
        }
        return AssertionSuccess();
    }
    case avro::AVRO_ARRAY: {
        auto& avro_array = datum.value<avro::GenericArray>();
        if (!std::holds_alternative<std::unique_ptr<iceberg::list_value>>(
              value)) {
            return AssertionFailure()
                   << fmt::format("Expected value {} to be a list", value);
        }
        auto& list = std::get<std::unique_ptr<iceberg::list_value>>(value);
        if (avro_array.value().size() != list->elements.size()) {
            return AssertionFailure() << fmt::format(
                     "Array size mismatch {} != {}",
                     avro_array.value().size(),
                     list->elements.size());
        }
        for (size_t i = 0; i < avro_array.value().size(); i++) {
            auto r = value_matches(
              avro_array.value()[i],
              list->elements[i],
              avro_array.value()[i].isUnion());
            if (!r) {
                return AssertionFailure()
                       << "Array element mismatch - " << r.message();
            }
        }

        return AssertionSuccess();
    }
    case avro::AVRO_MAP: {
        auto& avro_map = datum.value<avro::GenericMap>();
        if (!std::holds_alternative<std::unique_ptr<iceberg::map_value>>(
              value)) {
            return AssertionFailure()
                   << fmt::format("Expected value {} to be a map", value);
        }
        auto& map_v = std::get<std::unique_ptr<iceberg::map_value>>(value);
        if (avro_map.value().size() != map_v->kvs.size()) {
            return AssertionFailure() << fmt::format(
                     "Map size mismatch {} != {}",
                     avro_map.value().size(),
                     map_v->kvs.size());
        }
        for (size_t i = 0; i < avro_map.value().size(); ++i) {
            auto& avro_kv = avro_map.value()[i];
            auto& kv = map_v->kvs[i];
            if (!kv.val.has_value()) {
                return AssertionFailure()
                       << "Values in AVRO maps should always be present";
            }

            if (!std::holds_alternative<iceberg::primitive_value>(kv.key)) {
                return AssertionFailure() << "Map key must be a primitive";
            }
            auto& key_primitive = std::get<iceberg::primitive_value>(kv.key);
            if (!std::holds_alternative<iceberg::string_value>(key_primitive)) {
                return AssertionFailure() << "Map key must be a string";
            }
            auto& key_value = std::get<iceberg::string_value>(key_primitive);
            iobuf_parser parser(key_value.val.copy());

            ASSERT_VALUES_EQUAL(
              parser.read_string(key_value.val.size_bytes()), avro_kv.first);

            auto r = value_matches(
              avro_kv.second, kv.val, avro_kv.second.isUnion());
            if (!r) {
                return AssertionFailure()
                       << "Map value mismatch - " << r.message();
            }
        }

        return AssertionSuccess();
    }
    case avro::AVRO_UNION: {
        return AssertionFailure() << "Union should be handled at the top level";
    }
    case avro::AVRO_FIXED:
        if (datum.logicalType().type() == avro::LogicalType::DECIMAL) {
            return primitive_equal<iceberg::decimal_value>(
              datum,
              value,
              [](
                const iceberg::decimal_value& decimal,
                const avro::GenericDatum& expected) {
                  auto decimal_bytes = decimal_to_vector(decimal.val);
                  ASSERT_VALUES_EQUAL(
                    decimal_bytes,
                    expected.value<::avro::GenericFixed>().value())
              });
        }
        return primitive_equal<iceberg::fixed_value>(
          datum,
          value,
          [](
            const iceberg::fixed_value& current,
            const avro::GenericDatum& expected) {
              ASSERT_VALUES_EQUAL(
                iobuf_to_vector(current.val),
                expected.value<avro::GenericFixed>().value());
          });
    case avro::AVRO_SYMBOLIC:
        return AssertionFailure() << "Symbolic type not expected";
    case avro::AVRO_UNKNOWN:
        return AssertionFailure() << "Unknown type not expected";
    }
    return AssertionSuccess();
}
struct AvroValueTest : ::testing::TestWithParam<std::string_view> {
    /**
     * Validates if schema and value match each other
     */
    AssertionResult value_and_schema_matches(
      const iceberg::field_type& schema, const iceberg::value& value) {
        return std::visit(schema_validating_visitor{value}, schema);
    }
};

std::unordered_map<std::string_view, std::string_view> test_cases{
  {"tl_string", tl_string},
  {"tl_int", tl_int},
  {"tl_long", tl_long},
  {"tl_double", tl_double},
  {"tl_array", tl_array},
  {"tl_map", tl_map},
  {"tl_union", tl_union},
  {"big_record", big_record},
  {"logical_types", logical_types}};

TEST_P(AvroValueTest, RoundtripTest) {
    const auto test_case_key = GetParam();
    for (int i = 0; i < 100; ++i) {
        auto [value_result, schema_result, datum] = prepare_avro_test(
          test_cases[test_case_key]);
        ASSERT_TRUE(value_result.has_value());
        ASSERT_TRUE(schema_result.has_value());
        auto& iceberg_value = value_result.value();
        auto& iceberg_schema = schema_result.value();

        ASSERT_TRUE(value_and_schema_matches(iceberg_schema, iceberg_value));

        if (datum.type() != avro::AVRO_RECORD) {
            ASSERT_TRUE(
              std::holds_alternative<std::unique_ptr<iceberg::struct_value>>(
                iceberg_value));
            // top level type
            auto& struct_value
              = std::get<std::unique_ptr<iceberg::struct_value>>(iceberg_value);

            ASSERT_TRUE(value_matches(
              datum, struct_value->fields[0].value(), datum.isUnion()));
        } else {
            ASSERT_TRUE(
              value_matches(datum, value_result.value(), datum.isUnion()));
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
  RoundtripTest,
  AvroValueTest,
  Values(
    "tl_string",
    "tl_int",
    "tl_long",
    "tl_double",
    "tl_array",
    "tl_map",
    "tl_union",
    "big_record",
    "logical_types"));
