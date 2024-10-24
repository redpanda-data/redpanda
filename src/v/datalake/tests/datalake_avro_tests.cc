/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/schema_avro.h"
#include "gtest/gtest.h"
#include "iceberg/datatypes.h"

#include <seastar/core/sstring.hh>

#include <avro/Compiler.hh>
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

auto match_field(
  int id, const ss::sstring& name, const iceberg::field_type& ft) {
    return Pointee(AllOf(
      Field(&iceberg::nested_field::id, Eq(id)),
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
        1, iceberg::field_required::yes, iceberg::int_type{}));
    tl_type_expectations.emplace_back(
      "root",
      tl_map,
      iceberg::map_type::create(
        1,
        iceberg::string_type{},
        2,
        iceberg::field_required::yes,
        iceberg::int_type{}));

    iceberg::struct_type union_st;
    union_st.fields.push_back(iceberg::nested_field::create(
      1, "union_opt_0", iceberg::field_required::no, iceberg::int_type{}));
    union_st.fields.push_back(iceberg::nested_field::create(
      2, "union_opt_1", iceberg::field_required::no, iceberg::long_type{}));
    union_st.fields.push_back(iceberg::nested_field::create(
      3, "union_opt_2", iceberg::field_required::no, iceberg::float_type{}));

    tl_type_expectations.emplace_back("root", tl_union, std::move(union_st));

    for (auto& [name, schema, type] : tl_type_expectations) {
        auto root = load_json_schema(schema);
        auto iceberg_struct_res = datalake::type_to_iceberg(root);

        ASSERT_TRUE(iceberg_struct_res.has_value());

        ASSERT_THAT(
          iceberg_struct_res.value().fields,
          testing::ElementsAre(match_field(0, name, type)));
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

iceberg::struct_type nested_record(int initial_id) {
    iceberg::struct_type st;
    int i = initial_id;
    st.fields.push_back(iceberg::nested_field::create(
      i++, "inval1", iceberg::field_required::yes, iceberg::double_type{}));
    st.fields.push_back(iceberg::nested_field::create(
      i++, "inval2", iceberg::field_required::yes, iceberg::string_type{}));
    st.fields.push_back(iceberg::nested_field::create(
      i++, "inval3", iceberg::field_required::yes, iceberg::int_type{}));
    return st;
}

AssertionResult field_matches(
  const iceberg::nested_field_ptr& field,
  int id,
  const ss::sstring& name,
  const iceberg::field_type& ft) {
    if (field->id != id || field->name != name || field->type != ft) {
        return AssertionFailure() << fmt::format(
                 "\nexpected: (id: {}, name: {}, type: {})\n"
                 "actual  : (id: {}, name: {}, type: {})",
                 id,
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
      field_matches(struct_t.fields[0], 0, "mylong", iceberg::long_type{}));

    ASSERT_TRUE(
      field_matches(struct_t.fields[1], 4, "nestedrecord", nested_record(1)));

    ASSERT_TRUE(field_matches(
      struct_t.fields[2],
      7,
      "mymap",
      iceberg::map_type::create(
        5,
        iceberg::string_type{},
        6,
        iceberg::field_required::yes,
        iceberg::int_type{})));

    ASSERT_TRUE(field_matches(
      struct_t.fields[3],
      13,
      "recordmap",
      iceberg::map_type::create(
        11,
        iceberg::string_type{},
        12,
        iceberg::field_required::yes,
        nested_record(8))));

    ASSERT_TRUE(field_matches(
      struct_t.fields[4],
      15,
      "myarray",
      iceberg::list_type::create(
        14, iceberg::field_required::yes, iceberg::double_type{})));
    ASSERT_TRUE(
      field_matches(struct_t.fields[5], 16, "myenum", iceberg::int_type{}));

    iceberg::struct_type union_struct;
    // starts from union_1 as the union_0 is null type which is not represented
    // here
    union_struct.fields.push_back(iceberg::nested_field::create(
      19,
      "union_opt_1",
      iceberg::field_required::no,
      iceberg::map_type::create(
        17,
        iceberg::string_type{},
        18,
        iceberg::field_required::yes,
        iceberg::int_type{})));
    union_struct.fields.push_back(iceberg::nested_field::create(
      20, "union_opt_2", iceberg::field_required::no, iceberg::float_type{}));

    ASSERT_TRUE(field_matches(
      struct_t.fields[6], 21, "myunion", std::move(union_struct)));
    iceberg::struct_type another_union_struct;

    another_union_struct.fields.push_back(iceberg::nested_field::create(
      22, "union_opt_0", iceberg::field_required::no, iceberg::binary_type{}));

    ASSERT_TRUE(field_matches(
      struct_t.fields[7], 23, "anotherunion", std::move(another_union_struct)));

    ASSERT_TRUE(
      field_matches(struct_t.fields[8], 24, "mybool", iceberg::boolean_type{}));
    ASSERT_TRUE(field_matches(
      struct_t.fields[9], 28, "anothernested", nested_record(25)));
    ASSERT_TRUE(field_matches(
      struct_t.fields[10], 29, "myfixed", iceberg::fixed_type{.length = 16}));
    ASSERT_TRUE(field_matches(
      struct_t.fields[11], 30, "anotherint", iceberg::int_type{}));
    ASSERT_TRUE(
      field_matches(struct_t.fields[12], 31, "bytes", iceberg::binary_type{}));
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

// R"({"type":"int","logicalType":"time-millis"})",
// R"({"type":"long","logicalType":"time-micros"})",
// R"({"type":"long","logicalType":"timestamp-millis"})",
// R"({"type":"long","logicalType":"timestamp-micros"})",
// R"({"type":"fixed","name":"test","size":12,"logicalType":"duration"})",
// R"({"type":"string","logicalType":"uuid"})",

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
    EXPECT_TRUE(field_matches(
      struct_t.fields[0], 0, "uuid_string", iceberg::uuid_type{}));
    EXPECT_TRUE(
      field_matches(struct_t.fields[1], 1, "date_int", iceberg::date_type{}));
    EXPECT_TRUE(field_matches(
      struct_t.fields[2],
      2,
      "bytes_decimal",
      iceberg::decimal_type{.precision = 38, .scale = 9}));
    EXPECT_TRUE(field_matches(
      struct_t.fields[3],
      3,
      "fixed_decimal",
      iceberg::decimal_type{.precision = 20, .scale = 9}));
    EXPECT_TRUE(field_matches(
      struct_t.fields[4], 4, "int_time_ms", iceberg::time_type{}));
    EXPECT_TRUE(field_matches(
      struct_t.fields[5], 5, "long_time_micro", iceberg::time_type{}));
    EXPECT_TRUE(field_matches(
      struct_t.fields[6], 6, "long_ts_ms", iceberg::timestamp_type{}));
    EXPECT_TRUE(field_matches(
      struct_t.fields[7], 7, "long_ts_micro", iceberg::timestamp_type{}));
    // Duration logical type is not intrepreted in as a separate iceberg type
    EXPECT_TRUE(field_matches(
      struct_t.fields[8], 8, "duration_fixed", iceberg::fixed_type{12}));
}
