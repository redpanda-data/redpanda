// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/test/compatibility_avro.h"

#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/test/compatibility_common.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_set.h>
#include <avro/Compiler.hh>
#include <boost/test/tools/old/interface.hpp>

#include <array>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

namespace {

bool check_compatible(
  const pps::canonical_schema_definition& r,
  const pps::canonical_schema_definition& w) {
    pps::sharded_store s;
    return check_compatible(
             pps::make_avro_schema_definition(
               s, {pps::subject("r"), {r.shared_raw(), pps::schema_type::avro}})
               .get(),
             pps::make_avro_schema_definition(
               s, {pps::subject("w"), {w.shared_raw(), pps::schema_type::avro}})
               .get())
      .is_compat;
}

pps::compatibility_result check_compatible_verbose(
  const pps::canonical_schema_definition& r,
  const pps::canonical_schema_definition& w) {
    pps::sharded_store s;
    return check_compatible(
      pps::make_avro_schema_definition(
        s, {pps::subject("r"), {r.shared_raw(), pps::schema_type::avro}})
        .get(),
      pps::make_avro_schema_definition(
        s, {pps::subject("w"), {w.shared_raw(), pps::schema_type::avro}})
        .get(),
      pps::verbose::yes);
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_avro_type_promotion) {
    BOOST_REQUIRE(check_compatible(schema_long, schema_int));
    BOOST_REQUIRE(check_compatible(schema_float, schema_int));
    BOOST_REQUIRE(check_compatible(schema_double, schema_int));

    BOOST_REQUIRE(check_compatible(schema_float, schema_long));
    BOOST_REQUIRE(check_compatible(schema_double, schema_long));

    BOOST_REQUIRE(check_compatible(schema_double, schema_float));

    BOOST_REQUIRE(check_compatible(schema_string, schema_bytes));
    BOOST_REQUIRE(check_compatible(schema_bytes, schema_string));
}

SEASTAR_THREAD_TEST_CASE(test_avro_enum) {
    // Adding an enum field is ok
    BOOST_REQUIRE(check_compatible(enum3, enum2));

    // Removing an enum field without default is not ok
    BOOST_REQUIRE(!check_compatible(enum2, enum3));

    // Removing an enum field with default is ok
    BOOST_REQUIRE(check_compatible(enum3, enum2_def));

    // Test from Materialize (follows NodeSymbolic)
    BOOST_REQUIRE(check_compatible(enum2_mat, enum1_mat));
}

SEASTAR_THREAD_TEST_CASE(test_avro_union) {
    BOOST_REQUIRE(check_compatible(union2, union0));

    BOOST_REQUIRE(!check_compatible(union1, union0));
}

SEASTAR_THREAD_TEST_CASE(test_avro_array) {
    BOOST_REQUIRE(check_compatible(long_array, int_array));

    BOOST_REQUIRE(!check_compatible(int_array, long_array));
}

SEASTAR_THREAD_TEST_CASE(test_avro_basic_backwards_compat) {
    // Backward compatibility: A new schema is backward compatible if it can be
    // used to read the data written in the previous schema.

    // "adding a field with default is a backward compatible change
    BOOST_CHECK(check_compatible(schema2, schema1));

    // "adding a union field with default is a backward compatible change
    BOOST_CHECK(check_compatible(schema2_union_null_first, schema1));

    // "adding a field w/o default is NOT a backward compatible change
    BOOST_CHECK(!check_compatible(schema3, schema1));

    // "changing field name with alias is a backward compatible change
    // TODO(Ben): avro-cpp:
    // https://issues.apache.org/jira/browse/AVRO-1496
    // BOOST_CHECK(check_compatible(schema4, schema1));

    // evolving a field type to a union is a backward compatible change
    BOOST_CHECK(check_compatible(schema6, schema1));

    // removing a type from a union is NOT a backward compatible change
    BOOST_CHECK(!check_compatible(schema1, schema6));

    // adding a new type in union is a backward compatible change
    BOOST_CHECK(check_compatible(schema7, schema6));

    // removing a type from a union is NOT a backward compatible change
    BOOST_CHECK(!check_compatible(schema6, schema7));
}

SEASTAR_THREAD_TEST_CASE(test_avro_basic_backwards_transitive_compat) {
    // Backward transitive compatibility: A new schema is backward compatible if
    // it can be used to read the data written in all previous schemas.

    // iteratively adding fields with defaults is a compatible change
    BOOST_CHECK(check_compatible(schema8, schema1));
    BOOST_CHECK(check_compatible(schema8, schema2));

    // adding a field with default is a backward compatible change
    BOOST_CHECK(check_compatible(schema2, schema1));

    // removing a default is a compatible change, but not transitively
    BOOST_CHECK(check_compatible(schema3, schema2));
    BOOST_CHECK(!check_compatible(schema3, schema1));
}

SEASTAR_THREAD_TEST_CASE(test_schemaregistry_basic_forwards_compatibility) {
    // Forward compatibility: A new schema is forward compatible if the previous
    // schema can read data written in this schema.

    // adding a field is a forward compatible change
    BOOST_CHECK(check_compatible(schema1, schema2));

    // adding a field is a forward compatible change
    BOOST_CHECK(check_compatible(schema1, schema3));

    // adding a field is a forward compatible change
    BOOST_CHECK(check_compatible(schema2, schema3));

    // adding a field is a forward compatible change
    BOOST_CHECK(check_compatible(schema3, schema2));

    // removing a default is not a transitively compatible change
    // # Only schema 2 is checked!
    // # BOOST_CHECK(!check_compatible(schema3, schema1));
    BOOST_CHECK(check_compatible(schema2, schema1));
}

SEASTAR_THREAD_TEST_CASE(
  test_schemaregistry_basic_forwards_transitive_compatibility) {
    // Forward transitive compatibility: A new schema is forward compatible
    // if all previous schemas can read data written in this schema.
    // iteratively removing fields with defaults is a compatible change

    BOOST_CHECK(check_compatible(schema8, schema1));
    BOOST_CHECK(check_compatible(schema2, schema1));

    // adding default to a field is a compatible change
    BOOST_CHECK(check_compatible(schema3, schema2));

    // removing a field with a default is a compatible change
    BOOST_CHECK(check_compatible(schema2, schema1));

    // removing a default is not a transitively compatible change
    BOOST_CHECK(check_compatible(schema2, schema1));
    BOOST_CHECK(!check_compatible(schema3, schema1));
}

SEASTAR_THREAD_TEST_CASE(test_basic_full_compatibility) {
    // Full compatibility: A new schema is fully compatible if it’s both
    // backward and forward compatible.

    // adding a field with default is a backward and a forward
    // compatible change
    BOOST_CHECK(check_compatible(schema2, schema1));
    BOOST_CHECK(check_compatible(schema1, schema2));

    // transitively adding a field without a default is not a compatible
    // change # Only schema 2 is checked! #
    // BOOST_CHECK(!check_compatible(schema3, schema1)); #
    // BOOST_CHECK(!check_compatible(schema1, schema3)); #
    BOOST_CHECK(check_compatible(schema3, schema2));
    BOOST_CHECK(check_compatible(schema2, schema3));

    // transitively removing a field without a default is not a
    // compatible change # Only schema 2 is checked! #
    // BOOST_CHECK(check_compatible(schema1, schema3)); #
    // BOOST_CHECK(check_compatible(schema3, schema1)); #
    BOOST_CHECK(check_compatible(schema1, schema2));
    BOOST_CHECK(check_compatible(schema1, schema2));
}

SEASTAR_THREAD_TEST_CASE(test_basic_full_transitive_compatibility) {
    // Full transitive compatibility: A new schema is fully compatible
    // if it’s both transitively backward and transitively forward
    // compatible with the entire schema history.

    // iteratively adding fields with defaults is a compatible change
    BOOST_CHECK(check_compatible(schema8, schema1));
    BOOST_CHECK(check_compatible(schema1, schema8));
    BOOST_CHECK(check_compatible(schema8, schema2));
    BOOST_CHECK(check_compatible(schema2, schema8));

    // iteratively removing fields with defaults is a compatible change
    BOOST_CHECK(check_compatible(schema1, schema8));
    BOOST_CHECK(check_compatible(schema8, schema1));
    BOOST_CHECK(check_compatible(schema1, schema2));
    BOOST_CHECK(check_compatible(schema2, schema1));

    // adding default to a field is a compatible change
    BOOST_CHECK(check_compatible(schema2, schema3));
    BOOST_CHECK(check_compatible(schema3, schema2));

    // removing a field with a default is a compatible change
    BOOST_CHECK(check_compatible(schema1, schema2));
    BOOST_CHECK(check_compatible(schema2, schema1));

    // adding a field with default is a compatible change
    BOOST_CHECK(check_compatible(schema2, schema1));
    BOOST_CHECK(check_compatible(schema1, schema2));

    // removing a default from a field compatible change
    BOOST_CHECK(check_compatible(schema3, schema2));
    BOOST_CHECK(check_compatible(schema2, schema3));

    // transitively adding a field without a default is not a compatible
    // change
    BOOST_CHECK(check_compatible(schema3, schema2));
    BOOST_CHECK(!check_compatible(schema3, schema1));
    BOOST_CHECK(check_compatible(schema2, schema3));
    BOOST_CHECK(check_compatible(schema1, schema3));

    // transitively removing a field without a default is not a
    // compatible change
    BOOST_CHECK(check_compatible(schema1, schema2));
    BOOST_CHECK(check_compatible(schema1, schema3));
    BOOST_CHECK(check_compatible(schema2, schema1));
    BOOST_CHECK(!check_compatible(schema3, schema1));
}

SEASTAR_THREAD_TEST_CASE(test_avro_schema_definition) {
    // Parsing Canonical Form requires fields to be ordered:
    // name, type, fields, symbols, items, values, size
    pps::canonical_schema_definition expected{
      R"({"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"},{"name":"f2","type":"string","default":"foo"}]})",
      pps::schema_type::avro};
    pps::sharded_store s;
    auto valid = pps::make_avro_schema_definition(
                   s,
                   {pps::subject("s2"),
                    {schema2.shared_raw(), pps::schema_type::avro}})
                   .get();
    static_assert(
      std::
        is_same_v<std::decay_t<decltype(valid)>, pps::avro_schema_definition>,
      "schema2 is an avro_schema_definition");
    pps::canonical_schema_definition avro_conversion{valid};
    BOOST_CHECK_EQUAL(expected, avro_conversion);
    BOOST_CHECK_EQUAL(valid.name(), "myrecord");
}

SEASTAR_THREAD_TEST_CASE(test_avro_schema_definition_custom_attributes) {
    // https://github.com/redpanda-data/redpanda/issues/7274
    // custom attributes supported only at field level
    const auto avro_metadata_schema
      = pps::sanitize_avro_schema_definition(
          {R"({"type":"record","name":"foo","ignored_attr":true,"fields":[{"name":"bar","type":"float","extra_attr":true}]})",
           pps::schema_type::avro})
          .value();
    pps::canonical_schema_definition expected{
      R"({"type":"record","name":"foo","fields":[{"name":"bar","type":"float","extra_attr":true}]})",
      pps::schema_type::avro};
    pps::sharded_store s;
    auto valid = pps::make_avro_schema_definition(
                   s,
                   {pps::subject("s2"),
                    {avro_metadata_schema.shared_raw(),
                     pps::schema_type::avro}})
                   .get();
    static_assert(
      std::
        is_same_v<std::decay_t<decltype(valid)>, pps::avro_schema_definition>,
      "schema2 is an avro_schema_definition");
    pps::canonical_schema_definition avro_conversion{valid};
    BOOST_CHECK_EQUAL(expected, avro_conversion);
}

SEASTAR_THREAD_TEST_CASE(test_avro_alias_resolution_stopgap) {
    auto writer = avro::compileJsonSchemaFromString(
      R"({"type":"record","fields":[{"name":"bar","type":"float"}],"name":"foo"})");

    auto reader = avro::compileJsonSchemaFromString(
      R"({"type":"record","fields":[{"name":"bar","type":"float"}],"name":"foo_renamed","aliases":["foo"]})");

    auto& writer_root = *writer.root();
    auto& reader_root = *reader.root();

    // This should resolve to true but it currently resolves to false because
    // the Avro library doesn't fully support handling schema resolution with
    // aliases yet. When the avro library supports alias resolution this test
    // will fail and we should then update the compat check to don't report an
    // incompatibility when the record names are properly aliased.
    BOOST_CHECK(!writer_root.resolve(reader_root));
}

namespace {

const auto schema_old = pps::sanitize_avro_schema_definition(
                          {
                            R"({
    "type": "record",
    "name": "myrecord",
    "fields": [
        {
            "name": "f1",
            "type": "string"
        },
        {
            "name": "f2",
            "type": {
                "name": "nestedRec",
                "type": "record",
                "fields": [
                    {
                        "name": "nestedF",
                        "type": "string"
                    }
                ]
            }
        },
        {
            "name": "uF",
            "type": ["int", "string"]
        },
        {
            "name": "enumF",
            "type": {
                "name": "ABorC",
                "type": "enum",
                "symbols": ["a", "b", "c"]
            }
        },
        {
            "name": "fixedF",
            "type": {
                "type": "fixed",
                "name": "fixedT",
                "size": 1
            }
        },
	      {
	          "name": "oldUnion",
	          "type": ["int", "string"]
	      },
	      {
	          "name": "newUnion",
	          "type": "int"
	      },
        {
            "name": "someList",
            "type": {
                "type": "array",
                "items": "int"
            }
        },
        {
            "name": "otherEnumF",
            "type": {
                "type": "enum",
                "name": "someEnum1",
                "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
            }
        }
    ]
})",
                            pps::schema_type::avro})
                          .value();

const auto schema_new = pps::sanitize_avro_schema_definition(
                          {
                            R"({
    "type": "record",
    "name": "myrecord",
    "fields": [
        {
            "name": "f1",
            "type": "int"
        },
        {
            "name": "f2",
            "type": {
                "name": "nestedRec2",
                "type": "record",
                "fields": [
                    {
                        "name": "broken",
                        "type": "string"
                    }
                ]
            }
        },
        {
            "name": "uF",
            "type": ["string"]
        },
        {
            "name": "enumF",
            "type": {
                "name": "ABorC",
                "type": "enum",
                "symbols": ["a"]
            }
        },
        {
            "name": "fixedF",
            "type": {
                "type": "fixed",
                "name": "fixedT",
                "size": 2
            }
        },
        {
            "name": "oldUnion",
	          "type": "boolean"
	      },
	      {
	          "name": "newUnion",
	          "type": ["boolean", "string"]
	      },
        {
            "name": "someList",
            "type": {
                "type": "array",
                "items": "long"
            }
        },
        {
            "name": "otherEnumF",
            "type": {
                "type": "enum",
                "name": "someEnum2",
                "aliases": ["someEnum1"],
                "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
            }
        }
    ]
})",
                            pps::schema_type::avro})
                          .value();

using incompatibility = pps::avro_incompatibility;

const absl::flat_hash_set<incompatibility> forward_expected{
  {"/fields/0/type",
   incompatibility::Type::type_mismatch,
   "reader type: STRING not compatible with writer type: INT"},
  {"/fields/1/type/name",
   incompatibility::Type::name_mismatch,
   "expected: nestedRec2"},
  {"/fields/1/type/fields/0",
   incompatibility::Type::reader_field_missing_default_value,
   "nestedF"},
  {"/fields/4/type/size",
   incompatibility::Type::fixed_size_mismatch,
   "expected: 2, found: 1"},
  {"/fields/5/type",
   incompatibility::Type::missing_union_branch,
   "reader union lacking writer type: BOOLEAN"},
  {"/fields/6/type", /* NOTE: this is more path info than the reference impl */
   incompatibility::Type::type_mismatch,
   "reader type: INT not compatible with writer type: BOOLEAN"},
  {"/fields/6/type", /* NOTE: this is more path info than the reference impl */
   incompatibility::Type::type_mismatch,
   "reader type: INT not compatible with writer type: STRING"},
  {"/fields/7/type/items",
   incompatibility::Type::type_mismatch,
   "reader type: INT not compatible with writer type: LONG"},
  {"/fields/8/type/name",
   incompatibility::Type::name_mismatch,
   "expected: someEnum2"},
};
const absl::flat_hash_set<incompatibility> backward_expected{
  {"/fields/0/type",
   incompatibility::Type::type_mismatch,
   "reader type: INT not compatible with writer type: STRING"},
  {"/fields/1/type/name",
   incompatibility::Type::name_mismatch,
   "expected: nestedRec"},
  {"/fields/1/type/fields/0",
   incompatibility::Type::reader_field_missing_default_value,
   "broken"},
  {"/fields/2/type/0",
   incompatibility::Type::missing_union_branch,
   "reader union lacking writer type: INT"},
  {"/fields/3/type/symbols",
   incompatibility::Type::missing_enum_symbols,
   "[b, c]"},
  {"/fields/4/type/size",
   incompatibility::Type::fixed_size_mismatch,
   "expected: 1, found: 2"},
  {"/fields/5/type", /* NOTE: this is more path info than the reference impl */
   incompatibility::Type::type_mismatch,
   "reader type: BOOLEAN not compatible with writer type: INT"},
  {"/fields/5/type", /* NOTE: this is more path info than the reference impl */
   incompatibility::Type::type_mismatch,
   "reader type: BOOLEAN not compatible with writer type: STRING"},
  {"/fields/6/type",
   incompatibility::Type::missing_union_branch,
   "reader union lacking writer type: INT"},
  // Note: once Avro supports schema resolution with name aliases, the
  // incompatibility below should go away
  {"/fields/8/type/name",
   incompatibility::Type::name_mismatch,
   "expected: someEnum1 (alias resolution is not yet fully supported)"},
};

const auto compat_data = std::to_array<compat_test_data<incompatibility>>({
  {
    schema_old.share(),
    schema_new.share(),
    forward_expected,
  },
  {
    schema_new.share(),
    schema_old.share(),
    backward_expected,
  },
});

std::string format_set(const absl::flat_hash_set<ss::sstring>& d) {
    return fmt::format("{}", fmt::join(d, "\n"));
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_avro_compat_messages) {
    for (const auto& cd : compat_data) {
        auto compat = check_compatible_verbose(cd.reader, cd.writer);
        absl::flat_hash_set<ss::sstring> errs{
          compat.messages.begin(), compat.messages.end()};
        absl::flat_hash_set<ss::sstring> expected{
          cd.expected.messages.begin(), cd.expected.messages.end()};

        BOOST_CHECK(!compat.is_compat);
        BOOST_CHECK_EQUAL(errs.size(), expected.size());
        BOOST_REQUIRE_MESSAGE(
          errs == expected,
          fmt::format("{} != {}", format_set(errs), format_set(expected)));
    }
}
