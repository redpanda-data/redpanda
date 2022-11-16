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
#include "pandaproxy/schema_registry/types.h"

#include <seastar/testing/thread_test_case.hh>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

bool check_compatible(
  const pps::canonical_schema_definition& r,
  const pps::canonical_schema_definition& w) {
    pps::sharded_store s;
    return check_compatible(
      pps::make_avro_schema_definition(
        s, {pps::subject("r"), {r.raw(), pps::schema_type::avro}})
        .get(),
      pps::make_avro_schema_definition(
        s, {pps::subject("w"), {w.raw(), pps::schema_type::avro}})
        .get());
}

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
    auto valid
      = pps::make_avro_schema_definition(
          s, {pps::subject("s2"), {schema2.raw(), pps::schema_type::avro}})
          .get();
    static_assert(
      std::
        is_same_v<std::decay_t<decltype(valid)>, pps::avro_schema_definition>,
      "schema2 is an avro_schema_definition");
    pps::canonical_schema_definition avro_conversion{valid};
    BOOST_CHECK_EQUAL(expected, avro_conversion);
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
                    {avro_metadata_schema.raw(), pps::schema_type::avro}})
                   .get();
    static_assert(
      std::
        is_same_v<std::decay_t<decltype(valid)>, pps::avro_schema_definition>,
      "schema2 is an avro_schema_definition");
    pps::canonical_schema_definition avro_conversion{valid};
    BOOST_CHECK_EQUAL(expected, avro_conversion);
}
