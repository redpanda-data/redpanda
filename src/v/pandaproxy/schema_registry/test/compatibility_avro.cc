// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/store.h"
#include "pandaproxy/schema_registry/types.h"
#include "vassert.h"

#include <avro/Compiler.hh>
#include <avro/GenericDatum.hh>
#include <avro/ValidSchema.hh>
#include <avro/Validator.hh>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <rapidjson/encodings.h>

#include <memory>
#include <type_traits>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

const auto enum2 = pps::make_avro_schema_definition(R"({
  "name": "test2",
  "type": "enum",
  "symbols": ["One", "Two"]
})")
                     .value();

const auto enum3 = pps::make_avro_schema_definition(R"({
  "name": "test2",
  "type": "enum",
  "symbols": ["One", "Two", "Three"]
})")
                     .value();

const auto enum_2def = pps::make_avro_schema_definition(
                         R"({
  "name": "test2",
  "type": "enum",
  "symbols": ["One", "Two"],
  "default": "One"
})")
                         .value();

BOOST_AUTO_TEST_CASE(test_avro_enum) {
    // Adding an enum field is ok
    BOOST_REQUIRE(check_compatible(enum3, enum2));

    // Removing an enum field without default is not ok
    BOOST_REQUIRE(!check_compatible(enum2, enum3));

    // Removing an enum field with default is ok
    // TODO(Ben): Fix avro-cpp?
    // BOOST_REQUIRE(check_compatible(enum3, enum2_def));
}

// Schemas defined in AvroCompatibilityTest.java. Used here to ensure
// compatibility with the schema-registry
const auto schema1
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"}]})")
      .value();
const auto schema2
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":"string","name":"f2","default":"foo"}]})")
      .value();
const auto schema3
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":"string","name":"f2"}]})")
      .value();
const auto schema4
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1_new","aliases":["f1"]}]})")
      .value();
const auto schema6
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":["null","string"],"name":"f1","doc":"doc of f1"}]})")
      .value();
const auto schema7
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":["null","string","int"],"name":"f1","doc":"doc of f1"}]})")
      .value();
const auto schema8
  = pps::make_avro_schema_definition(
      R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"},{"type":"string","name":"f2","default":"foo"}]},{"type":"string","name":"f3","default":"bar"}]})")
      .value();
const auto badDefaultNullString_def = pps::make_avro_schema_definition(
  R"({"type":"record","name":"myrecord","fields":[{"type":["null","string"],"name":"f1","default":"null"},{"type":"string","name":"f2","default":"foo"},{"type":"string","name":"f3","default":"bar"}]})");

BOOST_AUTO_TEST_CASE(test_avro_basic_backwards_compat) {
    // Backward compatibility: A new schema is backward compatible if it can be
    // used to read the data written in the previous schema.

    // "adding a field with default is a backward compatible change
    BOOST_CHECK(check_compatible(schema2, schema1));

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

BOOST_AUTO_TEST_CASE(test_avro_basic_backwards_transitive_compat) {
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

BOOST_AUTO_TEST_CASE(test_schemaregistry_basic_forwards_compatibility) {
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

BOOST_AUTO_TEST_CASE(
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

BOOST_AUTO_TEST_CASE(test_basic_full_compatibility) {
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

BOOST_AUTO_TEST_CASE(test_basic_full_transitive_compatibility) {
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

BOOST_AUTO_TEST_CASE(test_avro_schema_definition) {
    // Parsing Canonical Form requires fields to be ordered:
    // name, type, fields, symbols, items, values, size
    pps::schema_definition expected{
      R"({"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"},{"name":"f2","type":"string","default":"foo"}]})"};
    static_assert(
      std::
        is_same_v<std::decay_t<decltype(schema2)>, pps::avro_schema_definition>,
      "schema2 is an avro_schema_definition");
    pps::schema_definition avro_conversion{schema2};
    BOOST_CHECK_EQUAL(expected, avro_conversion);
}
