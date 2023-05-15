// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/test/compatibility_avro.h"
#include "pandaproxy/schema_registry/types.h"

#include <boost/test/unit_test.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

pps::unparsed_schema_definition not_minimal{
  R"({
   "type": "record",
   "name": "myrecord",
   "fields": [{"type":"string","name":"f1"}]
})",
  pps::schema_type::avro};

pps::canonical_schema_definition not_minimal_sanitized{
  R"({"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]})",
  pps::schema_type::avro};

pps::unparsed_schema_definition leading_dot{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"fields":[{"name":"f1","type":["null","string"]}],"name":".r1","type":"record"}]},{"name":"two","type":["null",".r1"]}]})",
  pps::schema_type::avro};

pps::canonical_schema_definition leading_dot_sanitized{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"type":"record","name":"r1","fields":[{"name":"f1","type":["null","string"]}]}]},{"name":"two","type":["null","r1"]}]})",
  pps::schema_type::avro};

pps::unparsed_schema_definition leading_dot_ns{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"fields":[{"name":"f1","type":["null","string"]}],"name":".ns.r1","type":"record"}]},{"name":"two","type":["null",".ns.r1"]}]})",
  pps::schema_type::avro};

pps::canonical_schema_definition leading_dot_ns_sanitized{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"type":"record","name":"r1","namespace":".ns","fields":[{"name":"f1","type":["null","string"]}]}]},{"name":"two","type":["null",".ns.r1"]}]})",
  pps::schema_type::avro};

pps::unparsed_schema_definition record_not_sorted{
  R"({"name":"sort_record","type":"record","aliases":["alias"],"fields":[{"type":"string","name":"one"}],"namespace":"ns","doc":"doc"})",
  pps::schema_type::avro};

pps::canonical_schema_definition record_sorted_sanitized{
  R"({"type":"record","name":"sort_record","namespace":"ns","doc":"doc","fields":[{"name":"one","type":"string"}],"aliases":["alias"]})",
  pps::schema_type::avro};

pps::unparsed_schema_definition enum_not_sorted{
  R"({"name":"ns.sort_enum","type":"enum","aliases":["alias"],"symbols":["one", "two", "three"],"default":"two","doc":"doc"})",
  pps::schema_type::avro};

pps::canonical_schema_definition enum_sorted_sanitized{
  R"({"type":"enum","name":"sort_enum","namespace":"ns","doc":"doc","symbols":["one","two","three"],"default":"two","aliases":["alias"]})",
  pps::schema_type::avro};

pps::unparsed_schema_definition array_not_sorted{
  R"({"type": "array", "default": [], "items" : "string"})",
  pps::schema_type::avro};

pps::canonical_schema_definition array_sorted_sanitized{
  R"({"type":"array","items":"string","default":[]})", pps::schema_type::avro};

pps::unparsed_schema_definition map_not_sorted{
  R"({"type": "map", "default": {}, "values" : "string"})",
  pps::schema_type::avro};

pps::canonical_schema_definition map_sorted_sanitized{
  R"({"type":"map","values":"string","default":{}})", pps::schema_type::avro};

pps::unparsed_schema_definition fixed_not_sorted{
  R"({"size":16, "type": "fixed", "aliases":["fixed"], "name":"ns.sorted_fixed"})",
  pps::schema_type::avro};

pps::canonical_schema_definition fixed_sorted_sanitized{
  R"({"type":"fixed","name":"sorted_fixed","namespace":"ns","size":16,"aliases":["fixed"]})",
  pps::schema_type::avro};

pps::unparsed_schema_definition record_of_obj_unsanitized{
  R"({"name":"sort_record_of_obj","type":"record","fields":[{"type":{"type":"string","connect.parameters":{"tidb_type":"TEXT"}},"default":"","name":"field"}]})",
  pps::schema_type::avro};

pps::canonical_schema_definition record_of_obj_sanitized{
  R"({"type":"record","name":"sort_record_of_obj","fields":[{"name":"field","type":{"type":"string","connect.parameters":{"tidb_type":"TEXT"}},"default":""}]})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_minify) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(not_minimal).value(),
      not_minimal_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_name) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(leading_dot).value(),
      leading_dot_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_name_ns) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(leading_dot_ns).value(),
      leading_dot_ns_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_record_sorting) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(record_not_sorted).value(),
      record_sorted_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_enum_sorting) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(enum_not_sorted).value(),
      enum_sorted_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_array_sorting) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(array_not_sorted).value(),
      array_sorted_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_map_sorting) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(map_not_sorted).value(),
      map_sorted_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_fixed_sorting) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(fixed_not_sorted).value(),
      fixed_sorted_sanitized);
}

BOOST_AUTO_TEST_CASE(test_sanitize_record_of_obj_sorting) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(record_of_obj_unsanitized).value(),
      record_of_obj_sanitized);
}

pps::canonical_schema_definition debezium_schema{
  R"({"type":"record","name":"SchemaChangeKey","namespace":"io.debezium.connector.mysql","fields":[{"name":"databaseName","type":"string"}],"connect.name":"io.debezium.connector.mysql.SchemaChangeKey"})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_debzium) {
    auto unparsed = pandaproxy::schema_registry::unparsed_schema_definition{
      debezium_schema.raw()(), debezium_schema.type()};

    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(unparsed).value(), debezium_schema);
}
