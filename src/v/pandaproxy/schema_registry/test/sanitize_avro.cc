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
  R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"}]})",
  pps::schema_type::avro};

pps::unparsed_schema_definition leading_dot{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"fields":[{"name":"f1","type":["null","string"]}],"name":".r1","type":"record"}]},{"name":"two","type":["null",".r1"]}]})",
  pps::schema_type::avro};

pps::canonical_schema_definition leading_dot_sanitized{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"fields":[{"name":"f1","type":["null","string"]}],"name":"r1","type":"record"}]},{"name":"two","type":["null","r1"]}]})",
  pps::schema_type::avro};

pps::unparsed_schema_definition leading_dot_ns{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"fields":[{"name":"f1","type":["null","string"]}],"name":".ns.r1","type":"record"}]},{"name":"two","type":["null",".ns.r1"]}]})",
  pps::schema_type::avro};

pps::canonical_schema_definition leading_dot_ns_sanitized{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"fields":[{"name":"f1","type":["null","string"]}],"name":"r1","type":"record","namespace":".ns"}]},{"name":"two","type":["null",".ns.r1"]}]})",
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

pps::canonical_schema_definition debezium_schema{
  R"({"type":"record","name":"SchemaChangeKey","namespace":"io.debezium.connector.mysql","fields":[{"name":"databaseName","type":"string"}],"connect.name":"io.debezium.connector.mysql.SchemaChangeKey"})",
  pps::schema_type::avro};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_debzium) {
    auto unparsed = pandaproxy::schema_registry::unparsed_schema_definition{
      debezium_schema.raw()(), debezium_schema.type()};

    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(unparsed).value(), debezium_schema);
}
