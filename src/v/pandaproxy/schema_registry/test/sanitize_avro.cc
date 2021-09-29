// Copyright 2021 Vectorized, Inc.
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

pps::schema_definition not_minimal{
  R"({
   "type": "record",
   "name": "myrecord",
   "fields": [{"type":"string","name":"f1"}]
})"};

pps::schema_definition not_minimal_sanitized{
  R"({"type":"record","name":"myrecord","fields":[{"type":"string","name":"f1"}]})"};

pps::schema_definition leading_dot{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"fields":[{"name":"f1","type":["null","string"]}],"name":".r1","type":"record"}]},{"name":"two","type":["null",".r1"]}]})"};

pps::schema_definition leading_dot_sanitized{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"fields":[{"name":"f1","type":["null","string"]}],"name":"r1","type":"record"}]},{"name":"two","type":["null","r1"]}]})"};

pps::schema_definition leading_dot_ns{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"fields":[{"name":"f1","type":["null","string"]}],"name":".ns.r1","type":"record"}]},{"name":"two","type":["null",".ns.r1"]}]})"};

pps::schema_definition leading_dot_ns_sanitized{
  R"({"type":"record","name":"record","fields":[{"name":"one","type":["null",{"fields":[{"name":"f1","type":["null","string"]}],"name":"r1","type":"record","namespace":".ns"}]},{"name":"two","type":["null",".ns.r1"]}]})"};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_minify) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(not_minimal).value()(),
      not_minimal_sanitized());
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_name) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(leading_dot).value()(),
      leading_dot_sanitized());
}

BOOST_AUTO_TEST_CASE(test_sanitize_avro_name_ns) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(leading_dot_ns).value(),
      leading_dot_ns_sanitized);
}

pps::schema_definition debezium_schema{
  R"({"type":"record","name":"SchemaChangeKey","namespace":"io.debezium.connector.mysql","fields":[{"name":"databaseName","type":"string"}],"connect.name":"io.debezium.connector.mysql.SchemaChangeKey"})"};

BOOST_AUTO_TEST_CASE(test_sanitize_avro_debzium) {
    BOOST_REQUIRE_EQUAL(
      pps::sanitize_avro_schema_definition(debezium_schema).value(),
      debezium_schema);
}
