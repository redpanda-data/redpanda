// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#define BOOST_TEST_MODULE pandaproxy_schema_registry_unit

#include "pandaproxy/schema_registry/util.h"

#include <boost/test/unit_test.hpp>

namespace pps = pandaproxy::schema_registry;

constexpr std::string_view example_avro_schema{R"({
  "namespace": "com.acme",
  "protocol": "HelloWorld",
  "doc": "Protocol Greetings",

  "types": [
    {"name": "Greeting", "type": "record", "fields": [
      {"name": "message", "type": "string"}]},
    {"name": "Curse", "type": "error", "fields": [
      {"name": "message", "type": "string"}]}
  ],

  "messages": {
    "hello": {
      "doc": "Say hello.",
      "request": [{"name": "greeting", "type": "Greeting" }],
      "response": "Greeting",
      "errors": ["Curse"]
    }
  }
})"};

constexpr std::string_view minified_avro_schema{
  R"({"namespace":"com.acme","protocol":"HelloWorld","doc":"Protocol Greetings","types":[{"name":"Greeting","type":"record","fields":[{"name":"message","type":"string"}]},{"name":"Curse","type":"error","fields":[{"name":"message","type":"string"}]}],"messages":{"hello":{"doc":"Say hello.","request":[{"name":"greeting","type":"Greeting"}],"response":"Greeting","errors":["Curse"]}}})"};

BOOST_AUTO_TEST_CASE(test_make_schema_definition) {
    auto res = pps::make_schema_definition<json::UTF8<>>(example_avro_schema);

    BOOST_REQUIRE(res);
    auto str = to_string(std::move(res).value());
    BOOST_REQUIRE_EQUAL(str, minified_avro_schema);
}

BOOST_AUTO_TEST_CASE(test_make_schema_definition_failure) {
    auto res = pps::make_schema_definition<json::UTF8<>>(
      "this should fail to parse");

    BOOST_REQUIRE(res.has_error());
    BOOST_REQUIRE_EQUAL(res.error().code(), pps::error_code::schema_invalid);
}
