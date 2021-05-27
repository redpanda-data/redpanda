// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/requests/post_subject_versions.h"

#include "seastarx.h"

#include <seastar/testing/thread_test_case.hh>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <type_traits>

namespace ppj = pandaproxy::json;
namespace pps = pandaproxy::schema_registry;

SEASTAR_THREAD_TEST_CASE(test_post_subject_versions_parser) {
    const ss::sstring escaped_schema_def{
      R"({\n\"type\": \"record\",\n\"name\": \"test\",\n\"fields\":\n  [\n    {\n      \"type\": \"string\",\n      \"name\": \"field1\"\n    },\n    {\n      \"type\": \"com.acme.Referenced\",\n      \"name\": \"int\"\n    }\n  ]\n})"};
    const ss::sstring expected_schema_def{
      R"({"type":"record","name":"test","fields":[{"type":"string","name":"field1"},{"type":"com.acme.Referenced","name":"int"}]})"};

    const ss::sstring payload{
      R"(
{
  "schema": ")"
      + escaped_schema_def + R"(",
  "schemaType": "AVRO",
  "references": [
    {
       "name": "com.acme.Referenced",
       "subject":  "childSubject",
       "version": 1
    }
  ]
})"};
    const pps::subject sub{"test_subject"};
    const pps::post_subject_versions_request::body expected{
      .schema{pps::schema_definition{expected_schema_def}},
      .type = pps::schema_type::avro,
      .references{pps::post_subject_versions_request::schema_reference{
        .name{"com.acme.Referenced"},
        .sub{pps::subject{"childSubject"}},
        .version{pps::schema_version{1}}}}};

    auto result{ppj::rjson_parse(
      payload.data(), pps::post_subject_versions_request_handler{})};

    BOOST_REQUIRE_EQUAL(expected.schema, result.schema);
    BOOST_REQUIRE(expected.type == result.type);
    BOOST_REQUIRE(expected.references.size() == result.references.size());
}
