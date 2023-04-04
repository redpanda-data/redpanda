// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/requests/post_subject_versions.h"

#include "pandaproxy/schema_registry/types.h"
#include "seastarx.h"

#include <seastar/testing/thread_test_case.hh>

#include <fmt/ostream.h>

#include <type_traits>

namespace ppj = pandaproxy::json;
namespace pps = pandaproxy::schema_registry;

using parse_result
  = pps::post_subject_versions_request_handler<>::rjson_parse_result;

SEASTAR_THREAD_TEST_CASE(test_post_subject_versions_parser) {
    const ss::sstring escaped_schema_def{
      R"({\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"type\":\"string\",\"name\":\"field1\"},{\"type\":\"com.acme.Referenced\",\"name\":\"int\"}]})"};
    const pps::unparsed_schema_definition expected_schema_def{
      R"({"type":"record","name":"test","fields":[{"type":"string","name":"field1"},{"type":"com.acme.Referenced","name":"int"}]})",
      pps::schema_type::avro};

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
    const parse_result expected{
      {sub,
       expected_schema_def,
       {{.name{"com.acme.Referenced"},
         .sub{pps::subject{"childSubject"}},
         .version{pps::schema_version{1}}}}},
      std::nullopt,
      std::nullopt};

    auto result{ppj::rjson_parse(
      payload.data(), pps::post_subject_versions_request_handler{sub})};

    // canonicalisation now requires a sharded_store, for now, minify.
    // NOLINTBEGIN(bugprone-use-after-move)
    result.def = {
      std::move(result.def).sub(),
      pps::unparsed_schema_definition{
        ppj::minify(result.def.def().raw()()), pps::schema_type::avro},
      std::move(result.def).refs()};
    // NOLINTEND(bugprone-use-after-move)

    BOOST_REQUIRE_EQUAL(expected.def, result.def);
    BOOST_REQUIRE_EQUAL(expected.id.has_value(), result.id.has_value());
    if (expected.id.has_value()) {
        BOOST_REQUIRE_EQUAL(*expected.id, *result.id);
    }
    BOOST_REQUIRE_EQUAL(
      expected.version.has_value(), result.version.has_value());
    if (expected.version.has_value()) {
        BOOST_REQUIRE_EQUAL(*expected.version, *result.version);
    }
}
