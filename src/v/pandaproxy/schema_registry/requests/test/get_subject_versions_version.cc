// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/requests/get_subject_versions_version.h"

#include "base/seastarx.h"
#include "pandaproxy/json/rjson_util.h"

#include <seastar/testing/thread_test_case.hh>

namespace ppj = pandaproxy::json;
namespace pps = pandaproxy::schema_registry;

SEASTAR_THREAD_TEST_CASE(test_post_subject_versions_version_response) {
    const ss::sstring escaped_schema_def{
      R"({\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"type\":\"string\",\"name\":\"field1\"},{\"type\":\"com.acme.Referenced\",\"name\":\"int\"}]})"};
    const pps::canonical_schema_definition schema_def{
      R"({"type":"record","name":"test","fields":[{"type":"string","name":"field1"},{"type":"com.acme.Referenced","name":"int"}]})",
      pps::schema_type::avro,
      {{{"com.acme.Referenced"},
        pps::subject{"childSubject"},
        pps::schema_version{1}}}};
    const pps::subject sub{"imported-ref"};

    pps::post_subject_versions_version_response response{
      .schema{pps::subject{"imported-ref"}, schema_def.copy()},
      .id{12},
      .version{2}};

    const ss::sstring expected{
      R"(
{
  "subject": "imported-ref",
  "version": 2,
  "id": 12,
  "references": [
    {
       "name": "com.acme.Referenced",
       "subject":  "childSubject",
       "version": 1
    }
  ],
  "schema": ")"
      + escaped_schema_def + R"("})"};

    auto result = ppj::rjson_serialize_str(response);

    BOOST_REQUIRE_EQUAL(::json::minify(expected), result);
}
