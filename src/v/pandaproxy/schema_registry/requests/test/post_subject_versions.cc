// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/requests/post_subject_versions.h"

#include "base/seastarx.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <fmt/ostream.h>

#include <type_traits>

namespace ppj = pandaproxy::json;
namespace pps = pandaproxy::schema_registry;

using parse_result
  = pps::post_subject_versions_request_handler<>::rjson_parse_result;

SEASTAR_THREAD_TEST_CASE(test_post_subject_versions_parser) {
    pps::enable_qualified_subjects::set_local(true);
    auto reset_flag = ss::defer(
      [] { pps::enable_qualified_subjects::reset_local(); });
    const ss::sstring escaped_schema_def{
      R"({\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"type\":\"string\",\"name\":\"field1\"},{\"type\":\"com.acme.Referenced\",\"name\":\"int\"}]})"};
    const pps::schema_definition expected_schema_def{
      R"({"type":"record","name":"test","fields":[{"type":"string","name":"field1"},{"type":"com.acme.Referenced","name":"int"}]})",
      pps::schema_type::avro,
      {{.name{"com.acme.Referenced"},
        .sub{pps::subject{"childSubject"}},
        .version{pps::schema_version{1}}}},
      {}};

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
      {sub, expected_schema_def.share()}, std::nullopt, std::nullopt};

    auto result{ppj::impl::rjson_parse(
      payload.data(), pps::post_subject_versions_request_handler{sub})};

    // canonicalisation now requires a sharded_store, for now, minify.
    auto [rsub, unparsed] = std::move(result.def).destructure();
    auto [def, type, refs, meta] = std::move(unparsed).destructure();

    result.def = {
      std::move(rsub),
      pps::schema_definition{
        pps::schema_definition::raw_string{::json::minify(std::move(def)())},
        pps::schema_type::avro,
        std::move(refs),
        std::move(meta)}};

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

BOOST_AUTO_TEST_CASE(test_post_subject_versions_serde_metadata) {
    pps::enable_qualified_subjects::set_local(true);
    auto reset_flag = ss::defer(
      [] { pps::enable_qualified_subjects::reset_local(); });

    const pps::subject sub{"test_subject"};
    {
        constexpr std::string_view no_metadata{
          R"({
  "schema": "{\"type\":\"string\"}"
})"};
        auto val = ppj::impl::rjson_parse(
          no_metadata.data(), pps::post_subject_versions_request_handler{sub});
        BOOST_CHECK(!val.def.def().meta().has_value());
    }
    {
        constexpr std::string_view null_metadata{
          R"({
  "metadata": null,
  "schema": "{\"type\":\"string\"}"
})"};
        auto val = ppj::impl::rjson_parse(
          null_metadata.data(),
          pps::post_subject_versions_request_handler{sub});
        BOOST_CHECK(!val.def.def().meta().has_value());
    }
    {
        constexpr std::string_view empty_metadata{
          R"({
  "metadata": {},
  "schema": "{\"type\":\"string\"}"
})"};
        auto val = ppj::impl::rjson_parse(
          empty_metadata.data(),
          pps::post_subject_versions_request_handler{sub});
        BOOST_CHECK(val.def.def().meta().has_value());
        BOOST_CHECK(!val.def.def().meta()->properties.has_value());
    }
    {
        constexpr std::string_view null_metadata_properties{
          R"({
  "metadata": {
    "properties": null
  },
  "schema": "{\"type\":\"string\"}"
})"};
        auto val = ppj::impl::rjson_parse(
          null_metadata_properties.data(),
          pps::post_subject_versions_request_handler{sub});
        BOOST_CHECK(val.def.def().meta().has_value());
        BOOST_CHECK(!val.def.def().meta()->properties.has_value());
    }
    {
        constexpr std::string_view empty_metadata_properties{
          R"({
  "metadata": {
    "properties": {}
  },
  "schema": "{\"type\":\"string\"}"
})"};
        auto val = ppj::impl::rjson_parse(
          empty_metadata_properties.data(),
          pps::post_subject_versions_request_handler{sub});
        BOOST_CHECK(val.def.def().meta().has_value());
        BOOST_CHECK(val.def.def().meta()->properties.has_value());
    }
    {
        constexpr std::string_view metadata_properties{
          R"({
  "metadata": {
    "properties": {
      "string": "value1",
      "int": -42,
      "uint": 42,
      "double": 3.14,
      "bool": true
    }
  },
  "schema": "{\"type\":\"string\"}"
})"};
        auto val = ppj::impl::rjson_parse(
          metadata_properties.data(),
          pps::post_subject_versions_request_handler{sub});
        BOOST_CHECK(val.def.def().meta().has_value());
        BOOST_CHECK(val.def.def().meta()->properties.has_value());
    }
}
