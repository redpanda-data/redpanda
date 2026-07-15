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

namespace ppj = pandaproxy::json;
namespace pps = pandaproxy::schema_registry;

using parse_result
  = pps::post_subject_versions_request_handler<>::rjson_parse_result;

SEASTAR_THREAD_TEST_CASE(test_post_subject_versions_parser) {
    const ss::sstring escaped_schema_def{
      R"({\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"type\":\"string\",\"name\":\"field1\"},{\"type\":\"com.acme.Referenced\",\"name\":\"int\"}]})"};
    const pps::schema_definition expected_schema_def{
      R"({"type":"record","name":"test","fields":[{"type":"string","name":"field1"},{"type":"com.acme.Referenced","name":"int"}]})",
      pps::schema_type::avro,
      {{.name{"com.acme.Referenced"},
        .sub{pps::context_subject::unqualified("childSubject")},
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
    const auto sub = pps::context_subject::unqualified("test_subject");
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

// Records that the reader delivered the schema string through the chunked
// sink protocol rather than the contiguous String() path.
struct probe_request_handler
  : public pps::post_subject_versions_request_handler<> {
    probe_request_handler(pps::context_subject sub, bool* chunked_string_used)
      : pps::post_subject_versions_request_handler<>{std::move(sub)}
      , chunked_string_used{chunked_string_used} {}

    bool ChunkedString(::json::SizeType len) {
        *chunked_string_used = true;
        return pps::post_subject_versions_request_handler<>::ChunkedString(len);
    }

    bool* chunked_string_used;
};

// A large schema is decoded by the reader directly into an iobuf-backed sink
// (avoiding a large contiguous allocation); verify escape sequences and
// multi-fragment payloads round-trip through that path.
SEASTAR_THREAD_TEST_CASE(test_post_subject_versions_large_schema) {
    constexpr size_t num_reps = 100000;
    // built via std::string, whose amortized append avoids the quadratic
    // copying of repeated ss::sstring::operator+=
    std::string escaped_schema_def{R"({\"doc\":\")"};
    escaped_schema_def.reserve(32 * num_reps);
    std::string expected_schema_def{R"({"doc":")"};
    expected_schema_def.reserve(16 * num_reps);
    for (size_t i = 0; i < num_reps; ++i) {
        escaped_schema_def += R"(x\n\u00e9\ud83d\ude00)";
        expected_schema_def += "x\n\xc3\xa9\xf0\x9f\x98\x80";
    }
    escaped_schema_def += R"(\"})";
    expected_schema_def += R"("})";

    const std::string payload{R"({"schema": ")" + escaped_schema_def + R"("})"};
    const auto sub = pps::context_subject::unqualified("test_subject");

    {
        bool chunked_string_used = false;
        auto result{ppj::impl::rjson_parse(
          payload.data(), probe_request_handler{sub, &chunked_string_used})};
        BOOST_REQUIRE(chunked_string_used);
        BOOST_REQUIRE(
          result.def.def().raw()() == std::string_view{expected_schema_def});
    }
    {
        iobuf buf;
        buf.append(payload.data(), payload.size());
        bool chunked_string_used = false;
        auto result{ppj::rjson_parse(
          std::move(buf), probe_request_handler{sub, &chunked_string_used})};
        BOOST_REQUIRE(chunked_string_used);
        BOOST_REQUIRE(
          result.def.def().raw()() == std::string_view{expected_schema_def});
    }

    // parse errors inside the schema string are still detected
    constexpr std::string_view bad_escape{R"({"schema": "\q"})"};
    BOOST_REQUIRE_THROW(
      ppj::impl::rjson_parse(
        bad_escape.data(), pps::post_subject_versions_request_handler{sub}),
      ppj::parse_error);
}

BOOST_AUTO_TEST_CASE(test_post_subject_versions_serde_metadata) {
    const auto sub = pps::context_subject::unqualified("test_subject");
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
