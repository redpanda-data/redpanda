// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/test/avro_payloads.h"
#include "pandaproxy/schema_registry/test/client_utils.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"

namespace pp = pandaproxy;
namespace ppj = pp::json;
namespace pps = pp::schema_registry;

FIXTURE_TEST(
  schema_registry_post_subjects_subject_version, pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    info("Connecting client");
    auto client = make_schema_reg_client();

    {
        info("Post a schema as key (expect schema_id=1)");
        auto res = post_schema(
          client, pps::subject{"test-key"}, avro_int_payload);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(res.body, R"({"id":1})");
        BOOST_REQUIRE_EQUAL(
          res.headers.at(boost::beast::http::field::content_type),
          to_header_value(ppj::serialization_format::schema_registry_v1_json));
    }

    {
        info("Repost a schema as key (expect schema_id=1)");
        auto res = post_schema(
          client, pps::subject{"test-key"}, avro_int_payload);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(res.body, R"({"id":1})");
        BOOST_REQUIRE_EQUAL(
          res.headers.at(boost::beast::http::field::content_type),
          to_header_value(ppj::serialization_format::schema_registry_v1_json));
    }

    {
        info("Repost a schema as value (expect schema_id=1)");
        auto res = post_schema(
          client, pps::subject{"test-value"}, avro_int_payload);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(res.body, R"({"id":1})");
        BOOST_REQUIRE_EQUAL(
          res.headers.at(boost::beast::http::field::content_type),
          to_header_value(ppj::serialization_format::schema_registry_v1_json));
    }

    {
        info("Post a new schema as key (expect schema_id=2)");
        auto res = post_schema(
          client, pps::subject{"test-key"}, avro_long_payload);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(res.body, R"({"id":2})");
        BOOST_REQUIRE_EQUAL(
          res.headers.at(boost::beast::http::field::content_type),
          to_header_value(ppj::serialization_format::schema_registry_v1_json));
    }
}

FIXTURE_TEST(
  schema_registry_post_subjects_subject_version_invalid_payload,
  pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    info("Connecting client");
    auto client = make_schema_reg_client();

    {
        info("Post an invalid payload");
        auto res = post_schema(client, pps::subject{"test-key"}, R"(
{
  "schema": "\"int\"",
  "InvalidField": "AVRO"
})");

        BOOST_REQUIRE_EQUAL(
          res.headers.result(),
          boost::beast::http::status::unprocessable_entity);
        BOOST_REQUIRE(std::string_view(res.body).starts_with(
          R"({"error_code":422,"message":")"));
    }
}

FIXTURE_TEST(
  schema_registry_post_subjects_subject_version_invalid_schema,
  pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    info("Connecting client");
    auto client = make_schema_reg_client();

    {
        info("Post an invalid schema");
        auto res = post_schema(client, pps::subject{"test-key"}, R"(
{
  "schema": "not_json",
  "schemaType": "AVRO"
})");

        BOOST_REQUIRE_EQUAL(
          res.headers.result(),
          boost::beast::http::status::unprocessable_entity);
        BOOST_REQUIRE(std::string_view(res.body).starts_with(
          R"({"error_code":422,"message":")"));
    }
}

FIXTURE_TEST(
  schema_registry_post_subjects_subject_version_many_proto,
  pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    info("Connecting client");
    auto client = make_schema_reg_client();

    for (auto i = 0; i < 10; ++i) {
        info("Post a schema as key (expect schema_id=1)");
        auto res = post_schema(client, pps::subject{"test-key"}, R"(
{
  "schema": "syntax = \"proto3\"; message Simple { string i = 1; }",
  "schemaType": "PROTOBUF"
})");
        BOOST_REQUIRE_EQUAL(res.body, R"({"id":1})");
        BOOST_REQUIRE_EQUAL(
          res.headers.at(boost::beast::http::field::content_type),
          to_header_value(ppj::serialization_format::schema_registry_v1_json));
    }
}
