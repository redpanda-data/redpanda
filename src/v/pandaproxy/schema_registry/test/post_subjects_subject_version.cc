// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/error.h"
#include "pandaproxy/json/error.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/test/avro_payloads.h"
#include "pandaproxy/schema_registry/test/client_utils.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"

#include <array>

namespace pp = pandaproxy;
namespace ppj = pp::json;
namespace pps = pp::schema_registry;

struct request {
    pps::canonical_schema schema;
};

void rjson_serialize(
  ::json::Writer<::json::StringBuffer>& w, const request& r) {
    w.StartObject();
    w.Key("schema");
    ::json::rjson_serialize(w, r.schema.def().raw());
    if (r.schema.type() != pps::schema_type::avro) {
        w.Key("schemaType");
        ::json::rjson_serialize(w, to_string_view(r.schema.type()));
    }
    if (!r.schema.refs().empty()) {
        w.Key("references");
        w.StartArray();
        for (const auto& ref : r.schema.refs()) {
            w.StartObject();
            w.Key("name");
            ::json::rjson_serialize(w, ref.name);
            w.Key("subject");
            ::json::rjson_serialize(w, ref.sub);
            w.Key("version");
            ::json::rjson_serialize(w, ref.version);
            w.EndObject();
        }
        w.EndArray();
    }
    w.EndObject();
}

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
  schema_registry_post_subjects_subject_version_with_id,
  pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    info("Connecting client");
    auto client = make_schema_reg_client();

    const ss::sstring schema_2{
      R"({
  "schema": "\"string\"",
  "id": 2,
  "schemaType": "AVRO"
})"};

    const ss::sstring schema_4{
      R"({
  "schema": "\"int\"",
  "id": 4,
  "schemaType": "AVRO"
})"};

    const ss::sstring schema_4_as_2{
      R"({
  "schema": "\"int\"",
  "id": 2,
  "schemaType": "AVRO"
})"};

    const pps::subject subject{"test-key"};
    put_config(client, subject, pps::compatibility_level::none);

    {
        info("Post schema 4 as key with id 4 (higher than next_id)");
        auto res = post_schema(client, subject, schema_4);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(res.body, R"({"id":4})");

        res = get_subject_versions(client, subject);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);

        std::vector<pps::schema_version> expected{pps::schema_version{1}};
        auto versions = get_body_versions(res.body);
        BOOST_REQUIRE_EQUAL(versions, expected);
    }

    {
        info("Post schema 2 as key with id 2 (lower than next id)");
        auto res = post_schema(client, subject, schema_2);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(res.body, R"({"id":2})");

        res = get_subject_versions(client, subject);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);

        std::vector<pps::schema_version> expected{
          pps::schema_version{1}, pps::schema_version{2}};
        auto versions = get_body_versions(res.body);
        BOOST_REQUIRE_EQUAL(versions, expected);
    }

    {
        info("Post schema 4 as key with id 2 (expect error 42207)");
        auto res = post_schema(client, subject, schema_4_as_2);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(),
          boost::beast::http::status::unprocessable_entity);
        auto eb = get_error_body(res.body);
        BOOST_REQUIRE(
          eb.ec
          == pp::reply_error_code::subject_version_schema_id_already_exists);
        BOOST_REQUIRE_EQUAL(
          eb.message,
          R"(Schema already registered with id 4 instead of input id 2)");
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

FIXTURE_TEST(schema_registry_post_avro_references, pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    const auto company_req = request{pps::canonical_schema{
      pps::subject{"company-value"},
      pps::canonical_schema_definition(
        R"({
  "namespace": "com.redpanda",
  "type": "record",
  "name": "company",
  "fields": [
    {
      "name": "id",
      "type": "string"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "address",
      "type": "string"
    }
  ]
})",
        pps::schema_type::avro)}};

    const auto employee_req = request{pps::canonical_schema{
      pps::subject{"employee-value"},
      pps::canonical_schema_definition(
        R"({
  "namespace": "com.redpanda",
  "type": "record",
  "name": "employee",
  "fields": [
    {
      "name": "id",
      "type": "string"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "company",
      "type": "com.redpanda.company"
    }
  ]
})",
        pps::schema_type::avro),
      {{"com.redpanda.company",
        pps::subject{"company-value"},
        pps::schema_version{1}}}}};

    info("Connecting client");
    auto client = make_schema_reg_client();

    info("Post company schema (expect schema_id=1)");
    auto res = post_schema(
      client, company_req.schema.sub(), ppj::rjson_serialize(company_req));
    BOOST_REQUIRE_EQUAL(res.body, R"({"id":1})");
    BOOST_REQUIRE_EQUAL(
      res.headers.at(boost::beast::http::field::content_type),
      to_header_value(ppj::serialization_format::schema_registry_v1_json));

    info("Post employee schema (expect schema_id=2)");
    res = post_schema(
      client, employee_req.schema.sub(), ppj::rjson_serialize(employee_req));
    BOOST_REQUIRE_EQUAL(res.body, R"({"id":2})");
    BOOST_REQUIRE_EQUAL(
      res.headers.at(boost::beast::http::field::content_type),
      to_header_value(ppj::serialization_format::schema_registry_v1_json));
}
