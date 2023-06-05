// Copyright 2020 Redpanda Data, Inc.
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

#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/test/tools/old/interface.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

FIXTURE_TEST(test_delete_subject, pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    info("Connecting client");
    auto client = make_schema_reg_client();

    {
        info("Post a schema as key");
        auto res = post_schema(
          client, pps::subject{"test-key"}, avro_int_payload);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
    }

    {
        info("Expect version 1");
        auto res = get_subject_versions(client, pps::subject{"test-key"});
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);

        std::vector<pps::schema_version> expected{pps::schema_version{1}};
        auto versions = get_body_versions(res.body);
        BOOST_REQUIRE_EQUAL(versions, expected);
    }

    {
        info("Delete version 1");
        auto res = delete_subject_version(
          client, pps::subject{"test-key"}, pps::schema_version{1});
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
    }

    {
        info("Expect subject not_found");
        auto res = get_subject_versions(client, pps::subject{"test-key"});
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::not_found);
    }

    {
        info("Expect deleted version 1");
        auto res = get_subject_versions(
          client, pps::subject{"test-key"}, pps::include_deleted::yes);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);

        std::vector<pps::schema_version> expected{pps::schema_version{1}};
        auto versions = get_body_versions(res.body);
        BOOST_REQUIRE_EQUAL(versions, expected);
    }

    {
        info("Hard delete version 1");
        auto res = delete_subject_version(
          client,
          pps::subject{"test-key"},
          pps::schema_version{1},
          pps::permanent_delete::yes);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
    }

    {
        info("Expect sub not found");
        auto res = get_subject_versions(
          client, pps::subject{"test-key"}, pps::include_deleted::yes);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::not_found);
    }

    {
        info("Repost a schema as key");
        auto res = post_schema(
          client, pps::subject{"test-key"}, avro_int_payload);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
    }

    {
        info("Expect version 1");
        auto res = get_subject_versions(client, pps::subject{"test-key"});
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);

        std::vector<pps::schema_version> expected{pps::schema_version{1}};
        auto versions = get_body_versions(res.body);
        BOOST_REQUIRE_EQUAL(versions, expected);
    }

    {
        info("Delete subject");
        auto res = delete_subject(client, pps::subject{"test-key"});
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
    }
}

FIXTURE_TEST(test_delete_referenced_subject, pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    info("Connecting client");
    auto client = make_schema_reg_client();

    {
        info("Post a schema as key");
        auto res = post_schema(
          client, pps::subject{"int-key"}, avro_int_payload);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
    }

    {
        info("Expect version 1");
        auto res = get_subject_versions(client, pps::subject{"int-key"});
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);

        std::vector<pps::schema_version> expected{pps::schema_version{1}};
        auto versions = get_body_versions(res.body);
        BOOST_REQUIRE_EQUAL(versions, expected);
    }

    {
        info("Post a schema as key, which references the first");
        auto res = post_schema(
          client,
          pps::subject{"reference-key"},
          ss::sstring{
            R"(
{
  "schema": "\"long\"",
  "schemaType": "AVRO",
  "references": [
    {
       "name": "com.acme.Referenced",
       "subject":  "int-key",
       "version": 1
    }
  ]
})"});
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
    }

    {
        info("Delete version 1, expect failure 42206");
        auto res = delete_subject_version(
          client, pps::subject{"int-key"}, pps::schema_version{1});
        BOOST_REQUIRE_EQUAL(
          res.headers.result(),
          boost::beast::http::status::unprocessable_entity);
        BOOST_REQUIRE_EQUAL(get_body_error_code(res.body), 42206);
    }

    {
        info("Delete subject int-key, expect failure 42206 (still referenced)");
        auto res = delete_subject(client, pps::subject{"int-key"});
        BOOST_REQUIRE_EQUAL(
          res.headers.result(),
          boost::beast::http::status::unprocessable_entity);
        BOOST_REQUIRE_EQUAL(get_body_error_code(res.body), 42206);
    }

    {
        info("Delete subject reference-key");
        auto res = delete_subject(client, pps::subject{"reference-key"});
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
    }

    {
        info("Delete subject int-key (not referenced - soft deleted)");
        auto res = delete_subject(client, pps::subject{"int-key"});
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
    }
}
