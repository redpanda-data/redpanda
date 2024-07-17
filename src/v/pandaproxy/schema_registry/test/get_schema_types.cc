// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "http/client.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"

#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/test/tools/old/interface.hpp>

namespace pp = pandaproxy;
namespace ppj = pp::json;

FIXTURE_TEST(schema_registry_get_schema_types, pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    info("Connecting client");
    auto client = make_schema_reg_client();

    {
        info("List schema types (no headers)");
        auto res = http_request(client, "/schemas/types");
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(res.body, R"(["JSON","PROTOBUF","AVRO"])");
        BOOST_REQUIRE_EQUAL(
          res.headers.at(boost::beast::http::field::content_type),
          to_header_value(ppj::serialization_format::schema_registry_v1_json));
    }

    {
        info("List schema types (accept schema_registry_json)");
        auto res = http_request(
          client,
          "/schemas/types",
          boost::beast::http::verb::get,
          ppj::serialization_format::schema_registry_json,
          ppj::serialization_format::schema_registry_json);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(res.body, R"(["JSON","PROTOBUF","AVRO"])");
        BOOST_REQUIRE_EQUAL(
          res.headers.at(boost::beast::http::field::content_type),
          to_header_value(ppj::serialization_format::schema_registry_json));
    }
}
