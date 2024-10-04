/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "http/rest_client/rest_entity.h"

#include <boost/test/unit_test.hpp>

namespace bh = boost::beast::http;

class resource : public http::rest_client::rest_entity {
public:
    explicit resource(std::string_view root)
      : http::rest_client::rest_entity(root) {}
    ss::sstring resource_name() const final { return "thing"; }
};

class override_verbs : public http::rest_client::rest_entity {
public:
    explicit override_verbs(std::string_view root)
      : http::rest_client::rest_entity(root) {}
    ss::sstring resource_name() const final { return "o"; }

    verb create_verb() const final { return verb::put; }
    verb update_verb() const final { return verb::patch; }
};

BOOST_AUTO_TEST_CASE(test_resource_url) {
    resource a{"foo"};
    BOOST_REQUIRE_EQUAL(a.resource_url(), "foo/thing");
    resource b{"foo/"};
    BOOST_REQUIRE_EQUAL(a.resource_url(), "foo/thing");
}

BOOST_AUTO_TEST_CASE(test_resource_list) {
    resource r{"x/v1/prefix"};
    auto b = r.with_host("http://x.y").list().build();
    BOOST_REQUIRE(b.has_value());
    auto request = b.value();
    BOOST_REQUIRE(request.method() == bh::verb::get);
    BOOST_REQUIRE_EQUAL(request.at(bh::field::host), "x.y");
    BOOST_REQUIRE_EQUAL(request.target(), "/x/v1/prefix/thing");
}

BOOST_AUTO_TEST_CASE(test_resource_optional_headers_and_qparams) {
    resource r{"x/v1/prefix"};
    auto b = r.with_host("http://x.y")
               .list({{{"x-1", "v"}}}, {{{"next", "22"}}})
               .build();
    BOOST_REQUIRE(b.has_value());
    auto request = b.value();
    BOOST_REQUIRE_EQUAL(request.at("x-1"), "v");
    BOOST_REQUIRE_EQUAL(request.target(), "/x/v1/prefix/thing?next=22");
}

BOOST_AUTO_TEST_CASE(test_resource_create) {
    resource r{"x/v1/prefix"};
    auto b = r.with_host("http://x.y").create().build();
    BOOST_REQUIRE(b.has_value());
    auto request = b.value();
    BOOST_REQUIRE(request.method() == bh::verb::post);
    BOOST_REQUIRE_EQUAL(request.at(bh::field::host), "x.y");
    BOOST_REQUIRE_EQUAL(request.target(), "/x/v1/prefix/thing");
}

BOOST_AUTO_TEST_CASE(test_resource_get) {
    resource r{"x/v1/"};
    auto b = r.with_host("http://x.y").get("id").build();
    BOOST_REQUIRE(b.has_value());
    auto request = b.value();
    BOOST_REQUIRE(request.method() == bh::verb::get);
    BOOST_REQUIRE_EQUAL(request.at(bh::field::host), "x.y");
    BOOST_REQUIRE_EQUAL(request.target(), "/x/v1/thing/id");
}

BOOST_AUTO_TEST_CASE(test_resource_delete) {
    resource r{"x"};
    auto b = r.with_host("http://x.y").delete_("id").build();
    BOOST_REQUIRE(b.has_value());
    auto request = b.value();
    BOOST_REQUIRE(request.method() == bh::verb::delete_);
    BOOST_REQUIRE_EQUAL(request.at(bh::field::host), "x.y");
    BOOST_REQUIRE_EQUAL(request.target(), "/x/thing/id");
}

BOOST_AUTO_TEST_CASE(test_resource_update) {
    resource r{"x"};
    auto b = r.with_host("http://x.y").update("id").build();
    BOOST_REQUIRE(b.has_value());
    auto request = b.value();
    BOOST_REQUIRE(request.method() == bh::verb::post);
    BOOST_REQUIRE_EQUAL(request.at(bh::field::host), "x.y");
    BOOST_REQUIRE_EQUAL(request.target(), "/x/thing/id");
}

BOOST_AUTO_TEST_CASE(test_override_update) {
    override_verbs r{"x"};
    auto b = r.with_host("http://x.y").update("id").build();
    BOOST_REQUIRE(b.has_value());
    auto request = b.value();
    BOOST_REQUIRE(request.method() == bh::verb::patch);
    BOOST_REQUIRE_EQUAL(request.at(bh::field::host), "x.y");
    BOOST_REQUIRE_EQUAL(request.target(), "/x/o/id");
}

BOOST_AUTO_TEST_CASE(test_override_create) {
    override_verbs r{"x"};
    auto b = r.with_host("http://x.y").create().build();
    BOOST_REQUIRE(b.has_value());
    auto request = b.value();
    BOOST_REQUIRE(request.method() == bh::verb::put);
    BOOST_REQUIRE_EQUAL(request.at(bh::field::host), "x.y");
    BOOST_REQUIRE_EQUAL(request.target(), "/x/o");
}
