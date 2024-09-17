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

#include "http/request_builder.h"

#include <boost/test/unit_test.hpp>

namespace bh = boost::beast::http;

BOOST_AUTO_TEST_CASE(test_default_build_is_error) {
    http::request_builder rb;
    auto req = rb.build();
    BOOST_REQUIRE(!req.has_value());
    BOOST_REQUIRE(req.error() == rb.default_state);
}

BOOST_AUTO_TEST_CASE(test_with_everything) {
    http::request_builder rb;
    auto req = rb.host("https://foobar.xyz")
                 .target("san_mig")
                 .with_bearer_auth("secret")
                 .with_content_type("obsidian")
                 .with_size_of(iobuf::from("a"))
                 .header("one", "1")
                 .headers({{"two", "2"}, {"three", "3"}})
                 .method(boost::beast::http::verb::delete_)
                 .query_param("here")
                 .query_param_kv("name", "no")
                 .query_params({"g", "b", "u"})
                 .query_params_kv({{"trilogy", "of"}, {"containers", "four"}})
                 .build();
    BOOST_REQUIRE(req.has_value());
    const auto built = req.value();
    BOOST_REQUIRE_EQUAL(built.at(bh::field::host), "foobar.xyz");
    BOOST_REQUIRE_EQUAL(built.at(bh::field::authorization), "Bearer secret");
    BOOST_REQUIRE_EQUAL(built.at(bh::field::content_length), "1");
    BOOST_REQUIRE_EQUAL(built.at(bh::field::content_type), "obsidian");
    BOOST_REQUIRE_EQUAL(built.at("one"), "1");
    BOOST_REQUIRE_EQUAL(built.at("two"), "2");
    BOOST_REQUIRE_EQUAL(built.at("three"), "3");
    BOOST_REQUIRE(built.method() == bh::verb::delete_);

    const auto target = built.target();
    for (const auto& qp :
         {"here", "g", "b", "u", "name=no", "trilogy=of", "containers=four"}) {
        BOOST_REQUIRE(
          target.contains(fmt::format("&{}", qp))
          || target.contains(fmt::format("?{}", qp)));
    }

    BOOST_REQUIRE(target.starts_with("/san_mig?"));
    BOOST_REQUIRE(!target.ends_with("&"));

    std::string sorted{target};
    std::string expected{
      "/san_mig?here&g&b&u&name=no&trilogy=of&containers=four"};
    std::ranges::sort(sorted);
    std::ranges::sort(expected);
    BOOST_REQUIRE_EQUAL(sorted, expected);
}

BOOST_AUTO_TEST_CASE(test_invalid_host) {
    http::request_builder rb;
    const auto bad_host = "foobar.xyz";
    auto req = rb.host(bad_host).build();
    BOOST_REQUIRE(!req.has_value());
    BOOST_REQUIRE(
      req.error() == fmt::format("failed to parse host: {}", bad_host));
}
