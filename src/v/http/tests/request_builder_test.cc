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

#include <gtest/gtest.h>

namespace bh = boost::beast::http;

TEST(request_builder, test_default_build_is_error) {
    http::request_builder rb;
    auto req = rb.build();
    ASSERT_TRUE(!req.has_value());
    ASSERT_TRUE(req.error() == rb.default_state);
}

TEST(request_builder, test_with_everything) {
    http::request_builder rb;
    auto req = rb.host("https://foobar.xyz")
                 .path("san_mig")
                 .with_bearer_auth("secret")
                 .with_content_type("obsidian")
                 .with_content_length(1)
                 .header("one", "1")
                 .headers({{"two", "2"}, {"three", "3"}})
                 .method(boost::beast::http::verb::delete_)
                 .query_param("here")
                 .query_param_kv("name", "no")
                 .query_params({"g", "b", "u"})
                 .query_params_kv({{"trilogy", "of"}, {"containers", "four"}})
                 .build();
    ASSERT_TRUE(req.has_value());
    const auto& built = req.value();
    using enum bh::field;
    EXPECT_EQ(built[host], "foobar.xyz");
    EXPECT_EQ(built[authorization], "Bearer secret");
    EXPECT_EQ(built[content_length], "1");
    EXPECT_EQ(built[content_type], "obsidian");
    EXPECT_EQ(built["one"], "1");
    EXPECT_EQ(built["two"], "2");
    EXPECT_EQ(built["three"], "3");
    EXPECT_EQ(built.method(), bh::verb::delete_);

    const auto target = built.target();
    for (const auto& qp :
         {"here", "g", "b", "u", "name=no", "trilogy=of", "containers=four"}) {
        ASSERT_TRUE(
          target.contains(fmt::format("&{}", qp))
          || target.contains(fmt::format("?{}", qp)));
    }

    ASSERT_TRUE(target.starts_with("/san_mig?"));
    ASSERT_TRUE(!target.ends_with("&"));

    std::string sorted{target};
    std::string expected{
      "/san_mig?here&g&b&u&name=no&trilogy=of&containers=four"};
    std::ranges::sort(sorted);
    std::ranges::sort(expected);
    EXPECT_EQ(sorted, expected);
}

TEST(request_builder, test_invalid_host) {
    http::request_builder rb;
    const auto bad_host = "foobar.xyz";
    auto req = rb.host(bad_host).build();
    ASSERT_TRUE(!req.has_value());
    ASSERT_TRUE(
      req.error() == fmt::format("failed to parse host: {}", bad_host));
}

TEST(request_builder, test_build_without_host_is_error) {
    http::request_builder rb;
    auto req = rb.path("foo").build();
    ASSERT_TRUE(!req.has_value());
    EXPECT_EQ(req.error(), rb.default_state);
}

TEST(request_builder, test_url_percent_encoding) {
    http::request_builder rb;
    const auto req = rb.host("http://a.b")
                       .query_param_kv(
                         "afgz0119!#$&'()*+,/:;=?@[]afgz0119",
                         "afgz0119!#$&'()*+,/:;=?@[]afgz0119")
                       .build();
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(
      req->target(),
      "/?afgz0119%21%23%24%26%27%28%29%2a%2b%2c%2F%3a%3b%3d%3f%"
      "40%5b%5dafgz0119=afgz0119%21%23%24%26%27%28%29%2a%2b%2c%2F%3a%3b%3d%"
      "3f%40%5b%5dafgz0119");
}
