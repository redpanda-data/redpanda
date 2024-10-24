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

#include <gtest/gtest.h>

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

TEST(rest_entity_test, test_resource_url) {
    resource a{"foo"};
    EXPECT_EQ(a.resource_url(), "foo/thing");
    resource b{"foo/"};
    EXPECT_EQ(a.resource_url(), "foo/thing");
}

TEST(rest_entity_test, test_resource_list) {
    resource r{"x/v1/prefix"};
    auto b = r.list().host("http://x.y").build();
    ASSERT_TRUE(b.has_value());
    const auto& request = b.value();
    ASSERT_TRUE(request.method() == bh::verb::get);
    EXPECT_EQ(request[bh::field::host], "x.y");
    EXPECT_EQ(request.target(), "/x/v1/prefix/thing");
}

TEST(rest_entity_test, test_resource_optional_headers_and_qparams) {
    resource r{"x/v1/prefix"};
    auto b
      = r.list({{{"x-1", "v"}}}, {{{"next", "22"}}}).host("http://x.y").build();
    ASSERT_TRUE(b.has_value());
    const auto& request = b.value();
    EXPECT_EQ(request["x-1"], "v");
    EXPECT_EQ(request.target(), "/x/v1/prefix/thing?next=22");
}

TEST(rest_entity_test, test_resource_create) {
    resource r{"x/v1/prefix"};
    auto b = r.create().host("http://x.y").build();
    ASSERT_TRUE(b.has_value());
    const auto& request = b.value();
    ASSERT_TRUE(request.method() == bh::verb::post);
    EXPECT_EQ(request[bh::field::host], "x.y");
    EXPECT_EQ(request.target(), "/x/v1/prefix/thing");
}

TEST(rest_entity_test, test_resource_get) {
    resource r{"x/v1/"};
    auto b = r.get("id").host("http://x.y").build();
    ASSERT_TRUE(b.has_value());
    const auto& request = b.value();
    ASSERT_TRUE(request.method() == bh::verb::get);
    EXPECT_EQ(request[bh::field::host], "x.y");
    EXPECT_EQ(request.target(), "/x/v1/thing/id");
}

TEST(rest_entity_test, test_resource_delete) {
    resource r{"x"};
    auto b = r.delete_("id").host("http://x.y").build();
    ASSERT_TRUE(b.has_value());
    const auto& request = b.value();
    ASSERT_TRUE(request.method() == bh::verb::delete_);
    EXPECT_EQ(request[bh::field::host], "x.y");
    EXPECT_EQ(request.target(), "/x/thing/id");
}

TEST(rest_entity_test, test_resource_update) {
    resource r{"x"};
    auto b = r.update("id").host("http://x.y").build();
    ASSERT_TRUE(b.has_value());
    const auto& request = b.value();
    ASSERT_TRUE(request.method() == bh::verb::post);
    EXPECT_EQ(request[bh::field::host], "x.y");
    EXPECT_EQ(request.target(), "/x/thing/id");
}

TEST(rest_entity_test, test_override_update) {
    override_verbs r{"x"};
    auto b = r.update("id").host("http://x.y").build();
    ASSERT_TRUE(b.has_value());
    const auto& request = b.value();
    ASSERT_TRUE(request.method() == bh::verb::patch);
    EXPECT_EQ(request[bh::field::host], "x.y");
    EXPECT_EQ(request.target(), "/x/o/id");
}

TEST(rest_entity_test, test_override_create) {
    override_verbs r{"x"};
    auto b = r.create().host("http://x.y").build();
    ASSERT_TRUE(b.has_value());
    const auto& request = b.value();
    ASSERT_TRUE(request.method() == bh::verb::put);
    EXPECT_EQ(request[bh::field::host], "x.y");
    EXPECT_EQ(request.target(), "/x/o");
}
