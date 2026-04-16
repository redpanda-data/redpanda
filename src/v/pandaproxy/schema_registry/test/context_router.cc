// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/context_router.h"

#include "pandaproxy/schema_registry/exceptions.h"

#include <seastar/http/request.hh>

#include <gtest/gtest.h>

namespace pandaproxy::schema_registry {

TEST(ContextRouterTest, StartsWithContextDefault) {
    EXPECT_FALSE(starts_with_context(""));
    EXPECT_FALSE(starts_with_context("my-topic"));
    EXPECT_FALSE(starts_with_context("plain-subject"));
}

TEST(ContextRouterTest, StartsWithContextQualified) {
    EXPECT_TRUE(starts_with_context(":.staging:my-topic"));
    EXPECT_TRUE(starts_with_context(":.prod:"));
    EXPECT_TRUE(starts_with_context(":.:my-topic"));
}

TEST(ContextRouterTest, StartsWithContextWildcard) {
    EXPECT_TRUE(starts_with_context(":*:"));
    EXPECT_TRUE(starts_with_context(":*:my-topic"));
}

TEST(ContextRouterTest, StartsWithContextEdgeCases) {
    EXPECT_FALSE(starts_with_context(":"));
    EXPECT_FALSE(starts_with_context(":foo"));
    EXPECT_FALSE(starts_with_context(":*"));
}

TEST(ContextRouterTest, NormalizeContextWithDot) {
    EXPECT_EQ(normalize_context(".staging"), ".staging");
    EXPECT_EQ(normalize_context(".prod"), ".prod");
    EXPECT_EQ(normalize_context("."), ".");
}

TEST(ContextRouterTest, NormalizeContextWithoutDot) {
    EXPECT_EQ(normalize_context("staging"), ".staging");
    EXPECT_EQ(normalize_context("prod"), ".prod");
    EXPECT_EQ(normalize_context(""), ".");
}

TEST(ContextRouterTest, NormalizeContextStripColons) {
    EXPECT_EQ(normalize_context(":.staging"), ".staging");
    EXPECT_EQ(normalize_context(".staging:"), ".staging");
    EXPECT_EQ(normalize_context(":.staging:"), ".staging");
    EXPECT_EQ(normalize_context(":staging:"), ".staging");
}

TEST(ContextRouterTest, NormalizeContextRejectsEmbeddedColons) {
    EXPECT_THROW(normalize_context(".:."), exception);
    EXPECT_THROW(normalize_context("a:b"), exception);
    EXPECT_THROW(normalize_context(":.a:b:"), exception);
}

} // namespace pandaproxy::schema_registry
