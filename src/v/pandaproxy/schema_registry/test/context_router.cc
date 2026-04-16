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

TEST(ContextRouterTest, ScopeSubjectParam) {
    ss::http::request req;
    req.param.set("subject", "/my-topic");
    scope_subject_param(req, ".staging");
    EXPECT_EQ(req.get_path_param("subject"), ":.staging:my-topic");
}

TEST(ContextRouterTest, ScopeSubjectParamAlreadyQualified) {
    ss::http::request req;
    req.param.set("subject", "/:.prod:my-topic");
    scope_subject_param(req, ".staging");
    EXPECT_EQ(req.get_path_param("subject"), ":.prod:my-topic");
}

TEST(ContextRouterTest, ScopeSubjectParamWildcard) {
    ss::http::request req;
    req.param.set("subject", "/:*:my-topic");
    scope_subject_param(req, ".staging");
    EXPECT_EQ(req.get_path_param("subject"), ":*:my-topic");
}

TEST(ContextRouterTest, ScopeSubjectParamNoDot) {
    ss::http::request req;
    req.param.set("subject", "/my-topic");
    scope_subject_param(req, "staging");
    EXPECT_EQ(req.get_path_param("subject"), ":.staging:my-topic");
}

TEST(ContextRouterTest, ScopeSubjectQueryAbsent) {
    ss::http::request req;
    scope_subject_query(req, ".staging");
    EXPECT_EQ(req.get_query_param("subject"), ":.staging:");
}

TEST(ContextRouterTest, ScopeSubjectQueryPlain) {
    ss::http::request req;
    req.set_query_param("subject", "my-topic");
    scope_subject_query(req, ".staging");
    EXPECT_EQ(req.get_query_param("subject"), ":.staging:my-topic");
}

TEST(ContextRouterTest, ScopeSubjectQueryAlreadyQualified) {
    ss::http::request req;
    req.set_query_param("subject", ":.prod:my-topic");
    scope_subject_query(req, ".staging");
    EXPECT_EQ(req.get_query_param("subject"), ":.prod:my-topic");
}

TEST(ContextRouterTest, ScopeSubjectQueryNoDot) {
    ss::http::request req;
    scope_subject_query(req, "staging");
    EXPECT_EQ(req.get_query_param("subject"), ":.staging:");
}

TEST(ContextRouterTest, ScopeSubjectPrefixQueryAbsent) {
    ss::http::request req;
    scope_subject_prefix_query(req, ".staging");
    EXPECT_EQ(req.get_query_param("subjectPrefix"), ":.staging:");
}

TEST(ContextRouterTest, ScopeSubjectPrefixQueryPlain) {
    ss::http::request req;
    req.set_query_param("subjectPrefix", "my-");
    scope_subject_prefix_query(req, ".staging");
    EXPECT_EQ(req.get_query_param("subjectPrefix"), ":.staging:my-");
}

TEST(ContextRouterTest, ScopeSubjectPrefixQueryAlreadyQualified) {
    ss::http::request req;
    req.set_query_param("subjectPrefix", ":.prod:");
    scope_subject_prefix_query(req, ".staging");
    EXPECT_EQ(req.get_query_param("subjectPrefix"), ":.prod:");
}

TEST(ContextRouterTest, ScopeSubjectPrefixQueryNoDot) {
    ss::http::request req;
    scope_subject_prefix_query(req, "staging");
    EXPECT_EQ(req.get_query_param("subjectPrefix"), ":.staging:");
}

TEST(ContextRouterTest, InjectContextAsSubject) {
    ss::http::request req;
    inject_context_as_subject(req, ".staging");
    EXPECT_EQ(req.get_path_param("subject"), ":.staging:");
}

TEST(ContextRouterTest, InjectContextAsSubjectNoDot) {
    ss::http::request req;
    inject_context_as_subject(req, "staging");
    EXPECT_EQ(req.get_path_param("subject"), ":.staging:");
}

} // namespace pandaproxy::schema_registry
