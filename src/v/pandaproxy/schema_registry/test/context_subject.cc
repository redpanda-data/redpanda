// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/types.h"

#include <gtest/gtest.h>

namespace pandaproxy::schema_registry {

class ContextSubjectTest : public ::testing::Test {
protected:
    void SetUp() override { enable_qualified_subjects::set_local(true); }
    void TearDown() override { enable_qualified_subjects::reset_local(); }
};

TEST_F(ContextSubjectTest, FromString) {
    // Unqualified subjects use default context
    EXPECT_EQ(
      context_subject::from_string("my-topic"),
      (context_subject{default_context, subject{"my-topic"}}));

    // Qualified syntax: ":.context:subject"
    EXPECT_EQ(
      context_subject::from_string(":.my-ctx:my-topic"),
      (context_subject{context{".my-ctx"}, subject{"my-topic"}}));

    // Explicit default context ":.:subject"
    EXPECT_EQ(
      context_subject::from_string(":.:my-topic"),
      (context_subject{default_context, subject{"my-topic"}}));

    // Context-only form ":.ctx:" (empty subject, used for context-level config)
    EXPECT_EQ(
      context_subject::from_string(":.my-ctx:"),
      (context_subject{context{".my-ctx"}, subject{""}}));

    // Colons in subject after context are preserved
    EXPECT_EQ(
      context_subject::from_string(":.ctx:a:b:c"),
      (context_subject{context{".ctx"}, subject{"a:b:c"}}));

    // Invalid qualified syntax falls back to unqualified
    EXPECT_EQ(
      context_subject::from_string(":no-dot"),
      (context_subject{default_context, subject{":no-dot"}}));
    EXPECT_EQ(
      context_subject::from_string(":.no-second-colon"),
      (context_subject{default_context, subject{":.no-second-colon"}}));
}

TEST_F(ContextSubjectTest, ToStringAndRoundTrip) {
    // Default context: just the subject
    auto unqualified = context_subject{default_context, subject{"my-topic"}};
    EXPECT_EQ(unqualified.to_string(), "my-topic");
    EXPECT_EQ(
      context_subject::from_string(unqualified.to_string()), unqualified);

    // Non-default context: qualified format
    auto qualified = context_subject{context{".my-ctx"}, subject{"my-topic"}};
    EXPECT_EQ(qualified.to_string(), ":.my-ctx:my-topic");
    EXPECT_EQ(context_subject::from_string(qualified.to_string()), qualified);

    // Context-only (empty subject): qualified format
    auto ctx_only = context_subject{context{".my-ctx"}, subject{""}};
    EXPECT_EQ(ctx_only.to_string(), ":.my-ctx:");
    EXPECT_EQ(context_subject::from_string(ctx_only.to_string()), ctx_only);
}

TEST_F(ContextSubjectTest, FlagOffTreatsQualifiedAsLiteral) {
    enable_qualified_subjects::reset_local();
    enable_qualified_subjects::set_local(false);

    auto ctx_sub = context_subject::from_string(":.myctx:my-topic");

    // With flag off, the entire string is the subject in default context
    EXPECT_EQ(ctx_sub.ctx, default_context);
    EXPECT_EQ(ctx_sub.sub(), ":.myctx:my-topic");
}

TEST_F(ContextSubjectTest, FlagOnParsesQualifiedSyntax) {
    auto ctx_sub = context_subject::from_string(":.myctx:my-topic");

    // With flag on, qualified syntax is parsed
    EXPECT_EQ(ctx_sub.ctx(), ".myctx");
    EXPECT_EQ(ctx_sub.sub(), "my-topic");
}

TEST_F(ContextSubjectTest, FlagOnUnqualifiedUsesDefaultContext) {
    auto ctx_sub = context_subject::from_string("plain-topic");

    EXPECT_EQ(ctx_sub.ctx, default_context);
    EXPECT_EQ(ctx_sub.sub(), "plain-topic");
}

TEST_F(ContextSubjectTest, FlagOffUnqualifiedUsesDefaultContext) {
    enable_qualified_subjects::reset_local();
    enable_qualified_subjects::set_local(false);

    auto ctx_sub = context_subject::from_string("plain-topic");

    EXPECT_EQ(ctx_sub.ctx, default_context);
    EXPECT_EQ(ctx_sub.sub(), "plain-topic");
}

} // namespace pandaproxy::schema_registry
