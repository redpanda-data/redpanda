// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
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

    // Invalid qualified syntax (no dot after colon) falls back to unqualified
    EXPECT_EQ(
      context_subject::from_string(":no-dot"),
      (context_subject{default_context, subject{":no-dot"}}));

    // Context-only form without trailing colon: ":.ctx" (empty subject)
    EXPECT_EQ(
      context_subject::from_string(":.no-second-colon"),
      (context_subject{context{".no-second-colon"}, subject{""}}));
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

TEST_F(ContextSubjectTest, ValidateSubjectRejectsReservedNames) {
    // __GLOBAL as subject is always rejected, regardless of context or mode
    EXPECT_THROW(
      validate_context_subject({default_context, subject{"__GLOBAL"}}),
      exception);
    EXPECT_THROW(
      validate_context_subject({default_context, subject{"__GLOBAL"}}, true),
      exception);
    EXPECT_THROW(
      validate_context_subject({context{".myctx"}, subject{"__GLOBAL"}}),
      exception);

    // __EMPTY as subject is always rejected
    EXPECT_THROW(
      validate_context_subject({default_context, subject{"__EMPTY"}}),
      exception);
    EXPECT_THROW(
      validate_context_subject({default_context, subject{"__EMPTY"}}, true),
      exception);
    EXPECT_THROW(
      validate_context_subject({context{".myctx"}, subject{"__EMPTY"}}),
      exception);

    // .__GLOBAL context is rejected on regular endpoints (with or without
    // subject)
    EXPECT_THROW(
      validate_context_subject({context{".__GLOBAL"}, subject{"some-subject"}}),
      exception);
    EXPECT_THROW(
      validate_context_subject({context{".__GLOBAL"}, subject{""}}), exception);

    // Verify the error code
    try {
        validate_context_subject({default_context, subject{"__GLOBAL"}});
        FAIL() << "Expected exception";
    } catch (const exception& e) {
        EXPECT_EQ(e.code(), error_code::subject_invalid);
    }
}

TEST_F(ContextSubjectTest, ValidateSubjectAllowsValidNames) {
    // Normal subjects
    EXPECT_NO_THROW(
      validate_context_subject({default_context, subject{"my-topic"}}));
    EXPECT_NO_THROW(
      validate_context_subject({context{".staging"}, subject{"my-topic"}}));
    EXPECT_NO_THROW(validate_context_subject(
      {default_context, subject{"some.subject.name"}}));

    // Empty subject (context-only form) is not "__EMPTY"
    EXPECT_NO_THROW(validate_context_subject({context{".myctx"}, subject{""}}));
    EXPECT_NO_THROW(validate_context_subject({default_context, subject{""}}));

    // Substrings of reserved names are valid
    EXPECT_NO_THROW(
      validate_context_subject({default_context, subject{"__GLOBAL_stuff"}}));
    EXPECT_NO_THROW(
      validate_context_subject({default_context, subject{"prefix__EMPTY"}}));
    EXPECT_NO_THROW(
      validate_context_subject({default_context, subject{"my__GLOBAL"}}));

    // Case-sensitive: lowercase variants are valid
    EXPECT_NO_THROW(
      validate_context_subject({default_context, subject{"__global"}}));
    EXPECT_NO_THROW(
      validate_context_subject({default_context, subject{"__empty"}}));
    EXPECT_NO_THROW(
      validate_context_subject({default_context, subject{"__Global"}}));
}

TEST_F(ContextSubjectTest, ValidateSubjectConfigModeFlag) {
    // .__GLOBAL context is allowed on config/mode endpoints
    EXPECT_NO_THROW(validate_context_subject(
      {context{".__GLOBAL"}, subject{"some-subject"}}, true));
    EXPECT_NO_THROW(
      validate_context_subject({context{".__GLOBAL"}, subject{""}}, true));

    // But reserved subject names are still rejected even on config/mode
    EXPECT_THROW(
      validate_context_subject(
        {context{".__GLOBAL"}, subject{"__GLOBAL"}}, true),
      exception);
    EXPECT_THROW(
      validate_context_subject(
        {context{".__GLOBAL"}, subject{"__EMPTY"}}, true),
      exception);
}

class ContextSubjectReferenceTest : public ::testing::Test {
protected:
    void SetUp() override { enable_qualified_subjects::set_local(true); }
    void TearDown() override { enable_qualified_subjects::reset_local(); }
};

TEST_F(ContextSubjectReferenceTest, FromString) {
    // Unqualified subjects: qualified=false
    auto unqual = context_subject_reference::from_string("subject-for-C");
    EXPECT_EQ(
      unqual.sub, (context_subject{default_context, subject{"subject-for-C"}}));
    EXPECT_FALSE(unqual.qualified);

    // Qualified subjects: qualified=true
    auto qual = context_subject_reference::from_string(":.ctx:subject-for-C");
    EXPECT_EQ(
      qual.sub, (context_subject{context{".ctx"}, subject{"subject-for-C"}}));
    EXPECT_TRUE(qual.qualified);

    // Explicit default context is still qualified
    auto default_qual = context_subject_reference::from_string(
      ":.:subject-for-C");
    EXPECT_EQ(
      default_qual.sub,
      (context_subject{default_context, subject{"subject-for-C"}}));
    EXPECT_TRUE(default_qual.qualified);

    // Context-only form without second colon: :.something is qualified
    auto ctx_only = context_subject_reference::from_string(":.something");
    EXPECT_EQ(ctx_only.qualified, is_qualified::yes);
    EXPECT_EQ(
      ctx_only.sub, (context_subject{context{".something"}, subject{""}}));
}

TEST_F(ContextSubjectReferenceTest, Resolve) {
    auto parent_ctx = context{".parent"};

    // Unqualified: inherits parent's context
    auto unqual = context_subject_reference::from_string("subject-for-C");
    EXPECT_EQ(
      unqual.resolve(parent_ctx),
      (context_subject{parent_ctx, subject{"subject-for-C"}}));

    // Qualified: keeps its own context
    auto qual = context_subject_reference::from_string(":.other:subject-for-C");
    EXPECT_EQ(
      qual.resolve(parent_ctx),
      (context_subject{context{".other"}, subject{"subject-for-C"}}));
}

TEST_F(ContextSubjectReferenceTest, ToStringRoundTrip) {
    auto inputs = {
      "simple-subject",
      ":.:default-context-subject",
      ":.ctx:qualified-subject",
      ":.ctx-only:",
    };
    // Unqualified round-trip
    for (const auto& input : inputs) {
        SCOPED_TRACE(input);
        auto got = context_subject_reference::from_string(input).to_string();
        EXPECT_EQ(got, input);
    }
}

} // namespace pandaproxy::schema_registry
