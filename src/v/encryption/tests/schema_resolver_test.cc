/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "encryption/schema_resolver.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <vector>

using namespace encryption;

TEST_CORO(schema_resolver_test, topic_with_encrypt_rules) {
    schema_resolver resolver;

    resolver.register_rules(
      model::topic("payments"),
      topic_encryption_config{
        .format = schema_format::avro,
        .handle = std::monostate{},
        .rules = {encryption_rule{.tag = "PII", .kek_name = "pii-kek"}},
        .field_tags = {field_tag_mapping{
          .path = {"card_number"}, .tag = "PII"}},
      });

    auto result = co_await resolver.resolve(model::topic("payments"));

    ASSERT_TRUE_CORO(result.has_value());
    EXPECT_EQ(result->format, schema_format::avro);
    ASSERT_EQ_CORO(result->tagged_fields.size(), 1);
    EXPECT_EQ(
      result->tagged_fields[0].path, std::vector<ss::sstring>{"card_number"});
    EXPECT_EQ(result->tagged_fields[0].tag, "PII");
    EXPECT_EQ(result->tagged_fields[0].kek_name, "pii-kek");
}

TEST_CORO(schema_resolver_test, topic_without_rules) {
    schema_resolver resolver;

    auto result = co_await resolver.resolve(model::topic("no-rules-topic"));

    EXPECT_FALSE(result.has_value());
}

TEST_CORO(schema_resolver_test, multiple_rules_different_tags) {
    schema_resolver resolver;

    resolver.register_rules(
      model::topic("users"),
      topic_encryption_config{
        .format = schema_format::protobuf,
        .handle = std::monostate{},
        .rules
        = {encryption_rule{.tag = "PII", .kek_name = "pii-kek"}, encryption_rule{.tag = "FINANCIAL", .kek_name = "finance-kek"}},
        .field_tags
        = {field_tag_mapping{.path = {"ssn"}, .tag = "PII"}, field_tag_mapping{.path = {"salary"}, .tag = "FINANCIAL"}},
      });

    auto result = co_await resolver.resolve(model::topic("users"));

    ASSERT_TRUE_CORO(result.has_value());
    EXPECT_EQ(result->format, schema_format::protobuf);
    ASSERT_EQ_CORO(result->tagged_fields.size(), 2);

    EXPECT_EQ(result->tagged_fields[0].path, std::vector<ss::sstring>{"ssn"});
    EXPECT_EQ(result->tagged_fields[0].tag, "PII");
    EXPECT_EQ(result->tagged_fields[0].kek_name, "pii-kek");

    EXPECT_EQ(
      result->tagged_fields[1].path, std::vector<ss::sstring>{"salary"});
    EXPECT_EQ(result->tagged_fields[1].tag, "FINANCIAL");
    EXPECT_EQ(result->tagged_fields[1].kek_name, "finance-kek");
}

TEST_CORO(schema_resolver_test, first_matching_rule_wins) {
    schema_resolver resolver;

    // Two rules match the same tag "PII"; the first should win.
    resolver.register_rules(
      model::topic("orders"),
      topic_encryption_config{
        .format = schema_format::json,
        .handle = std::monostate{},
        .rules
        = {encryption_rule{.tag = "PII", .kek_name = "primary-kek"}, encryption_rule{.tag = "PII", .kek_name = "secondary-kek"}},
        .field_tags = {field_tag_mapping{.path = {"email"}, .tag = "PII"}},
      });

    auto result = co_await resolver.resolve(model::topic("orders"));

    ASSERT_TRUE_CORO(result.has_value());
    ASSERT_EQ_CORO(result->tagged_fields.size(), 1);
    EXPECT_EQ(result->tagged_fields[0].kek_name, "primary-kek");
}

TEST_CORO(schema_resolver_test, has_encryption_rules_cached) {
    schema_resolver resolver;

    EXPECT_FALSE(
      resolver.has_encryption_rules_cached(model::topic("not-registered")));

    resolver.register_rules(
      model::topic("secret-topic"),
      topic_encryption_config{
        .format = schema_format::avro,
        .handle = std::monostate{},
        .rules = {encryption_rule{.tag = "SECRET", .kek_name = "s-kek"}},
        .field_tags = {field_tag_mapping{.path = {"data"}, .tag = "SECRET"}},
      });

    EXPECT_TRUE(
      resolver.has_encryption_rules_cached(model::topic("secret-topic")));
    co_return;
}

TEST_CORO(schema_resolver_test, resolve_caches_result) {
    schema_resolver resolver;

    resolver.register_rules(
      model::topic("cached-topic"),
      topic_encryption_config{
        .format = schema_format::avro,
        .handle = std::monostate{},
        .rules = {encryption_rule{.tag = "PII", .kek_name = "kek-1"}},
        .field_tags = {field_tag_mapping{.path = {"name"}, .tag = "PII"}},
      });

    // First resolve populates the cache.
    auto result1 = co_await resolver.resolve(model::topic("cached-topic"));
    ASSERT_TRUE_CORO(result1.has_value());

    // Second resolve should return the same result from cache.
    auto result2 = co_await resolver.resolve(model::topic("cached-topic"));
    ASSERT_TRUE_CORO(result2.has_value());
    ASSERT_EQ_CORO(result2->tagged_fields.size(), 1);
    EXPECT_EQ(result2->tagged_fields[0].kek_name, "kek-1");
}

TEST_CORO(schema_resolver_test, no_matching_tags_returns_nullopt) {
    schema_resolver resolver;

    // Rules and field tags exist, but tags don't match.
    resolver.register_rules(
      model::topic("mismatch"),
      topic_encryption_config{
        .format = schema_format::avro,
        .handle = std::monostate{},
        .rules = {encryption_rule{.tag = "FINANCIAL", .kek_name = "fin-kek"}},
        .field_tags = {field_tag_mapping{.path = {"name"}, .tag = "PII"}},
      });

    auto result = co_await resolver.resolve(model::topic("mismatch"));

    EXPECT_FALSE(result.has_value());
}

TEST_CORO(schema_resolver_test, nested_field_path) {
    schema_resolver resolver;

    resolver.register_rules(
      model::topic("nested"),
      topic_encryption_config{
        .format = schema_format::avro,
        .handle = std::monostate{},
        .rules = {encryption_rule{.tag = "PII", .kek_name = "kek-1"}},
        .field_tags = {field_tag_mapping{
          .path = {"address", "street"}, .tag = "PII"}},
      });

    auto result = co_await resolver.resolve(model::topic("nested"));

    ASSERT_TRUE_CORO(result.has_value());
    ASSERT_EQ_CORO(result->tagged_fields.size(), 1);
    auto expected_path = std::vector<ss::sstring>{"address", "street"};
    EXPECT_EQ(result->tagged_fields[0].path, expected_path);
}

TEST_CORO(schema_resolver_test, register_rules_invalidates_cache) {
    schema_resolver resolver;

    resolver.register_rules(
      model::topic("evolving"),
      topic_encryption_config{
        .format = schema_format::avro,
        .handle = std::monostate{},
        .rules = {encryption_rule{.tag = "PII", .kek_name = "old-kek"}},
        .field_tags = {field_tag_mapping{.path = {"name"}, .tag = "PII"}},
      });

    auto result1 = co_await resolver.resolve(model::topic("evolving"));
    ASSERT_TRUE_CORO(result1.has_value());
    EXPECT_EQ(result1->tagged_fields[0].kek_name, "old-kek");

    // Re-register with updated rules; cache should be invalidated.
    resolver.register_rules(
      model::topic("evolving"),
      topic_encryption_config{
        .format = schema_format::avro,
        .handle = std::monostate{},
        .rules = {encryption_rule{.tag = "PII", .kek_name = "new-kek"}},
        .field_tags = {field_tag_mapping{.path = {"name"}, .tag = "PII"}},
      });

    auto result2 = co_await resolver.resolve(model::topic("evolving"));
    ASSERT_TRUE_CORO(result2.has_value());
    EXPECT_EQ(result2->tagged_fields[0].kek_name, "new-kek");
}
