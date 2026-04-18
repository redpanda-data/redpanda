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

#include "encryption/schema_annotation_parser.h"

#include <gtest/gtest.h>

using namespace encryption;

TEST(schema_annotation_parser_test, avro_single_field) {
    ss::sstring schema = R"({
        "type": "record",
        "name": "PersonalData",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "ssn", "type": "string",
             "encryption:kek_name": "pii-key",
             "encryption:kms_key_id": "arn:aws:kms:us-east-1:123456789:key/abc"}
        ]
    })";

    auto annotations = parse_avro_encryption_annotations(schema);

    ASSERT_EQ(annotations.size(), 1);
    EXPECT_EQ(annotations[0].path, std::vector<ss::sstring>{"ssn"});
    EXPECT_EQ(annotations[0].kek_name, "pii-key");
    EXPECT_EQ(
      annotations[0].kms_key_id, "arn:aws:kms:us-east-1:123456789:key/abc");
}

TEST(schema_annotation_parser_test, avro_multiple_fields) {
    ss::sstring schema = R"({
        "type": "record",
        "name": "UserData",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "ssn", "type": "string",
             "encryption:kek_name": "pii-key",
             "encryption:kms_key_id": "arn:aws:kms:key1"},
            {"name": "credit_card", "type": "string",
             "encryption:kek_name": "financial-key",
             "encryption:kms_key_id": "arn:aws:kms:key2"}
        ]
    })";

    auto annotations = parse_avro_encryption_annotations(schema);

    ASSERT_EQ(annotations.size(), 2);

    EXPECT_EQ(annotations[0].path, std::vector<ss::sstring>{"ssn"});
    EXPECT_EQ(annotations[0].kek_name, "pii-key");
    EXPECT_EQ(annotations[0].kms_key_id, "arn:aws:kms:key1");

    EXPECT_EQ(annotations[1].path, std::vector<ss::sstring>{"credit_card"});
    EXPECT_EQ(annotations[1].kek_name, "financial-key");
    EXPECT_EQ(annotations[1].kms_key_id, "arn:aws:kms:key2");
}

TEST(schema_annotation_parser_test, avro_nested_record) {
    ss::sstring schema = R"({
        "type": "record",
        "name": "Outer",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "inner", "type": {
                "type": "record",
                "name": "Inner",
                "fields": [
                    {"name": "secret", "type": "string",
                     "encryption:kek_name": "key1"}
                ]
            }}
        ]
    })";

    auto annotations = parse_avro_encryption_annotations(schema);

    ASSERT_EQ(annotations.size(), 1);
    auto expected_path = std::vector<ss::sstring>{"inner", "secret"};
    EXPECT_EQ(annotations[0].path, expected_path);
    EXPECT_EQ(annotations[0].kek_name, "key1");
    EXPECT_TRUE(annotations[0].kms_key_id.empty());
}

TEST(schema_annotation_parser_test, avro_no_annotations) {
    ss::sstring schema = R"({
        "type": "record",
        "name": "Plain",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"}
        ]
    })";

    auto annotations = parse_avro_encryption_annotations(schema);

    EXPECT_TRUE(annotations.empty());
}

TEST(schema_annotation_parser_test, avro_kek_without_key_id) {
    ss::sstring schema = R"({
        "type": "record",
        "name": "Simple",
        "fields": [
            {"name": "token", "type": "string",
             "encryption:kek_name": "my-kek"}
        ]
    })";

    auto annotations = parse_avro_encryption_annotations(schema);

    ASSERT_EQ(annotations.size(), 1);
    EXPECT_EQ(annotations[0].path, std::vector<ss::sstring>{"token"});
    EXPECT_EQ(annotations[0].kek_name, "my-kek");
    EXPECT_TRUE(annotations[0].kms_key_id.empty());
}

TEST(schema_annotation_parser_test, avro_union_with_nested_record) {
    ss::sstring schema = R"({
        "type": "record",
        "name": "Outer",
        "fields": [
            {"name": "optional_inner", "type": ["null", {
                "type": "record",
                "name": "Inner",
                "fields": [
                    {"name": "data", "type": "string",
                     "encryption:kek_name": "union-key"}
                ]
            }]}
        ]
    })";

    auto annotations = parse_avro_encryption_annotations(schema);

    ASSERT_EQ(annotations.size(), 1);
    auto expected_path = std::vector<ss::sstring>{"optional_inner", "data"};
    EXPECT_EQ(annotations[0].path, expected_path);
    EXPECT_EQ(annotations[0].kek_name, "union-key");
}

TEST(schema_annotation_parser_test, json_schema_single_field) {
    ss::sstring schema = R"({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "ssn": {"type": "string",
                    "encryption:kek_name": "pii-key",
                    "encryption:kms_key_id": "arn:aws:kms:key1"}
        }
    })";

    auto annotations = parse_json_schema_encryption_annotations(schema);

    ASSERT_EQ(annotations.size(), 1);
    EXPECT_EQ(annotations[0].path, std::vector<ss::sstring>{"ssn"});
    EXPECT_EQ(annotations[0].kek_name, "pii-key");
    EXPECT_EQ(annotations[0].kms_key_id, "arn:aws:kms:key1");
}

TEST(schema_annotation_parser_test, json_schema_nested) {
    ss::sstring schema = R"({
        "type": "object",
        "properties": {
            "address": {
                "type": "object",
                "properties": {
                    "street": {"type": "string",
                               "encryption:kek_name": "pii-key"}
                }
            }
        }
    })";

    auto annotations = parse_json_schema_encryption_annotations(schema);

    ASSERT_EQ(annotations.size(), 1);
    auto expected_path = std::vector<ss::sstring>{"address", "street"};
    EXPECT_EQ(annotations[0].path, expected_path);
    EXPECT_EQ(annotations[0].kek_name, "pii-key");
    EXPECT_TRUE(annotations[0].kms_key_id.empty());
}

TEST(schema_annotation_parser_test, json_schema_no_annotations) {
    ss::sstring schema = R"({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "name": {"type": "string"}
        }
    })";

    auto annotations = parse_json_schema_encryption_annotations(schema);

    EXPECT_TRUE(annotations.empty());
}

TEST(schema_annotation_parser_test, invalid_json_returns_empty) {
    ss::sstring bad_json = "not valid json {{{";

    auto avro_result = parse_avro_encryption_annotations(bad_json);
    EXPECT_TRUE(avro_result.empty());

    auto json_result = parse_json_schema_encryption_annotations(bad_json);
    EXPECT_TRUE(json_result.empty());
}
