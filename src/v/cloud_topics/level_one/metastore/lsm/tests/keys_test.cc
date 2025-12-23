/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "model/fundamental.h"

#include <gtest/gtest.h>

using namespace cloud_topics::l1;

namespace {

// Fixed UUIDs for predictable/real test strings
constexpr std::string_view test_uuid = "12345678-1234-5678-1234-567812345678";

// Helper to create a test topic_id_partition with fixed UUID
model::topic_id_partition
make_tidp(std::string_view uuid_str = test_uuid, int partition = 0) {
    return model::topic_id_partition(
      model::topic_id(uuid_t::from_string(uuid_str)),
      model::partition_id(partition));
}

} // namespace

TEST(MetadataRowKeyTest, TestRoundTrip) {
    auto tidp = make_tidp(test_uuid, 42);
    auto encoded = metadata_row_key::encode(tidp);

    // 00 <test_uuid_1> <padded 42 (2a)>
    ASSERT_STREQ(encoded.data(), "00123456781234567812345678123456780000002a");
    auto decoded = metadata_row_key::decode(encoded);

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->tidp, tidp);
    EXPECT_EQ(decoded->tidp.partition(), model::partition_id(42));
}

TEST(ExtentRowKeyTest, TestRoundTrip) {
    auto tidp = make_tidp(test_uuid, 42);
    kafka::offset base_offset(12345);
    auto encoded = extent_row_key::encode(tidp, base_offset);

    // 01 <test_uuid_1> <padded 42 (2a)> <padded 12345 (3039)>
    ASSERT_STREQ(
      encoded.data(),
      "01123456781234567812345678123456780000002a0000000000003039");
    auto decoded = extent_row_key::decode(encoded);

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->tidp, tidp);
    EXPECT_EQ(decoded->tidp.partition(), model::partition_id(42));
    EXPECT_EQ(decoded->base_offset, base_offset);
}

TEST(TermRowKeyTest, TestRoundTrip) {
    auto tidp = make_tidp(test_uuid, 42);
    model::term_id term(987);
    auto encoded = term_row_key::encode(tidp, term);

    // 02 <test_uuid_1> <padded 42 (2a)> <padded 987 (3db)>
    ASSERT_STREQ(
      encoded.data(),
      "02123456781234567812345678123456780000002a00000000000003db");
    auto decoded = term_row_key::decode(encoded);

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->tidp, tidp);
    EXPECT_EQ(decoded->tidp.partition(), model::partition_id(42));
    EXPECT_EQ(decoded->term, term);
}

TEST(CompactionRowKeyTest, TestRoundTrip) {
    auto tidp = make_tidp(test_uuid, 42);
    auto encoded = compaction_row_key::encode(tidp);

    // 03 <test_uuid_1> <padded 42 (2a)>
    ASSERT_STREQ(encoded.data(), "03123456781234567812345678123456780000002a");
    auto decoded = compaction_row_key::decode(encoded);

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->tidp, tidp);
    EXPECT_EQ(decoded->tidp.partition(), model::partition_id(42));
}

TEST(ObjectRowKeyTest, TestRoundTrip) {
    object_id oid(uuid_t::from_string(test_uuid));
    auto encoded = object_row_key::encode(oid);

    // 04 <test_uuid_1>
    ASSERT_STREQ(encoded.data(), "0412345678123456781234567812345678");
    auto decoded = object_row_key::decode(encoded);

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->oid, oid);
}

TEST(MetadataRowKeyTest, TestBadDecoding) {
    constexpr std::string_view valid_encoded
      = "00123456781234567812345678123456780000002a";

    // All truncated versions should fail.
    for (size_t len = 0; len < valid_encoded.size(); ++len) {
        EXPECT_FALSE(
          metadata_row_key::decode(valid_encoded.substr(0, len)).has_value());
    }

    // Wrong row type.
    EXPECT_FALSE(
      metadata_row_key::decode("01123456781234567812345678123456780000002a")
        .has_value());

    // Invalid hex.
    EXPECT_FALSE(
      metadata_row_key::decode("00ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ0000002a")
        .has_value());
    EXPECT_FALSE(
      metadata_row_key::decode("0012345678123456781234567812345678ZZZZZZZZ")
        .has_value());
}

TEST(ExtentRowKeyTest, TestBadDecoding) {
    constexpr std::string_view valid_encoded
      = "01123456781234567812345678123456780000002a0000000000003039";

    // All truncated versions should fail.
    for (size_t len = 0; len < valid_encoded.size(); ++len) {
        EXPECT_FALSE(
          extent_row_key::decode(valid_encoded.substr(0, len)).has_value());
    }

    // Wrong row type.
    EXPECT_FALSE(
      extent_row_key::decode(
        "00123456781234567812345678123456780000002a0000000000003039")
        .has_value());

    // Invalid hex.
    EXPECT_FALSE(
      extent_row_key::decode(
        "01123456781234567812345678123456780000002aZZZZZZZZZZZZZZZZ")
        .has_value());
    EXPECT_FALSE(
      extent_row_key::decode(
        "0112345678123456781234567812345678ZZZZZZZZ0000000000003039")
        .has_value());
    EXPECT_FALSE(
      extent_row_key::decode(
        "01ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ0000002a0000000000003039")
        .has_value());
}

TEST(TermRowKeyTest, TestBadDecoding) {
    constexpr std::string_view valid_encoded
      = "02123456781234567812345678123456780000002a00000000000003db";

    // All truncated versions should fail.
    for (size_t len = 0; len < valid_encoded.size(); ++len) {
        EXPECT_FALSE(
          term_row_key::decode(valid_encoded.substr(0, len)).has_value());
    }

    // Wrong row type.
    EXPECT_FALSE(
      term_row_key::decode(
        "00123456781234567812345678123456780000002a00000000000003db")
        .has_value());

    // Invalid hex.
    EXPECT_FALSE(
      term_row_key::decode(
        "02123456781234567812345678123456780000002aZZZZZZZZZZZZZZZZ")
        .has_value());
    EXPECT_FALSE(
      term_row_key::decode(
        "0212345678123456781234567812345678ZZZZZZZZ00000000000003db")
        .has_value());
    EXPECT_FALSE(
      term_row_key::decode(
        "02ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ0000002a00000000000003db")
        .has_value());
}

TEST(CompactionRowKeyTest, TestBadDecoding) {
    constexpr std::string_view valid_encoded
      = "03123456781234567812345678123456780000002a";

    // All truncated versions should fail.
    for (size_t len = 0; len < valid_encoded.size(); ++len) {
        EXPECT_FALSE(
          compaction_row_key::decode(valid_encoded.substr(0, len)).has_value());
    }

    // Wrong row type.
    EXPECT_FALSE(
      compaction_row_key::decode("00123456781234567812345678123456780000002a")
        .has_value());

    // Invalid hex.
    EXPECT_FALSE(
      compaction_row_key::decode("03ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ0000002a")
        .has_value());
    EXPECT_FALSE(
      compaction_row_key::decode("0312345678123456781234567812345678ZZZZZZZZ")
        .has_value());
}

TEST(ObjectRowKeyTest, TestBadDecoding) {
    constexpr std::string_view valid_encoded
      = "04abcdef0123456789abcdef0123456789";

    // All truncated versions should fail.
    for (size_t len = 0; len < valid_encoded.size(); ++len) {
        EXPECT_FALSE(
          object_row_key::decode(valid_encoded.substr(0, len)).has_value());
    }

    // Wrong row type.
    EXPECT_FALSE(
      object_row_key::decode("00abcdef0123456789abcdef0123456789").has_value());

    // Invalid hex.
    EXPECT_FALSE(
      object_row_key::decode("04ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ").has_value());
}
