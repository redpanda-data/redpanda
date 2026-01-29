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
#include "random/generators.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace cloud_topics::l1;

namespace {

constexpr std::string_view uuid = "12345678-1234-5678-1234-567812345678";

model::topic_id_partition
tp(std::string_view uuid_str = uuid, int partition = 0) {
    return model::topic_id_partition(
      model::topic_id(uuid_t::from_string(uuid_str)),
      model::partition_id(partition));
}

template<typename KeyType>
void verify_truncation_fails(std::string_view valid_encoded) {
    for (size_t len = 0; len < valid_encoded.size(); ++len) {
        EXPECT_FALSE(KeyType::decode(valid_encoded.substr(0, len)).has_value())
          << "Length " << len << " should fail";
    }
}

using o = kafka::offset;
using t = model::term_id;
constexpr auto meta_key = &metadata_row_key::encode;
constexpr auto ext_key = &extent_row_key::encode;
constexpr auto term_key = &term_row_key::encode;
constexpr auto cmp_key = &compaction_row_key::encode;
constexpr auto obj_key = &object_row_key::encode;

} // namespace

TEST(MetadataRowKeyTest, TestRoundTrip) {
    auto tidp = tp(uuid, 42);
    auto encoded = meta_key(tidp);

    // 00 <test_uuid_1> <padded 42 (2a)>
    ASSERT_STREQ(encoded.data(), "00123456781234567812345678123456780000002a");
    auto decoded = metadata_row_key::decode(encoded);

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->tidp, tidp);
    EXPECT_EQ(decoded->tidp.partition(), model::partition_id(42));
}

TEST(ExtentRowKeyTest, TestRoundTrip) {
    auto tidp = tp(uuid, 42);
    o base_offset(12345);
    auto encoded = ext_key(tidp, base_offset);

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
    auto tidp = tp(uuid, 42);
    t term(987);
    auto encoded = term_key(tidp, term);

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
    auto tidp = tp(uuid, 42);
    auto encoded = cmp_key(tidp);

    // 03 <test_uuid_1> <padded 42 (2a)>
    ASSERT_STREQ(encoded.data(), "03123456781234567812345678123456780000002a");
    auto decoded = compaction_row_key::decode(encoded);

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->tidp, tidp);
    EXPECT_EQ(decoded->tidp.partition(), model::partition_id(42));
}

TEST(ObjectRowKeyTest, TestRoundTrip) {
    object_id oid(uuid_t::from_string(uuid));
    auto encoded = obj_key(oid);

    // 04 <test_uuid_1>
    ASSERT_STREQ(encoded.data(), "0412345678123456781234567812345678");
    auto decoded = object_row_key::decode(encoded);

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->oid, oid);
}

TEST(MetadataRowKeyTest, TestBadDecoding) {
    constexpr std::string_view valid
      = "00123456781234567812345678123456780000002a";
    verify_truncation_fails<metadata_row_key>(valid);

    // Wrong row type.
    EXPECT_FALSE(
      metadata_row_key::decode("01123456781234567812345678123456780000002a")
        .has_value());

    // Invalid hex in topic_id / partition_id.
    EXPECT_FALSE(
      metadata_row_key::decode("00ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ0000002a")
        .has_value());
    EXPECT_FALSE(
      metadata_row_key::decode("0012345678123456781234567812345678ZZZZZZZZ")
        .has_value());
}

TEST(ExtentRowKeyTest, TestBadDecoding) {
    constexpr std::string_view valid
      = "01123456781234567812345678123456780000002a0000000000003039";
    verify_truncation_fails<extent_row_key>(valid);

    // Wrong row type.
    EXPECT_FALSE(
      extent_row_key::decode(
        "00123456781234567812345678123456780000002a0000000000003039")
        .has_value());

    // Invalid hex in topic_id / partition_id / offset.
    EXPECT_FALSE(
      extent_row_key::decode(
        "01ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ0000002a0000000000003039")
        .has_value());
    EXPECT_FALSE(
      extent_row_key::decode(
        "0112345678123456781234567812345678ZZZZZZZZ0000000000003039")
        .has_value());
    EXPECT_FALSE(
      extent_row_key::decode(
        "01123456781234567812345678123456780000002aZZZZZZZZZZZZZZZZ")
        .has_value());
}

TEST(TermRowKeyTest, TestBadDecoding) {
    constexpr std::string_view valid
      = "02123456781234567812345678123456780000002a00000000000003db";
    verify_truncation_fails<term_row_key>(valid);

    // Wrong row type.
    EXPECT_FALSE(
      term_row_key::decode(
        "00123456781234567812345678123456780000002a00000000000003db")
        .has_value());

    // Invalid hex in topic_id / partition_id / term.
    EXPECT_FALSE(
      term_row_key::decode(
        "02ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ0000002a00000000000003db")
        .has_value());
    EXPECT_FALSE(
      term_row_key::decode(
        "0212345678123456781234567812345678ZZZZZZZZ00000000000003db")
        .has_value());
    EXPECT_FALSE(
      term_row_key::decode(
        "02123456781234567812345678123456780000002aZZZZZZZZZZZZZZZZ")
        .has_value());
}

TEST(CompactionRowKeyTest, TestBadDecoding) {
    constexpr std::string_view valid
      = "03123456781234567812345678123456780000002a";
    verify_truncation_fails<compaction_row_key>(valid);

    // Wrong row type.
    EXPECT_FALSE(
      compaction_row_key::decode("00123456781234567812345678123456780000002a")
        .has_value());

    // Invalid hex in topic_id / partition_id.
    EXPECT_FALSE(
      compaction_row_key::decode("03ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ0000002a")
        .has_value());
    EXPECT_FALSE(
      compaction_row_key::decode("0312345678123456781234567812345678ZZZZZZZZ")
        .has_value());
}

TEST(ObjectRowKeyTest, TestBadDecoding) {
    constexpr std::string_view valid = "04abcdef0123456789abcdef0123456789";
    verify_truncation_fails<object_row_key>(valid);

    // Wrong row type.
    EXPECT_FALSE(
      object_row_key::decode("00abcdef0123456789abcdef0123456789").has_value());

    // Invalid hex in object_id.
    EXPECT_FALSE(
      object_row_key::decode("04ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ").has_value());
}

TEST(KeyOrderingTest, TestGlobalKeyOrdering) {
    std::vector<ss::sstring> keys;
    constexpr auto uuid1 = "11111111-1111-1111-1111-111111111111";
    constexpr auto uuid2 = "22222222-2222-2222-2222-222222222222";
    constexpr int p0 = 0;
    constexpr int p1 = 1;
    auto oid1 = object_id(uuid_t::from_string(uuid1));
    auto oid2 = object_id(uuid_t::from_string(uuid2));

    // Add all kinds of keys.
    // NOTE: we're inserting in sorted order for readability, and then
    // shuffling, and then sorting to test.

    keys.push_back(meta_key(tp(uuid1, p0)));
    keys.push_back(meta_key(tp(uuid1, p1)));
    keys.push_back(meta_key(tp(uuid2, p0)));
    keys.push_back(meta_key(tp(uuid2, p1)));

    keys.push_back(ext_key(tp(uuid1, p0), o(0)));
    keys.push_back(ext_key(tp(uuid1, p0), o(100)));
    keys.push_back(ext_key(tp(uuid1, p1), o(0)));
    keys.push_back(ext_key(tp(uuid2, p0), o(50)));

    keys.push_back(term_key(tp(uuid1, p0), t(1)));
    keys.push_back(term_key(tp(uuid1, p0), t(5)));
    keys.push_back(term_key(tp(uuid2, p1), t(3)));

    keys.push_back(cmp_key(tp(uuid1, p0)));
    keys.push_back(cmp_key(tp(uuid2, p1)));

    keys.push_back(obj_key(oid1));
    keys.push_back(obj_key(oid2));

    std::shuffle(
      keys.begin(), keys.end(), random_generators::global().engine());
    EXPECT_THAT(
      keys,
      testing::WhenSorted(
        testing::ElementsAre(
          meta_key(tp(uuid1, p0)),
          meta_key(tp(uuid1, p1)),
          meta_key(tp(uuid2, p0)),
          meta_key(tp(uuid2, p1)),
          ext_key(tp(uuid1, p0), o(0)),
          ext_key(tp(uuid1, p0), o(100)),
          ext_key(tp(uuid1, p1), o(0)),
          ext_key(tp(uuid2, p0), o(50)),
          term_key(tp(uuid1, p0), t(1)),
          term_key(tp(uuid1, p0), t(5)),
          term_key(tp(uuid2, p1), t(3)),
          cmp_key(tp(uuid1, p0)),
          cmp_key(tp(uuid2, p1)),
          obj_key(oid1),
          obj_key(oid2))));
}
