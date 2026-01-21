/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/object_utils.h"
#include "gmock/gmock.h"
#include "ssx/sformat.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <ranges>
#include <vector>

#define UUID_REGEX                                                             \
    "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

#define PREFIX_REGEX "[0-9]{3}"

TEST(ObjectPathFactory, LevelZeroPathFormat) {
    auto path = cloud_topics::object_path_factory::level_zero_path(
      cloud_topics::object_id::create(cloud_topics::cluster_epoch{42}));
    EXPECT_THAT(
      path().string(),
      ::testing::MatchesRegex(
        "^level_zero/data/" PREFIX_REGEX "/000000000000000042/" UUID_REGEX
        "$"));
}

TEST(ObjectPathFactory, LevelZeroPathPrefixFormat) {
    // key details:
    // - exactly one '/' between dir and extension
    // - no trailing '/'
    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_prefix("12"),
      "level_zero/data/12");
}

TEST(ObjectPathFactory, LevelZeroDataDir) {
    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_data_dir(),
      cloud_storage_clients::object_key("level_zero/data/"));
}

TEST(ObjectPathFactory, LevelZeroParseEpoch) {
    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_epoch(
        "level_zero/data/000/000000000000010042/"),
      cloud_topics::cluster_epoch(10042));

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_epoch(
        "level_zero/data/000/000000000000010042/asdfalksjdflkjsdflkj"),
      cloud_topics::cluster_epoch(10042));

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_epoch(
        "level_asdf_zero/data/000/000000000000010042/asdfasdf")
        .error(),
      "L0 object name missing prefix: "
      "level_asdf_zero/data/000/000000000000010042/asdfasdf");

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_epoch(
        "level_zero/data/000/0000000000010042/")
        .error(),
      "L0 object name is too short: level_zero/data/000/0000000000010042/");

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_epoch(
        "level_zero/data/00/")
        .error(),
      "L0 object name is too short: level_zero/data/00/");

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_epoch(
        "level_zero/data/000/00000X0000000010042/asdfasdf")
        .error(),
      "L0 object name has invalid epoch: "
      "level_zero/data/000/00000X0000000010042/asdfasdf");
}

TEST(ObjectPathFactory, LevelZeroParsePrefix) {
    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_prefix(
        "level_zero/data/123/"),
      123);

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_prefix(
        "level_zero/data/123/000000000000010042/"),
      123);

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_prefix(
        "level_asdf_zero/data/000/000000000000010042/asdfasdf")
        .error(),
      "L0 object name missing prefix: "
      "level_asdf_zero/data/000/000000000000010042/asdfasdf");

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_prefix(
        "level_zero/data/00/")
        .error(),
      "L0 object name is too short: level_zero/data/00/");

    EXPECT_EQ(
      cloud_topics::object_path_factory::level_zero_path_to_prefix(
        "level_zero/data/0X0/0000000000000010042/asdfasdf")
        .error(),
      "L0 object name has invalid prefix: "
      "level_zero/data/0X0/0000000000000010042/asdfasdf");
}

// -----------------------------------------------------------------------------
// Trie Tests
// -----------------------------------------------------------------------------

namespace {

// Helper to generate the expected "before prune" result: all numbers in [min,
// max] as 3-digit zero-padded strings.
std::vector<ss::sstring>
expected_before_prune(cloud_topics::prefix_range_inclusive range) {
    return std::views::iota((size_t)range.min, (size_t)range.max + 1)
           | std::views::transform(
             [](auto i) { return ssx::sformat("{:03}", i); })
           | std::ranges::to<std::vector<ss::sstring>>();
}

struct trie_test_case {
    cloud_topics::prefix_range_inclusive range;
    std::vector<ss::sstring> expected_after_prune;
};

std::ostream& operator<<(std::ostream& os, const trie_test_case& tc) {
    fmt::print(
      os, "[{}] -> {{{}}}", tc.range, fmt::join(tc.expected_after_prune, ","));
    return os;
}

} // namespace

class TrieCollectPrefixesTest
  : public ::testing::TestWithParam<trie_test_case> {};

TEST_P(TrieCollectPrefixesTest, BeforePruneReturnsAllPaddedNumbers) {
    const auto& tc = GetParam();
    cloud_topics::trie t;

    t.insert(tc.range);

    EXPECT_THAT(
      t.collect(),
      testing::UnorderedElementsAreArray(expected_before_prune(tc.range)))
      << "Before prune, trie should return all numbers in range as 3-digit "
         "strings";
}

TEST_P(TrieCollectPrefixesTest, AfterPruneReturnsMinimalPrefixes) {
    const auto& tc = GetParam();
    cloud_topics::trie t;

    t.insert(tc.range);
    t.prune();

    EXPECT_THAT(
      t.collect(), testing::UnorderedElementsAreArray(tc.expected_after_prune))
      << "After prune, trie should return minimal covering prefixes";
}

TEST_P(TrieCollectPrefixesTest, CollectIsIdempotent) {
    const auto& tc = GetParam();
    cloud_topics::trie t;

    t.insert(tc.range);
    t.prune();

    EXPECT_THAT(t.collect(), testing::UnorderedElementsAreArray(t.collect()));
}

INSTANTIATE_TEST_SUITE_P(
  TriePrefixCompression,
  TrieCollectPrefixesTest,
  ::testing::Values(
    // [0, 12] -> {'00', '010', '011', '012'}
    // 00 covers 000-009 (saturated at depth 2)
    trie_test_case{
      .range = {0, 12}, .expected_after_prune = {"00", "010", "011", "012"}},

    // [100, 199] -> {'1'}
    // 1 covers 100-199 (saturated at depth 1)
    trie_test_case{.range = {100, 199}, .expected_after_prune = {"1"}},

    // [89, 300] -> {'089', '09', '1', '2', '300'}
    // 089 is a single value
    // 09 covers 090-099 (saturated at depth 2)
    // 1 covers 100-199 (saturated at depth 1)
    // 2 covers 200-299 (saturated at depth 1)
    // 300 is a single value
    trie_test_case{
      .range = {89, 300},
      .expected_after_prune = {"089", "09", "1", "2", "300"}},

    // Single value case
    trie_test_case{.range = {42, 42}, .expected_after_prune = {"042"}},

    // Full range [0, 999] should compress to empty prefix (root)
    trie_test_case{.range = {0, 999}, .expected_after_prune = {""}},

    // Partial range that doesn't align to digit boundaries
    trie_test_case{
      .range = {123, 127},
      .expected_after_prune = {"123", "124", "125", "126", "127"}},

    // Range spanning two "tens" groups
    trie_test_case{
      .range = {5, 14},
      .expected_after_prune
      = {"005", "006", "007", "008", "009", "010", "011", "012", "013", "014"}},

    // Range exactly covering one "tens" group
    trie_test_case{.range = {50, 59}, .expected_after_prune = {"05"}},

    // Range exactly covering one "hundreds" group
    trie_test_case{.range = {200, 299}, .expected_after_prune = {"2"}},

    // Range at the boundary
    trie_test_case{.range = {990, 999}, .expected_after_prune = {"99"}}));

// Test empty trie behavior
// Note: An empty/cleared trie returns {""} (a single empty string prefix),
// which semantically means "match everything" (no filtering).
TEST(TrieTest, EmptyTrieReturnsEmptyPrefix) {
    cloud_topics::trie t;
    ASSERT_TRUE(t.collect().empty());
}

// Test clear behavior
TEST(TrieTest, ClearResetsTrieToInitialState) {
    cloud_topics::trie t;
    t.insert({0, 100});
    t.prune();

    // Verify it has specific prefixes before clear
    auto before_clear = t.collect();
    EXPECT_EQ(before_clear.size(), 2);
    EXPECT_THAT(before_clear, testing::UnorderedElementsAre("0", "100"));

    t.clear();

    // After clear, should behave like a fresh trie
    auto after_clear = t.collect();
    ASSERT_TRUE(t.collect().empty());
}

TEST(TrieTest, ClearAllowsReuse) {
    cloud_topics::trie t;

    // First use
    t.insert({100, 199});
    t.prune();
    EXPECT_THAT(t.collect(), testing::ElementsAre("1"));

    // Clear and reuse with different range
    t.clear();
    t.insert({200, 299});
    t.prune();
    EXPECT_THAT(t.collect(), testing::ElementsAre("2"));
}

// Test prune idempotency
TEST(TrieTest, PruneIsIdempotent) {
    cloud_topics::trie t;
    t.insert({89, 300});

    t.prune();
    auto after_first_prune = t.collect();

    t.prune();
    auto after_second_prune = t.collect();

    EXPECT_THAT(
      after_second_prune, testing::ElementsAreArray(after_second_prune));
}

// -----------------------------------------------------------------------------
// PrefixCompressor Tests
// -----------------------------------------------------------------------------

TEST(PrefixCompressorTest, ConsumeWithNoRangeReturnsNullopt) {
    cloud_topics::prefix_compressor pc;
    EXPECT_EQ(pc.consume_prefix(), std::nullopt);
    pc.set_range(std::nullopt);
    EXPECT_EQ(pc.consume_prefix(), std::nullopt);
    // Multiple calls still return nullopt
    EXPECT_EQ(pc.consume_prefix(), std::nullopt);
    EXPECT_EQ(pc.consume_prefix(), std::nullopt);
}

TEST(PrefixCompressorTest, ConsumeIteratesThroughPrefixesAndWraps) {
    cloud_topics::prefix_compressor pc;
    // Use a small range for simplicity: [100, 102] -> 3 prefixes
    pc.set_range(cloud_topics::prefix_range_inclusive{100, 102});

    auto all_prefixes = pc.compressed_key_prefixes();
    ASSERT_EQ(all_prefixes.size(), 3);

    for (auto _ : std::views::iota(0, 2)) {
        // Consume all prefixes
        for (size_t i = 0; i < all_prefixes.size(); ++i) {
            auto prefix = pc.consume_prefix();
            ASSERT_TRUE(prefix.has_value());
            EXPECT_EQ(prefix.value(), all_prefixes[i]);
        }

        // Next consume should return nullopt (end of iteration)
        EXPECT_EQ(pc.consume_prefix(), std::nullopt);
    }
}

TEST(PrefixCompressorTest, SetIdenticalRangeIsNoOp) {
    cloud_topics::prefix_compressor pc;
    pc.set_range(cloud_topics::prefix_range_inclusive{100, 102});

    // Consume one prefix
    auto first = pc.consume_prefix();
    ASSERT_TRUE(first.has_value());

    // Set the same range again - should preserve position
    pc.set_range(cloud_topics::prefix_range_inclusive{100, 102});

    // Should continue from where we left off, not restart
    auto second = pc.consume_prefix();
    ASSERT_TRUE(second.has_value());
    EXPECT_NE(first.value(), second.value());
}

TEST(PrefixCompressorTest, SetDifferentRangeResets) {
    cloud_topics::prefix_compressor pc;
    pc.set_range(cloud_topics::prefix_range_inclusive{100, 102});

    // Consume one prefix
    auto first = pc.consume_prefix();
    ASSERT_TRUE(first.has_value());

    // Set a different range - should reset
    pc.set_range(cloud_topics::prefix_range_inclusive{200, 202});

    auto new_prefixes = pc.compressed_key_prefixes();

    // Should start from beginning of new range
    auto new_first = pc.consume_prefix();
    ASSERT_TRUE(new_first.has_value());
    EXPECT_EQ(new_first.value(), new_prefixes[0]);
}

TEST(PrefixCompressorTest, SetNulloptClearsState) {
    cloud_topics::prefix_compressor pc;
    pc.set_range(cloud_topics::prefix_range_inclusive{100, 102});

    // Consume one prefix
    ASSERT_TRUE(pc.consume_prefix().has_value());

    // Clear the range
    pc.set_range(std::nullopt);

    // Should return nullopt now
    EXPECT_EQ(pc.consume_prefix(), std::nullopt);
    EXPECT_TRUE(pc.compressed_key_prefixes().empty());
}
