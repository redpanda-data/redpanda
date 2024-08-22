/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "base/units.h"
#include "storage/key_offset_map.h"

#include <gtest/gtest.h>

namespace {
storage::compaction_key k(std::string_view key) {
    return storage::compaction_key(
      bytes(reinterpret_cast<const uint8_t*>(key.data()), key.size()));
}

model::offset o(int o) { return model::offset(o); }
} // namespace

template<typename T>
class KeyOffsetMapTest : public ::testing::Test {
public:
    T map;
};

/*
 * helper to default-initialize the map in the typed test suite. since
 * allocations are futurized we don't want to bake it into constructor.
 */
class default_sized_hash_key_offset_map : public storage::hash_key_offset_map {
public:
    default_sized_hash_key_offset_map() { initialize(1_MiB).get(); }
};

using test_types = ::testing::
  Types<storage::simple_key_offset_map, default_sized_hash_key_offset_map>;

TYPED_TEST_SUITE(KeyOffsetMapTest, test_types);

TYPED_TEST(KeyOffsetMapTest, Empty) {
    auto& map = this->map;
    EXPECT_EQ(map.size(), 0);
    EXPECT_EQ(map.max_offset(), model::offset{});
    EXPECT_EQ(map.get(k("key")).get(), std::nullopt);
}

TYPED_TEST(KeyOffsetMapTest, Put) {
    auto& map = this->map;
    EXPECT_EQ(map.put(k("key"), o(9)).get(), true);
    EXPECT_EQ(map.size(), 1);
    EXPECT_EQ(map.max_offset(), o(9));
    EXPECT_EQ(map.get(k("key")).get(), o(9));
}

TYPED_TEST(KeyOffsetMapTest, PutGet) {
    auto& map = this->map;

    // new key
    EXPECT_EQ(map.put(k("key0"), o(9)).get(), true);
    EXPECT_EQ(map.get(k("key0")).get(), o(9));

    // new key
    EXPECT_EQ(map.put(k("key1"), o(9)).get(), true);
    EXPECT_EQ(map.get(k("key1")).get(), o(9));

    // new key
    EXPECT_EQ(map.put(k("key2"), o(99)).get(), true);
    EXPECT_EQ(map.get(k("key2")).get(), o(99));

    // overwrite
    EXPECT_EQ(map.put(k("key0"), o(10)).get(), true);
    EXPECT_EQ(map.get(k("key0")).get(), o(10));

    EXPECT_EQ(map.size(), 3);
}

TYPED_TEST(KeyOffsetMapTest, PutKeepsLargestOffset) {
    auto& map = this->map;

    // put(9) - first entry
    EXPECT_EQ(map.put(k("key"), o(9)).get(), true);
    EXPECT_EQ(map.get(k("key")).get(), o(9));

    // put(8) - no change
    EXPECT_EQ(map.put(k("key"), o(8)).get(), true);
    EXPECT_EQ(map.get(k("key")).get(), o(9));

    // put(10) - overwrites with 10
    EXPECT_EQ(map.put(k("key"), o(10)).get(), true);
    EXPECT_EQ(map.get(k("key")).get(), o(10));

    EXPECT_EQ(map.size(), 1);
}

TYPED_TEST(KeyOffsetMapTest, MaxOffset) {
    auto& map = this->map;

    // put(9)
    EXPECT_EQ(map.put(k("key0"), o(9)).get(), true);
    EXPECT_EQ(map.max_offset(), o(9));

    // put(9) - no change
    EXPECT_EQ(map.put(k("key1"), o(9)).get(), true);
    EXPECT_EQ(map.max_offset(), o(9));

    // put(8) - no change
    EXPECT_EQ(map.put(k("key2"), o(8)).get(), true);
    EXPECT_EQ(map.max_offset(), o(9));

    // put(10) - new max
    EXPECT_EQ(map.put(k("key3"), o(10)).get(), true);
    EXPECT_EQ(map.max_offset(), o(10));

    EXPECT_EQ(map.size(), 4);
}

TYPED_TEST(KeyOffsetMapTest, ManyEntries) {
    static constexpr auto test_size = 1000;

    auto& map = this->map;
    std::unordered_map<std::string, int> truth;

    for (int i = 0; i < test_size; ++i) {
        const auto key = fmt::format("key-{}", i);
        storage::compaction_key ck(bytes(key.begin(), key.end()));
        EXPECT_TRUE(map.put(ck, model::offset(i)).get()) << fmt::format(
          "Inserting key {} offset {} size {} capacity {}",
          key,
          i,
          map.size(),
          map.capacity());
        EXPECT_TRUE(truth.emplace(key, i).second);
    }

    EXPECT_EQ(map.size(), truth.size())
      << fmt::format("map size {} capacity {}", map.size(), map.capacity());

    for (const auto& [key, val] : truth) {
        storage::compaction_key ck(bytes(key.begin(), key.end()));
        const auto maybe_val = map.get(ck).get();
        ASSERT_TRUE(maybe_val.has_value());
        EXPECT_EQ(maybe_val.value(), model::offset(val));
    }
}

TYPED_TEST(KeyOffsetMapTest, UpdateSucceedsWhenFull) {
    auto& map = this->map;

    // insert until full
    int count = 0;
    while (true) {
        const auto key = fmt::format("key-{}", count);
        storage::compaction_key ck(bytes(key.begin(), key.end()));
        if (!map.put(ck, model::offset(count)).get()) {
            break;
        }
        ++count;
    }
    ASSERT_GT(count, 0);
    ASSERT_EQ(map.size(), map.capacity());

    // none of the values should be equal to `count`
    for (int i = 0; i < count; ++i) {
        const auto key = fmt::format("key-{}", i);
        storage::compaction_key ck(bytes(key.begin(), key.end()));
        const auto val = map.get(ck).get();
        ASSERT_TRUE(val.has_value());
        ASSERT_NE(val.value(), model::offset(count));
    }

    // when at capacity, we can still update existing keys
    // here we make all the values equal to `count`.
    for (int i = 0; i < count; ++i) {
        const auto key = fmt::format("key-{}", i);
        storage::compaction_key ck(bytes(key.begin(), key.end()));
        ASSERT_TRUE(map.put(ck, model::offset(count)).get());
    }

    // all of the values should be equal to `count`
    for (int i = 0; i < count; ++i) {
        const auto key = fmt::format("key-{}", i);
        storage::compaction_key ck(bytes(key.begin(), key.end()));
        const auto val = map.get(ck).get();
        ASSERT_TRUE(val.has_value());
        ASSERT_EQ(val.value(), model::offset(count));
    }
}

TEST(HashKeyOffsetMapTest, HitRate) {
    storage::hash_key_offset_map map;
    map.initialize(20_MiB).get();

    int i = 0;
    while (true) {
        const auto key = fmt::format("key-{}", i);
        storage::compaction_key ck(bytes(key.begin(), key.end()));
        if (!map.put(ck, model::offset(i)).get()) {
            break;
        }
        ++i;
    }

    EXPECT_LE(map.hit_rate(), 0.3) << fmt::format("Inserted {}", i);
}

TEST(HashKeyOffsetMapTest, Initialize) {
    storage::hash_key_offset_map map;
    map.initialize(1_MiB).get();

    // fill it up with keys with offset 100
    for (int i = 0;; ++i) {
        const auto key = fmt::format("key-{}", i);
        storage::compaction_key ck(bytes(key.begin(), key.end()));
        if (!map.put(ck, model::offset(100)).get()) {
            break;
        }
    }
    EXPECT_GT(map.size(), 0);
    const auto orig_size = map.size();

    // we'll try to overwrite with 99
    for (size_t i = 0; i < orig_size; ++i) {
        const auto key = fmt::format("key-{}", i);
        storage::compaction_key ck(bytes(key.begin(), key.end()));
        ASSERT_TRUE(map.put(ck, model::offset(99)).get());
    }

    // but it won't work cause they are still in the map
    for (size_t i = 0; i < orig_size; ++i) {
        const auto key = fmt::format("key-{}", i);
        storage::compaction_key ck(bytes(key.begin(), key.end()));
        const auto val = map.get(ck).get();
        ASSERT_TRUE(val.has_value());
        ASSERT_EQ(val.value(), model::offset(100));
    }
    EXPECT_EQ(map.size(), orig_size);

    // but now we'll initialize it
    map.reset().get();
    EXPECT_EQ(map.size(), 0);

    // now try to write the 99 offsets
    for (size_t i = 0; i < orig_size; ++i) {
        const auto key = fmt::format("key-{}", i);
        storage::compaction_key ck(bytes(key.begin(), key.end()));
        EXPECT_TRUE(map.put(ck, model::offset(99)).get());
    }
    EXPECT_EQ(map.size(), orig_size);

    // and we'll see the offset = 99 entries
    for (size_t i = 0; i < orig_size; ++i) {
        const auto key = fmt::format("key-{}", i);
        storage::compaction_key ck(bytes(key.begin(), key.end()));
        const auto val = map.get(ck).get();
        ASSERT_TRUE(val.has_value());
        ASSERT_EQ(val.value(), model::offset(99));
    }
}
