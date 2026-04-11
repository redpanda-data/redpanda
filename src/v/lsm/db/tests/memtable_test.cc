// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/tests/iterator_test_harness.h"
#include "lsm/db/memtable.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <string_view>

namespace {

using lsm::internal::operator""_key;

class memtable_iterator_factory {
public:
    std::unique_ptr<lsm::internal::iterator>
    make_iterator(std::map<lsm::internal::key, iobuf> map) {
        auto memtable = ss::make_lw_shared<lsm::db::memtable>();
        for (auto& [k, v] : map) {
            memtable->put(k, v.share());
        }
        return memtable->create_iterator();
    }
};

} // namespace

using MemtableIteratorType = ::testing::Types<memtable_iterator_factory>;

INSTANTIATE_TYPED_TEST_SUITE_P(
  MemtableIteratorSuite, CoreIteratorTest, MemtableIteratorType);

class MemtableTest : public testing::Test {
public:
    void add(lsm::internal::key key, iobuf value) {
        _table->put(std::move(key), std::move(value));
    }

    void remove(lsm::internal::key key) { _table->remove(std::move(key)); }

    auto get(lsm::internal::key_view key) { return _table->get(key); }

    auto create_iterator() { return _table->create_iterator(); }

private:
    ss::lw_shared_ptr<lsm::db::memtable> _table
      = ss::make_lw_shared<lsm::db::memtable>();
};

TEST_F(MemtableTest, GetAtVersion) {
    add("key1@1"_key, iobuf::from("value1"));
    add("key1@2"_key, iobuf::from("value2"));
    add("key1@3"_key, iobuf::from("value3"));
    add("key0@4"_key, iobuf::from("value4"));
    add("key2@5"_key, iobuf::from("value5"));
    add("key1@6"_key, iobuf::from("value6"));
    add("key3@7"_key, iobuf::from("boo!"));
    remove("key3@-8"_key);

    struct testcase {
        uint64_t version;
        ss::sstring key;
        std::optional<ss::sstring> value;
    };

    std::vector<testcase> testcases = {
      {
        .version = 1,
        .key = "key1",
        .value = "value1",
      },
      {
        .version = 0,
        .key = "key1",
      },
      {
        .version = 2,
        .key = "key1",
        .value = "value2",
      },
      {
        .version = 3,
        .key = "key1",
        .value = "value3",
      },
      {
        .version = 4,
        .key = "key1",
        .value = "value3",
      },
      {
        .version = 6,
        .key = "key1",
        .value = "value6",
      },
      {
        .version = 99,
        .key = "key1",
        .value = "value6",
      },
      {
        .version = 4,
        .key = "key0",
        .value = "value4",
      },
      {
        .version = 5,
        .key = "key0",
        .value = "value4",
      },
      {
        .version = 3,
        .key = "key0",
      },
      {
        .version = 7,
        .key = "key3",
        .value = "boo!",
      },
      {
        .version = 8,
        .key = "key3",
        .value = "",
      },
      {
        .version = 9,
        .key = "key3",
        .value = "",
      },
    };

    for (const auto& tc : testcases) {
        auto key = lsm::internal::key::encode({
          .key = lsm::user_key_view(tc.key),
          .seqno = lsm::internal::sequence_number(tc.version),
        });
        if (!tc.value) {
            EXPECT_TRUE(get(key).is_missing()) << "key: " << key.decode();
        } else if (tc.value->empty()) {
            EXPECT_TRUE(get(key).is_tombstone()) << "key: " << key.decode();
        } else {
            auto v = iobuf::from(*tc.value);
            EXPECT_THAT(get(key).take_value(), testing::Optional(std::ref(v)))
              << "key: " << key.decode();
        }
    }
}

TEST_F(MemtableTest, StableIterator) {
    add("key1@1"_key, iobuf::from("value1"));
    add("key1@2"_key, iobuf::from("value2"));
    add("key5@3"_key, iobuf::from("value3"));
    auto it = create_iterator();
    it->seek("key1@2"_key).get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key(), "key1@2"_key);
    EXPECT_EQ(it->value(), iobuf::from("value2"));
    add("key1@4"_key, iobuf::from("value4"));
    add("key2@5"_key, iobuf::from("value5"));
    EXPECT_EQ(it->key(), "key1@2"_key) << it->key().decode();
    EXPECT_EQ(it->value(), iobuf::from("value2")) << it->value().hexdump(10);
    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key(), "key1@1"_key) << it->key().decode();
    EXPECT_EQ(it->value(), iobuf::from("value1")) << it->value().hexdump(10);
    it->prev().get();
    ASSERT_TRUE(it->valid());
    it->prev().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key(), "key1@4"_key) << it->key().decode();
    EXPECT_EQ(it->value(), iobuf::from("value4")) << it->value().hexdump(10);
}
