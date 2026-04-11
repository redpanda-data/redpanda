// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/iter.h"
#include "lsm/db/memtable.h"

#include <seastar/core/sharded.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <memory>
#include <string_view>

namespace {

using lsm::internal::operator""_seek_key;
using lsm::internal::operator""_seqno;

class DBIteratorTest : public testing::Test {
public:
    void SetUp() override {
        _memtable = ss::make_lw_shared<lsm::db::memtable>();
        _options = ss::make_lw_shared<lsm::internal::options>();
        _sample_count = 0;
    }

    std::unique_ptr<lsm::internal::iterator> make_db_iterator(
      lsm::internal::sequence_number seqno
      = lsm::internal::sequence_number::max()) {
        auto underlying = _memtable->create_iterator();
        return lsm::db::create_db_iterator(
          std::move(underlying),
          seqno,
          _options,
          [this](lsm::internal::key_view k) {
              _sample_count++;
              _last_sample = lsm::internal::key(k);
              return ss::make_ready_future<>();
          });
    }

    void add_entry(
      std::string_view key,
      std::string_view value,
      lsm::internal::sequence_number seqno,
      lsm::internal::value_type type = lsm::internal::value_type::value) {
        auto ikey = lsm::internal::key::encode(
          {.key = lsm::user_key_view{key}, .seqno = seqno, .type = type});
        if (type == lsm::internal::value_type::value) {
            _memtable->put(ikey, iobuf::from(value));
        } else {
            _memtable->remove(ikey);
        }
    }

    void add_value(
      std::string_view key,
      std::string_view value,
      lsm::internal::sequence_number seqno) {
        add_entry(key, value, seqno, lsm::internal::value_type::value);
    }

    void
    add_tombstone(std::string_view key, lsm::internal::sequence_number seqno) {
        add_entry(key, "", seqno, lsm::internal::value_type::tombstone);
    }

protected:
    ss::lw_shared_ptr<lsm::db::memtable> _memtable;
    ss::lw_shared_ptr<lsm::internal::options> _options;
    int _sample_count;
    lsm::internal::key _last_sample;
};

TEST_F(DBIteratorTest, Empty) {
    auto it = make_db_iterator();

    it->seek_to_first().get();
    EXPECT_FALSE(it->valid());

    it->seek_to_last().get();
    EXPECT_FALSE(it->valid());

    it->seek("foo"_seek_key).get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, SingleValue) {
    add_value("foo", "bar", 100_seqno);

    auto it = make_db_iterator();

    it->seek_to_first().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "foo");
    EXPECT_EQ(it->value(), iobuf::from("bar"));

    it->next().get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, MultipleValues) {
    add_value("a", "val_a", 100_seqno);
    add_value("b", "val_b", 101_seqno);
    add_value("c", "val_c", 102_seqno);

    auto it = make_db_iterator();

    it->seek_to_first().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "a");
    EXPECT_EQ(it->value(), iobuf::from("val_a"));

    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "b");
    EXPECT_EQ(it->value(), iobuf::from("val_b"));

    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");
    EXPECT_EQ(it->value(), iobuf::from("val_c"));

    it->next().get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, SeekToLast) {
    add_value("a", "val_a", 100_seqno);
    add_value("b", "val_b", 101_seqno);
    add_value("c", "val_c", 102_seqno);

    auto it = make_db_iterator();

    it->seek_to_last().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");
    EXPECT_EQ(it->value(), iobuf::from("val_c"));

    it->prev().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "b");
    EXPECT_EQ(it->value(), iobuf::from("val_b"));

    it->prev().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "a");
    EXPECT_EQ(it->value(), iobuf::from("val_a"));

    it->prev().get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, Seek) {
    add_value("a", "val_a", 100_seqno);
    add_value("c", "val_c", 101_seqno);
    add_value("e", "val_e", 102_seqno);

    auto it = make_db_iterator();

    // Seek to exact match
    it->seek("c"_seek_key).get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");
    EXPECT_EQ(it->value(), iobuf::from("val_c"));

    // Seek to key between entries
    it->seek("b"_seek_key).get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");

    // Seek to key before first
    it->seek(""_seek_key).get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "a");

    // Seek to key after last
    it->seek("z"_seek_key).get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, OverwrittenValuesSameKey) {
    // Multiple versions of same key - should only see latest
    add_value("key", "old_value", 100_seqno);
    add_value("key", "new_value", 200_seqno);

    auto it = make_db_iterator();

    it->seek_to_first().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "key");
    EXPECT_EQ(it->value(), iobuf::from("new_value"));

    it->next().get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, SequenceNumberFiltering) {
    add_value("key1", "v1", 100_seqno);
    add_value("key2", "v2", 200_seqno);
    add_value("key3", "v3", 300_seqno);

    // Iterator with seqno 150 should only see key1
    auto it = make_db_iterator(150_seqno);

    it->seek_to_first().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "key1");
    EXPECT_EQ(it->value(), iobuf::from("v1"));

    it->next().get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, TombstoneHidesValue) {
    add_value("key", "value", 100_seqno);
    add_tombstone("key", 200_seqno);

    auto it = make_db_iterator();

    it->seek_to_first().get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, TombstoneAtSnapshot) {
    add_value("key", "value", 100_seqno);
    add_tombstone("key", 200_seqno);

    // View at seqno 150 should see the value
    auto it = make_db_iterator(150_seqno);

    it->seek_to_first().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "key");
    EXPECT_EQ(it->value(), iobuf::from("value"));
}

TEST_F(DBIteratorTest, TombstoneDoesNotAffectOtherKeys) {
    add_value("a", "val_a", 100_seqno);
    add_value("b", "val_b", 101_seqno);
    add_value("c", "val_c", 102_seqno);
    add_tombstone("b", 200_seqno);

    auto it = make_db_iterator();

    it->seek_to_first().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "a");

    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");

    it->next().get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, ForwardBackwardNavigation) {
    add_value("a", "val_a", 100_seqno);
    add_value("b", "val_b", 101_seqno);
    add_value("c", "val_c", 102_seqno);

    auto it = make_db_iterator();

    // Forward
    it->seek_to_first().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "a");

    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "b");

    // Switch to backward
    it->prev().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "a");

    // Switch to forward again
    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "b");

    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");
}

TEST_F(DBIteratorTest, BackwardForwardNavigation) {
    add_value("a", "val_a", 100_seqno);
    add_value("b", "val_b", 101_seqno);
    add_value("c", "val_c", 102_seqno);

    auto it = make_db_iterator();

    // Backward
    it->seek_to_last().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");

    it->prev().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "b");

    // Switch to forward
    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");

    // Switch to backward again
    it->prev().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "b");

    it->prev().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "a");
}

TEST_F(DBIteratorTest, PrevWithOverwrittenKeys) {
    add_value("a", "val_a", 100_seqno);
    add_value("b", "old_b", 101_seqno);
    add_value("b", "new_b", 102_seqno);
    add_value("c", "val_c", 103_seqno);

    auto it = make_db_iterator();

    it->seek_to_last().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");

    it->prev().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "b");
    EXPECT_EQ(it->value(), iobuf::from("new_b"));

    it->prev().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "a");
}

TEST_F(DBIteratorTest, SeekWithOverwrittenKeys) {
    add_value("a", "val_a", 100_seqno);
    add_value("b", "old_b", 101_seqno);
    add_value("b", "new_b", 102_seqno);
    add_value("c", "val_c", 103_seqno);

    auto it = make_db_iterator();

    it->seek("b"_seek_key).get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "b");
    EXPECT_EQ(it->value(), iobuf::from("new_b"));

    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");
}

TEST_F(DBIteratorTest, ComplexSequenceWithDeletions) {
    add_value("a", "val_a1", 100_seqno);
    add_value("b", "val_b1", 150_seqno);
    add_value("c", "val_c1", 180_seqno);
    add_value("a", "val_a2", 200_seqno);
    add_tombstone("b", 250_seqno);

    auto it = make_db_iterator();

    it->seek_to_first().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "a");
    EXPECT_EQ(it->value(), iobuf::from("val_a2"));

    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");
    EXPECT_EQ(it->value(), iobuf::from("val_c1"));

    it->next().get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, SnapshotIsolation) {
    add_value("key", "v1", 100_seqno);
    auto it_v1 = make_db_iterator(150_seqno);

    add_value("key", "v2", 200_seqno);
    auto it_v2 = make_db_iterator(250_seqno);

    add_value("key", "v3", 300_seqno);
    auto it_v3 = make_db_iterator();

    // Each iterator should see its snapshot
    it_v1->seek_to_first().get();
    ASSERT_TRUE(it_v1->valid());
    EXPECT_EQ(it_v1->value(), iobuf::from("v1"));

    it_v2->seek_to_first().get();
    ASSERT_TRUE(it_v2->valid());
    EXPECT_EQ(it_v2->value(), iobuf::from("v2"));

    it_v3->seek_to_first().get();
    ASSERT_TRUE(it_v3->valid());
    EXPECT_EQ(it_v3->value(), iobuf::from("v3"));
}

TEST_F(DBIteratorTest, EmptyRange) {
    add_value("a", "val_a", 100_seqno);
    add_value("z", "val_z", 101_seqno);

    auto it = make_db_iterator();

    it->seek("m"_seek_key).get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "z");

    it->next().get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, AlternatingForwardBackward) {
    add_value("a", "val_a", 100_seqno);
    add_value("b", "val_b", 101_seqno);
    add_value("c", "val_c", 102_seqno);
    add_value("d", "val_d", 103_seqno);

    auto it = make_db_iterator();

    it->seek("b"_seek_key).get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "b");

    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");

    it->prev().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "b");

    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");

    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "d");

    it->prev().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "c");
}

TEST_F(DBIteratorTest, KeySamplingCallback) {
    // Add enough data to trigger sampling
    _options->read_bytes_period = 10;

    for (char c = 'a'; c <= 'z'; ++c) {
        add_value(
          std::to_string(c),
          "123456789",
          100_seqno
            + lsm::internal::sequence_number(static_cast<uint64_t>(c) + 100));
    }

    auto it = make_db_iterator();

    _sample_count = 0;

    // Iterate through all entries
    it->seek_to_first().get();
    while (it->valid()) {
        it->next().get();
    }

    // Should have taken at least one sample
    EXPECT_GT(_sample_count, 0);
}

TEST_F(DBIteratorTest, TombstoneBeforeValues) {
    add_value("b", "val_b", 100_seqno);
    add_tombstone("a", 101_seqno);

    auto it = make_db_iterator();

    it->seek_to_first().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "b");

    it->next().get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, TombstoneAfterValues) {
    add_value("a", "val_a", 100_seqno);
    add_tombstone("b", 101_seqno);

    auto it = make_db_iterator();

    it->seek_to_first().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "a");

    it->next().get();
    EXPECT_FALSE(it->valid());
}

TEST_F(DBIteratorTest, MultipleVersionsWithSnapshot) {
    add_value("key", "v1", 100_seqno);
    add_value("key", "v2", 200_seqno);
    add_value("key", "v3", 300_seqno);
    add_value("key", "v4", 400_seqno);

    auto it_150 = make_db_iterator(150_seqno);
    auto it_250 = make_db_iterator(250_seqno);
    auto it_350 = make_db_iterator(350_seqno);

    it_150->seek_to_first().get();
    ASSERT_TRUE(it_150->valid());
    EXPECT_EQ(it_150->value(), iobuf::from("v1"));

    it_250->seek_to_first().get();
    ASSERT_TRUE(it_250->valid());
    EXPECT_EQ(it_250->value(), iobuf::from("v2"));

    it_350->seek_to_first().get();
    ASSERT_TRUE(it_350->valid());
    EXPECT_EQ(it_350->value(), iobuf::from("v3"));
}

} // namespace
