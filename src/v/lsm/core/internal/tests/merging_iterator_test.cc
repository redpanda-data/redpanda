// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/merging_iterator.h"
#include "lsm/core/internal/tests/iterator_test_harness.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

using ::testing::ElementsAre;
using ::testing::Pair;

namespace {

class simple_iterator : public lsm::internal::iterator {
public:
    explicit simple_iterator(std::map<lsm::internal::key, iobuf> data)
      : _data(std::move(data)) {}

    bool valid() const override { return _it != _data.end(); }

    ss::future<> seek_to_first() override {
        _it = _data.begin();
        return ss::now();
    }

    ss::future<> seek_to_last() override {
        _it = _data.empty() ? _data.end() : std::prev(_data.end());
        return ss::now();
    }

    ss::future<> seek(lsm::internal::key_view target) override {
        _it = _data.lower_bound(lsm::internal::key(target));
        return ss::now();
    }

    ss::future<> next() override {
        if (_it != _data.end()) {
            ++_it;
        }
        return ss::now();
    }

    ss::future<> prev() override {
        if (_it == _data.begin()) {
            _it = _data.end();
        } else if (_it != _data.end()) {
            --_it;
        } else if (!_data.empty()) {
            _it = std::prev(_data.end());
        }
        return ss::now();
    }

    lsm::internal::key_view key() override { return _it->first; }

    iobuf value() override { return _it->second.copy(); }

private:
    std::map<lsm::internal::key, iobuf> _data;
    std::map<lsm::internal::key, iobuf>::iterator _it;
};

class merging_iterator_factory {
public:
    std::unique_ptr<lsm::internal::iterator>
    make_iterator(std::map<lsm::internal::key, iobuf> map) {
        chunked_vector<std::unique_ptr<lsm::internal::iterator>> children;
        children.push_back(lsm::internal::iterator::create_empty());
        children.push_back(std::make_unique<simple_iterator>(std::move(map)));
        children.push_back(lsm::internal::iterator::create_empty());
        return lsm::internal::create_merging_iterator(std::move(children));
    }
};

std::string to_string(const iobuf& b) {
    std::string s;
    for (const auto& frag : b) {
        s.append(frag.get(), frag.size());
    }
    return s;
}

std::map<lsm::internal::key, iobuf>
make_test_data(std::vector<std::pair<std::string, std::string>> data) {
    std::map<lsm::internal::key, iobuf> result;
    for (auto& [k, v] : data) {
        result.emplace(
          lsm::internal::key::encode({.key = lsm::user_key_view(k)}),
          iobuf::from(v));
    }
    return result;
}

std::vector<std::pair<std::string, std::string>>
collect_all_pairs(std::unique_ptr<lsm::internal::iterator>& it) {
    std::vector<std::pair<std::string, std::string>> results;
    it->seek_to_first().get();
    while (it->valid()) {
        results.emplace_back(it->key().user_key()(), to_string(it->value()));
        it->next().get();
    }
    return results;
}

std::vector<std::pair<std::string, std::string>>
collect_all_pairs_reverse(std::unique_ptr<lsm::internal::iterator>& it) {
    std::vector<std::pair<std::string, std::string>> results;
    it->seek_to_last().get();
    while (it->valid()) {
        results.emplace_back(it->key().user_key()(), to_string(it->value()));
        it->prev().get();
    }
    return results;
}

} // namespace

using MergingIteratorType = ::testing::Types<merging_iterator_factory>;

INSTANTIATE_TYPED_TEST_SUITE_P(
  MergingIteratorSuite, CoreIteratorTest, MergingIteratorType);

TEST(MergingIteratorTest, MergeTwoIterators) {
    auto data1 = make_test_data({{"a", "1"}, {"c", "3"}, {"e", "5"}});
    auto data2 = make_test_data({{"b", "2"}, {"d", "4"}, {"f", "6"}});

    chunked_vector<std::unique_ptr<lsm::internal::iterator>> children;
    children.push_back(std::make_unique<simple_iterator>(std::move(data1)));
    children.push_back(std::make_unique<simple_iterator>(std::move(data2)));
    auto it = lsm::internal::create_merging_iterator(std::move(children));

    auto results = collect_all_pairs(it);
    EXPECT_THAT(
      results,
      ElementsAre(
        Pair("a", "1"),
        Pair("b", "2"),
        Pair("c", "3"),
        Pair("d", "4"),
        Pair("e", "5"),
        Pair("f", "6")));
}

TEST(MergingIteratorTest, DuplicateKeys) {
    auto data1 = make_test_data({{"a", "1"}, {"b", "2"}, {"c", "3"}});
    auto data2 = make_test_data({{"a", "10"}, {"b", "20"}, {"d", "40"}});

    chunked_vector<std::unique_ptr<lsm::internal::iterator>> children;
    children.push_back(std::make_unique<simple_iterator>(std::move(data1)));
    children.push_back(std::make_unique<simple_iterator>(std::move(data2)));
    auto it = lsm::internal::create_merging_iterator(std::move(children));

    // Verify that duplicates are yielded (should see "a" twice, "b" twice)
    auto results = collect_all_pairs(it);

    // Should get both values for duplicate keys
    EXPECT_THAT(
      results,
      ElementsAre(
        Pair("a", "1"),
        Pair("a", "10"),
        Pair("b", "2"),
        Pair("b", "20"),
        Pair("c", "3"),
        Pair("d", "40")));
}

TEST(MergingIteratorTest, EmptyChildren) {
    chunked_vector<std::unique_ptr<lsm::internal::iterator>> children;
    auto it = lsm::internal::create_merging_iterator(std::move(children));

    it->seek_to_first().get();
    EXPECT_FALSE(it->valid());

    it->seek_to_last().get();
    EXPECT_FALSE(it->valid());
}

TEST(MergingIteratorTest, SingleChild) {
    auto data = make_test_data({{"a", "1"}, {"b", "2"}});

    chunked_vector<std::unique_ptr<lsm::internal::iterator>> children;
    children.push_back(std::make_unique<simple_iterator>(std::move(data)));
    auto it = lsm::internal::create_merging_iterator(std::move(children));

    it->seek_to_first().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "a");
    EXPECT_EQ(it->value(), iobuf::from("1"));

    it->next().get();
    ASSERT_TRUE(it->valid());
    EXPECT_EQ(it->key().user_key(), "b");
    EXPECT_EQ(it->value(), iobuf::from("2"));

    it->next().get();
    EXPECT_FALSE(it->valid());
}

TEST(MergingIteratorTest, BackwardIteration) {
    auto data1 = make_test_data({{"a", "1"}, {"c", "3"}});
    auto data2 = make_test_data({{"b", "2"}, {"d", "4"}});

    chunked_vector<std::unique_ptr<lsm::internal::iterator>> children;
    children.push_back(std::make_unique<simple_iterator>(std::move(data1)));
    children.push_back(std::make_unique<simple_iterator>(std::move(data2)));
    auto it = lsm::internal::create_merging_iterator(std::move(children));

    auto results = collect_all_pairs_reverse(it);
    EXPECT_THAT(
      results,
      ElementsAre(
        Pair("d", "4"), Pair("c", "3"), Pair("b", "2"), Pair("a", "1")));
}
