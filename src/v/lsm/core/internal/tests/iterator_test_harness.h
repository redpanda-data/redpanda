// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

using data = std::map<std::string, std::string>;

template<typename T>
concept Closeable = requires(T t) {
    { t.close() } -> std::same_as<void>;
};

template<typename IteratorFactory>
class CoreIteratorTest : public ::testing::Test {
protected:
    std::unique_ptr<lsm::internal::iterator> make_iterator(const data& d) {
        std::map<lsm::internal::key, iobuf> encoded;
        for (const auto& [key, value] : d) {
            auto encoded_key = lsm::internal::key::encode(
              {.key = lsm::user_key_view(key)});
            encoded[std::move(encoded_key)] = iobuf::from(value);
        }
        return _factory.make_iterator(std::move(encoded));
    }

    void TearDown() override {
        if constexpr (Closeable<IteratorFactory>) {
            _factory.close();
        }
    }

private:
    IteratorFactory _factory;
};

namespace lsm::internal::testing {
using lsm::internal::operator""_key;
} // namespace lsm::internal::testing

TYPED_TEST_SUITE_P(CoreIteratorTest);

TYPED_TEST_P(CoreIteratorTest, Empty) {
    using namespace lsm::internal::testing;
    auto filter = this->make_iterator({});
    filter->seek_to_first().get();
    ASSERT_FALSE(filter->valid());
    filter->seek_to_last().get();
    ASSERT_FALSE(filter->valid());
    filter->seek("foo"_key).get();
    ASSERT_FALSE(filter->valid());
    filter->seek("bar"_key).get();
    ASSERT_FALSE(filter->valid());
    filter->seek(""_key).get();
    ASSERT_FALSE(filter->valid());
}

TYPED_TEST_P(CoreIteratorTest, Single) {
    using namespace lsm::internal::testing;
    auto check_first = [](lsm::internal::iterator* it) {
        it->seek_to_first().get();
        ASSERT_TRUE(it->valid());
        EXPECT_EQ(it->key(), "foo"_key);
        EXPECT_EQ(it->value(), iobuf::from("bar"));
    };
    auto check_last = [](lsm::internal::iterator* it) {
        it->seek_to_last().get();
        ASSERT_TRUE(it->valid());
        EXPECT_EQ(it->key(), "foo"_key);
        EXPECT_EQ(it->value(), iobuf::from("bar"));
    };
    auto check_seek_at = [](lsm::internal::iterator* it) {
        it->seek("foo"_key).get();
        ASSERT_TRUE(it->valid());
        EXPECT_EQ(it->key(), "foo"_key);
        EXPECT_EQ(it->value(), iobuf::from("bar"));
    };
    auto check_seek_before = [](lsm::internal::iterator* it) {
        it->seek("fo"_key).get();
        ASSERT_TRUE(it->valid());
        EXPECT_EQ(it->key(), "foo"_key);
        EXPECT_EQ(it->value(), iobuf::from("bar"));
    };
    auto check_seek_after = [](lsm::internal::iterator* it) {
        it->seek("fooo"_key).get();
        EXPECT_FALSE(it->valid());
    };
    auto it = this->make_iterator({{"foo", "bar"}});
    for (auto& check :
         std::vector<std::function<void(lsm::internal::iterator*)>>{
           check_first,
           check_last,
           check_seek_at,
           check_seek_before,
           check_seek_after}) {
        check(it.get());
        check(this->make_iterator({{"foo", "bar"}}).get());
    }
}

MATCHER_P2(IsValid, key, value, "") { // NOLINT
    *result_listener << "where the key is " << key << " and the value is "
                     << value;
    if (!arg->valid()) {
        *result_listener << " but the iterator is not valid";
        return false;
    }
    if (arg->key().user_key() != key) {
        *result_listener << " but the key is " << arg->key().user_key();
        return false;
    }
    if (arg->value() != iobuf::from(value)) {
        *result_listener << " but the value is " << arg->value();
        return false;
    }
    return true;
}

TYPED_TEST_P(CoreIteratorTest, FullScans) {
    std::map<std::string, std::string> data;
    for (int i = 0; i < 2000; ++i) {
        data.emplace(fmt::format("k{:04}", i), fmt::format("v{}", i));
    }
    auto it = this->make_iterator(data);
    it->seek_to_first().get();
    for (int i = 0; i < 2000; ++i) {
        ASSERT_THAT(
          it, IsValid(fmt::format("k{:04}", i), fmt::format("v{}", i)));
        it->next().get();
    }
    EXPECT_FALSE(it->valid());
    it->seek_to_last().get();
    for (int i = 1999; i >= 0; --i) {
        ASSERT_THAT(
          it, IsValid(fmt::format("k{:04}", i), fmt::format("v{}", i)));
        it->prev().get();
    }
    EXPECT_FALSE(it->valid());
    it->seek_to_first().get();
    for (int i = 0; i < 2000; ++i) {
        ASSERT_THAT(
          it, IsValid(fmt::format("k{:04}", i), fmt::format("v{}", i)));
        it->next().get();
        // Make sure we can go back
        if (it->valid()) {
            it->prev().get();
            ASSERT_THAT(
              it, IsValid(fmt::format("k{:04}", i), fmt::format("v{}", i)));
            it->next().get();
        }
    }
    EXPECT_FALSE(it->valid());
}

TYPED_TEST_P(CoreIteratorTest, Seek) {
    using namespace lsm::internal::testing;
    std::map<std::string, std::string> data;
    for (int i = 0; i < 2000; ++i) {
        data.emplace(fmt::format("k{:04}", i), fmt::format("v{}", i));
    }
    auto it = this->make_iterator(data);
    it->seek("k100"_key).get();
    ASSERT_THAT(it, IsValid("k1000", "v1000"));
    it->seek("k050"_key).get();
    ASSERT_THAT(it, IsValid("k0500", "v500"));
    it->seek("k0333"_key).get();
    ASSERT_THAT(it, IsValid("k0333", "v333"));
    it->seek("k"_key).get();
    ASSERT_THAT(it, IsValid("k0000", "v0"));
    it->seek(""_key).get();
    ASSERT_THAT(it, IsValid("k0000", "v0"));
    it->seek("k1995"_key).get();
    ASSERT_THAT(it, IsValid("k1995", "v1995"));
}

REGISTER_TYPED_TEST_SUITE_P(CoreIteratorTest, Empty, Single, FullScans, Seek);
