// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/tests/throwing_iterator.h"
#include "lsm/core/internal/two_level_iterator.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <stdexcept>
#include <utility>

namespace {

using lsm::internal::testing::throwing_iterator;

class TwoLevelIteratorExceptionSafetyTest : public ::testing::Test {
public:
    void SetUp() override {
        // Index has one entry whose key is the block's largest user key.
        std::map<lsm::internal::key, iobuf> index_entries;
        index_entries.emplace(
          lsm::internal::key::encode({.key = lsm::user_key_view("c")}),
          iobuf::from("block1"));

        auto idx = std::make_unique<throwing_iterator>(
          std::move(index_entries));
        _index = idx.get();

        _it = lsm::internal::create_two_level_iterator(
          std::move(idx),
          [this](
            iobuf) -> ss::future<std::unique_ptr<lsm::internal::iterator>> {
              if (_data_fn_throws) {
                  _data_fn_throws = false;
                  return ss::make_exception_future<
                    std::unique_ptr<lsm::internal::iterator>>(
                    std::runtime_error("data_iter_fn fail"));
              }
              std::map<lsm::internal::key, iobuf> block_data;
              for (auto k : {"a", "b", "c"}) {
                  block_data.emplace(
                    lsm::internal::key::encode({.key = lsm::user_key_view(k)}),
                    iobuf::from(k));
              }
              auto data_iter = std::make_unique<throwing_iterator>(
                std::move(block_data));
              _last_data = data_iter.get();
              if (_arm_data_on_create) {
                  _arm_data_on_create = false;
                  data_iter->fail_next();
              }
              return ss::make_ready_future<
                std::unique_ptr<lsm::internal::iterator>>(std::move(data_iter));
          });
    }

protected:
    throwing_iterator* _index = nullptr;
    // Raw pointer to the most recently created data iterator. Owned by
    // the two-level iterator under test.
    throwing_iterator* _last_data = nullptr;
    // If set, the next data_iter_fn invocation throws.
    bool _data_fn_throws = false;
    // If set, the next data_iter_fn invocation returns an iterator
    // already armed to fail on its next mutating call.
    bool _arm_data_on_create = false;
    std::unique_ptr<lsm::internal::iterator> _it;
};

// After any thrown await in a mutating method, valid() must report false.
TEST_F(TwoLevelIteratorExceptionSafetyTest, IndexSeekThrowLeavesInvalid) {
    auto target = lsm::internal::key::encode({.key = lsm::user_key_view("a")});
    _it->seek(target).get();
    ASSERT_TRUE(_it->valid());

    _index->fail_next();
    EXPECT_THROW(_it->seek(target).get(), std::runtime_error);
    EXPECT_FALSE(_it->valid());
}

// data_iter_fn throwing during init_data_block leaves valid() == false.
TEST_F(TwoLevelIteratorExceptionSafetyTest, DataIterFnThrowLeavesInvalid) {
    auto target = lsm::internal::key::encode({.key = lsm::user_key_view("a")});
    _it->seek(target).get();
    ASSERT_TRUE(_it->valid());

    _data_fn_throws = true;
    EXPECT_THROW(_it->seek(target).get(), std::runtime_error);
    EXPECT_FALSE(_it->valid());
}

// _data_iter is assigned by init_data_block, then its own seek throws.
TEST_F(TwoLevelIteratorExceptionSafetyTest, DataIterSeekThrowLeavesInvalid) {
    auto target = lsm::internal::key::encode({.key = lsm::user_key_view("a")});
    _it->seek(target).get();
    ASSERT_TRUE(_it->valid());

    _arm_data_on_create = true;
    EXPECT_THROW(_it->seek(target).get(), std::runtime_error);
    EXPECT_FALSE(_it->valid());
}

// The data iterator throwing inside next() leaves valid() == false.
TEST_F(TwoLevelIteratorExceptionSafetyTest, DataIterNextThrowLeavesInvalid) {
    auto target = lsm::internal::key::encode({.key = lsm::user_key_view("a")});
    _it->seek(target).get();
    ASSERT_TRUE(_it->valid());

    _last_data->fail_next();
    EXPECT_THROW(_it->next().get(), std::runtime_error);
    EXPECT_FALSE(_it->valid());
}

// The data iterator throwing inside prev() leaves valid() == false.
TEST_F(TwoLevelIteratorExceptionSafetyTest, DataIterPrevThrowLeavesInvalid) {
    _it->seek_to_last().get();
    ASSERT_TRUE(_it->valid());

    _last_data->fail_next();
    EXPECT_THROW(_it->prev().get(), std::runtime_error);
    EXPECT_FALSE(_it->valid());
}

// seek_to_first with the index throwing leaves valid() == false.
TEST_F(
  TwoLevelIteratorExceptionSafetyTest, SeekToFirstIndexThrowLeavesInvalid) {
    _it->seek_to_first().get();
    ASSERT_TRUE(_it->valid());

    _index->fail_next();
    EXPECT_THROW(_it->seek_to_first().get(), std::runtime_error);
    EXPECT_FALSE(_it->valid());
}

// seek_to_last with the index throwing leaves valid() == false.
TEST_F(TwoLevelIteratorExceptionSafetyTest, SeekToLastIndexThrowLeavesInvalid) {
    _it->seek_to_last().get();
    ASSERT_TRUE(_it->valid());

    _index->fail_next();
    EXPECT_THROW(_it->seek_to_last().get(), std::runtime_error);
    EXPECT_FALSE(_it->valid());
}

} // namespace
