// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "base/seastarx.h"
#include "lsm/block/builder.h"
#include "lsm/block/reader.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/tests/iterator_test_harness.h"

#include <seastar/core/file.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace {
class block_iterator_factory {
public:
    std::unique_ptr<lsm::internal::iterator>
    make_iterator(std::map<lsm::internal::key, iobuf> map) {
        lsm::block::builder builder;
        for (auto& [key, value] : map) {
            builder.add(key, std::move(value));
        }
        auto b = builder.finish();
        auto c = lsm::block::contents::copy_from(b);
        return lsm::block::reader(std::move(c)).create_iterator();
    }
};
} // namespace

using BlockIteratorType = ::testing::Types<block_iterator_factory>;

INSTANTIATE_TYPED_TEST_SUITE_P(
  BlockIteratorSuite, CoreIteratorTest, BlockIteratorType);
