/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

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
