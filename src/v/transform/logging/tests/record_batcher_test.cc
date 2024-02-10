/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/units.h"
#include "transform/logging/record_batcher.h"
#include "transform/logging/tests/utils.h"

#include <gtest/gtest.h>

namespace transform::logging {

TEST(TransformLoggingRecordBatcherTest, TestMaxBytes) {
    constexpr size_t batch_max_bytes = 1_KiB;
    constexpr size_t total_bytes = batch_max_bytes * 10;
    record_batcher batcher{batch_max_bytes};

    // General goal here is to generate enough data that we'll need several
    // batches.
    auto bytes_remain = static_cast<ssize_t>(total_bytes);
    while (bytes_remain > 0) {
        auto k = testing::random_length_iobuf(batch_max_bytes / 2);
        bytes_remain -= static_cast<ssize_t>(k.size_bytes());
        auto v = testing::random_length_iobuf(batch_max_bytes / 2);
        bytes_remain -= static_cast<ssize_t>(v.size_bytes());
        batcher.append(std::move(k), std::move(v));
    }

    auto batches = batcher.finish();
    auto tot = batches.size();
    EXPECT_TRUE(!batches.empty());
    int i = 1;
    for (auto& b : batches) {
        EXPECT_LE(static_cast<size_t>(b.size_bytes()), batch_max_bytes)
          << fmt::format("Batch {}/{} size {}", i, tot, b.size_bytes());
        ++i;
    }
}

TEST(TransformLoggingRecordBatcherTest, TestReuseBatcher) {
    constexpr size_t batch_max_bytes = 1_KiB;
    record_batcher batcher{batch_max_bytes};

    for (int i = 0; i < 4; ++i) {
        auto k = testing::random_length_iobuf(batch_max_bytes / 4);
        auto v = testing::random_length_iobuf(batch_max_bytes / 4);
        auto ks = k.size_bytes();
        auto vs = v.size_bytes();
        batcher.append(std::move(k), std::move(v));

        // we haven't finalized any batches yet
        EXPECT_EQ(batcher.total_size_bytes(), 0);

        auto batches = batcher.finish();
        EXPECT_EQ(batches.size(), 1);
        EXPECT_GT(batches.back().size_bytes(), ks + vs);
    }
}

} // namespace transform::logging
