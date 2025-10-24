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

#include "base/units.h"
#include "kafka/server/fetch_metadata_cache.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

TEST_CORO(fetch_metadata_cache_tests, test_avg_bytes_per_offset) {
    kafka::fetch_metadata_cache mdc{};
    EXPECT_EQ(mdc.size(), 0);

    const auto ktp = model::ktp_with_hash{
      model::topic{"test_topic"}, model::partition_id{1}};

    mdc.insert_or_assign(
      ktp, model::offset(0), model::offset(10), model::offset(10), 0, 0, 0);
    auto res = mdc.get(ktp).value();
    EXPECT_EQ(res.avg_bytes_per_offset, 0);
    EXPECT_EQ(mdc.size(), 1);

    mdc.insert_or_assign(
      ktp,
      model::offset(0),
      model::offset(15),
      model::offset(15),
      5,
      1,
      5 * 1_MiB);
    res = mdc.get(ktp).value();
    EXPECT_EQ(res.avg_bytes_per_offset, 1_MiB);
    EXPECT_EQ(res.avg_bytes_per_batch, 5_MiB);

    mdc.insert_or_assign(
      ktp, model::offset(0), model::offset(15), model::offset(15), 1, 1, 2_MiB);
    res = mdc.get(ktp).value();
    EXPECT_GE(res.avg_bytes_per_offset, 1_MiB);
    EXPECT_LT(res.avg_bytes_per_batch, 5_MiB);
    co_return;
}
