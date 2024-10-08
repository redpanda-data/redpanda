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
#include "resource_mgmt/memory_groups.h"
#include "utils/human.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

static constexpr size_t total_shares_without_optionals = 10;
static constexpr size_t total_wasm_shares = 1;
static constexpr size_t total_datalake_shares = 1;

// It's not really useful to know the exact byte values for each of these
// numbers so we just make sure we're within a MB
MATCHER_P(IsApprox, n, "") {
    *result_listener << "expected " << human::bytes(arg)
                     << " to be within 1MiB of " << human::bytes(n);
    size_t low = n - 1_MiB;
    size_t high = n + 1_MiB;
    return arg >= low && arg <= high;
}

class MemoryGroupSharesTest
  : public ::testing::Test
  , public ::testing::WithParamInterface<std::tuple<bool, bool, bool>> {
public:
    static constexpr size_t total_memory = 2_GiB;
    static constexpr size_t user_wasm_reservation = 20_MiB;
    static constexpr size_t user_compaction_reservation = 20_MiB;

    bool compaction_enabled() const { return std::get<0>(GetParam()); }
    bool wasm_enabled() const { return std::get<1>(GetParam()); }
    bool datalake_enabled() const { return std::get<2>(GetParam()); }
};

TEST_P(MemoryGroupSharesTest, DividesSharesCorrectly) {
    auto total_available_memory = total_memory;
    auto total_system_memory = total_memory;
    if (wasm_enabled()) {
        total_system_memory -= user_wasm_reservation;
        total_available_memory -= user_wasm_reservation;
    }
    compaction_memory_reservation reservation{};
    if (compaction_enabled()) {
        total_available_memory -= user_compaction_reservation;
        reservation = {
          .max_bytes = user_compaction_reservation, .max_limit_pct = 100};
    }

    class system_memory_groups groups(
      total_system_memory, reservation, wasm_enabled(), datalake_enabled());
    auto total_shares = total_shares_without_optionals;
    if (wasm_enabled()) {
        total_shares += total_wasm_shares;
    }
    if (datalake_enabled()) {
        total_shares += total_datalake_shares;
    }
    EXPECT_THAT(
      groups.chunk_cache_min_memory(),
      IsApprox(total_available_memory * 1.0 / total_shares));
    EXPECT_THAT(
      groups.chunk_cache_max_memory(),
      IsApprox(total_available_memory * 3.0 / total_shares));
    EXPECT_THAT(
      groups.tiered_storage_max_memory(),
      IsApprox(total_available_memory * 1.0 / total_shares));
    EXPECT_THAT(
      groups.recovery_max_memory(),
      IsApprox(total_available_memory * 1.0 / total_shares));
    EXPECT_THAT(
      groups.kafka_total_memory(),
      IsApprox(total_available_memory * 3.0 / total_shares));
    EXPECT_THAT(
      groups.rpc_total_memory(),
      IsApprox(total_available_memory * 2.0 / total_shares));
    if (wasm_enabled()) {
        EXPECT_THAT(
          groups.data_transforms_max_memory(),
          IsApprox(total_available_memory * 1.0 / total_shares));
    } else {
        EXPECT_THAT(groups.data_transforms_max_memory(), 0);
    }
    if (datalake_enabled()) {
        EXPECT_THAT(
          groups.datalake_max_memory(),
          IsApprox(total_available_memory * 1.0 / total_shares));
    } else {
        EXPECT_THAT(groups.datalake_max_memory(), 0);
    }

    if (compaction_enabled()) {
        EXPECT_EQ(
          groups.compaction_reserved_memory(), user_compaction_reservation);
    } else {
        EXPECT_EQ(groups.compaction_reserved_memory(), 0);
    }
    EXPECT_LE(
      groups.data_transforms_max_memory() + groups.chunk_cache_max_memory()
        + groups.kafka_total_memory() + groups.recovery_max_memory()
        + groups.rpc_total_memory() + groups.tiered_storage_max_memory()
        + groups.datalake_max_memory(),
      total_available_memory);
}

TEST(MemoryGroups, CompactionMemoryBytes) {
    constexpr size_t total_memory = 2_GiB;
    constexpr size_t compaction_max_bytes = 1_GiB;
    for (auto pct = 50; pct <= 100; pct++) {
        // Configure the percent limit just above the bytes-configured value.
        // We should be capped.
        system_memory_groups groups(
          total_memory,
          /*compaction_memory_reservation=*/
          {
            .max_bytes = compaction_max_bytes,
            .max_limit_pct = double(pct),
          },
          /*wasm_enabled=*/false,
          /*datalake_enabled=*/false);
        EXPECT_EQ(1_GiB, groups.compaction_reserved_memory());
    }
    for (auto pct = 1; pct <= 49; pct++) {
        // Below we shouldn't be capped.
        system_memory_groups groups(
          total_memory,
          /*compaction_memory_reservation=*/
          {
            .max_bytes = compaction_max_bytes,
            .max_limit_pct = double(pct),
          },
          /*wasm_enabled=*/false,
          /*datalake_enabled=*/false);
        EXPECT_EQ(
          total_memory * pct / 100, groups.compaction_reserved_memory());
    }
}

INSTANTIATE_TEST_SUITE_P(
  MemoryGroupShares,
  MemoryGroupSharesTest,
  ::testing::Combine(::testing::Bool(), ::testing::Bool(), ::testing::Bool()));
