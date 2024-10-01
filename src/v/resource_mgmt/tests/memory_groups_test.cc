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

static constexpr size_t total_shares_without_transforms = 11;
static constexpr size_t total_shares_with_transforms = 12;

// It's not really useful to know the exact byte values for each of these
// numbers so we just make sure we're within a MB
MATCHER_P(IsApprox, n, "") {
    *result_listener << "expected " << human::bytes(arg)
                     << " to be within 1MiB of " << human::bytes(n);
    size_t low = n - 1_MiB;
    size_t high = n + 1_MiB;
    return arg >= low && arg <= high;
}

TEST(MemoryGroups, HasCompatibility) {
    class system_memory_groups groups(
      2_GiB, /*compaction_memory_reservation=*/{}, /*wasm_enabled=*/false);
    auto shares = total_shares_without_transforms;
    EXPECT_THAT(
      groups.chunk_cache_min_memory(), IsApprox(2_GiB * 1.0 / shares));
    EXPECT_THAT(
      groups.tiered_storage_max_memory(), IsApprox(2_GiB * 1.0 / shares));
    EXPECT_THAT(groups.recovery_max_memory(), IsApprox(2_GiB * 1.0 / shares));
    // These round differently than the original calculation.
    EXPECT_THAT(
      groups.chunk_cache_max_memory(), IsApprox(2_GiB * 3.0 / shares));
    EXPECT_THAT(groups.kafka_total_memory(), IsApprox(2_GiB * 3.0 / shares));
    EXPECT_THAT(groups.rpc_total_memory(), IsApprox(2_GiB * 2.0 / shares));
    EXPECT_THAT(groups.datalake_max_memory(), IsApprox(2_GiB * 1.0 / shares));
    EXPECT_THAT(groups.data_transforms_max_memory(), 0);

    EXPECT_EQ(0, groups.compaction_reserved_memory());
}

TEST(MemoryGroups, DividesSharesWithWasm) {
    constexpr size_t user_wasm_reservation = 20_MiB;
    auto total_memory = 2_GiB - user_wasm_reservation;
    class system_memory_groups groups(
      total_memory,
      /*compaction_memory_reservation=*/{},
      /*wasm_enabled=*/true);
    auto shares = total_shares_with_transforms;
    EXPECT_THAT(
      groups.chunk_cache_min_memory(), IsApprox(total_memory * 1.0 / shares));
    EXPECT_THAT(
      groups.chunk_cache_max_memory(), IsApprox(total_memory * 3.0 / shares));
    EXPECT_THAT(
      groups.tiered_storage_max_memory(),
      IsApprox(total_memory * 1.0 / shares));
    EXPECT_THAT(
      groups.recovery_max_memory(), IsApprox(total_memory * 1.0 / shares));
    EXPECT_THAT(
      groups.kafka_total_memory(), IsApprox(total_memory * 3.0 / shares));
    EXPECT_THAT(
      groups.rpc_total_memory(), IsApprox(total_memory * 2.0 / shares));
    EXPECT_THAT(
      groups.data_transforms_max_memory(),
      IsApprox(total_memory * 1.0 / shares));
    EXPECT_THAT(
      groups.datalake_max_memory(), IsApprox(total_memory * 1.0 / shares));
    EXPECT_LE(
      groups.data_transforms_max_memory() + groups.chunk_cache_max_memory()
        + groups.kafka_total_memory() + groups.recovery_max_memory()
        + groups.rpc_total_memory() + groups.tiered_storage_max_memory()
        + groups.datalake_max_memory(),
      2_GiB - user_wasm_reservation);

    EXPECT_EQ(0, groups.compaction_reserved_memory());
}

TEST(MemoryGroups, DividesSharesWithCompaction) {
    constexpr size_t compaction_reserved_memory = 20_MiB;
    class system_memory_groups groups(
      2_GiB,
      /*compaction_memory_reservation=*/
      {.max_bytes = compaction_reserved_memory, .max_limit_pct = 100.0},
      /*wasm_enabled=*/true);
    auto total_memory = 2_GiB - compaction_reserved_memory;
    auto shares = total_shares_with_transforms;
    EXPECT_THAT(
      groups.chunk_cache_min_memory(), IsApprox(total_memory * 1.0 / shares));
    EXPECT_THAT(
      groups.chunk_cache_max_memory(), IsApprox(total_memory * 3.0 / shares));
    EXPECT_THAT(
      groups.tiered_storage_max_memory(),
      IsApprox(total_memory * 1.0 / shares));
    EXPECT_THAT(
      groups.recovery_max_memory(), IsApprox(total_memory * 1.0 / shares));
    EXPECT_THAT(
      groups.kafka_total_memory(), IsApprox(total_memory * 3.0 / shares));
    EXPECT_THAT(
      groups.rpc_total_memory(), IsApprox(total_memory * 2.0 / shares));
    EXPECT_THAT(
      groups.data_transforms_max_memory(),
      IsApprox(total_memory * 1.0 / shares));
    EXPECT_THAT(
      groups.datalake_max_memory(), IsApprox(total_memory * 1.0 / shares));
    EXPECT_LE(
      groups.data_transforms_max_memory() + groups.chunk_cache_max_memory()
        + groups.kafka_total_memory() + groups.recovery_max_memory()
        + groups.rpc_total_memory() + groups.tiered_storage_max_memory()
        + groups.datalake_max_memory(),
      2_GiB - compaction_reserved_memory);

    EXPECT_EQ(compaction_reserved_memory, groups.compaction_reserved_memory());
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
          /*wasm_enabled=*/false);
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
          /*wasm_enabled=*/false);
        EXPECT_EQ(
          total_memory * pct / 100, groups.compaction_reserved_memory());
    }
}
