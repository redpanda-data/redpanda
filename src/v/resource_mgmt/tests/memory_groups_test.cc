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

#include "resource_mgmt/memory_groups.h"
#include "units.h"
#include "utils/human.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

TEST(MemoryGroups, HasCompatibility) {
    class system_memory_groups groups(2_GiB, /*wasm_enabled=*/false);
    EXPECT_THAT(groups.chunk_cache_min_memory(), 2_GiB * .1);
    EXPECT_THAT(groups.tiered_storage_max_memory(), 2_GiB * .1);
    EXPECT_THAT(groups.recovery_max_memory(), 2_GiB * .1);
    // These round differently than the original calculation.
    EXPECT_THAT(groups.chunk_cache_max_memory(), (2_GiB * .3) - 2);
    EXPECT_THAT(groups.kafka_total_memory(), (2_GiB * .3) - 2);
    EXPECT_THAT(groups.rpc_total_memory(), (2_GiB * .2) - 1);
    EXPECT_THAT(groups.data_transforms_max_memory(), 0);
}

// It's not really useful to know the exact byte values for each of these
// numbers so we just make sure we're within a MB
MATCHER_P(IsApprox, n, "") {
    *result_listener << "expected " << human::bytes(arg)
                     << " to be within 1MiB of " << human::bytes(n);
    size_t low = n - 1_MiB;
    size_t high = n + 1_MiB;
    return arg >= low && arg <= high;
}

TEST(MemoryGroups, DividesSharesWithWasm) {
    constexpr size_t user_wasm_reservation = 20_MiB;
    class system_memory_groups groups(
      2_GiB - user_wasm_reservation, /*wasm_enabled=*/true);
    EXPECT_THAT(groups.chunk_cache_min_memory(), IsApprox(184_MiB));
    EXPECT_THAT(groups.chunk_cache_max_memory(), IsApprox(553_MiB));
    EXPECT_THAT(groups.tiered_storage_max_memory(), IsApprox(184_MiB));
    EXPECT_THAT(groups.recovery_max_memory(), IsApprox(184_MiB));
    EXPECT_THAT(groups.kafka_total_memory(), IsApprox(553_MiB));
    EXPECT_THAT(groups.rpc_total_memory(), IsApprox(368_MiB));
    EXPECT_THAT(groups.data_transforms_max_memory(), IsApprox(184_MiB));
    EXPECT_LT(
      groups.data_transforms_max_memory() + groups.chunk_cache_max_memory()
        + groups.kafka_total_memory() + groups.recovery_max_memory()
        + groups.rpc_total_memory() + groups.tiered_storage_max_memory(),
      2_GiB - user_wasm_reservation);
}
