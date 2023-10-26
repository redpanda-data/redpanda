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

#include "config/configuration.h"
#include "config/node_config.h"
#include "seastarx.h"

#include <seastar/core/memory.hh>

namespace {
bool wasm_enabled() {
    return config::shard_local_cfg().data_transforms_enabled.value()
           && !config::node().emergency_disable_data_transforms.value();
}
} // namespace

size_t memory_groups::kafka_total_memory() {
    // 30%
    return total_memory() * .30; // NOLINT
}

size_t memory_groups::rpc_total_memory() {
    // 20%
    return total_memory() * .20; // NOLINT
}

size_t memory_groups::chunk_cache_min_memory() {
    return total_memory() * .10; // NOLINT
}

size_t memory_groups::chunk_cache_max_memory() {
    return total_memory() * .30; // NOLINT
}

size_t memory_groups::recovery_max_memory() {
    return total_memory() * .10; // NOLINT
}

size_t memory_groups::tiered_storage_max_memory() {
    return total_memory() * .10; // NOLINT
}

size_t memory_groups::total_memory() {
    size_t total = ss::memory::stats().total_memory();
    if (wasm_enabled()) {
        size_t wasm_memory_reservation
          = config::shard_local_cfg().wasm_per_core_memory_reservation.value();
        total -= wasm_memory_reservation;
    }
    return total;
}
