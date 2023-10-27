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

struct memory_shares {
    constexpr static size_t kafka = 3;
    constexpr static size_t rpc = 2;
    constexpr static size_t recovery = 1;
    constexpr static size_t tiered_storage = 1;
    constexpr static size_t data_transforms = 1;

    static size_t total_shares(bool with_wasm) {
        size_t total = kafka + rpc + recovery + tiered_storage;
        if (with_wasm) {
            total += data_transforms;
        }
        return total;
    }
};

} // namespace

system_memory_groups::system_memory_groups(
  size_t total_system_memory, bool wasm_enabled)
  : _total_system_memory(total_system_memory)
  , _wasm_enabled(wasm_enabled) {}

size_t system_memory_groups::chunk_cache_min_memory() {
    return total_memory() * .10; // NOLINT
}

size_t system_memory_groups::chunk_cache_max_memory() {
    return total_memory() * .30; // NOLINT
}

size_t system_memory_groups::kafka_total_memory() {
    return subsystem_memory<memory_shares::kafka>();
}

size_t system_memory_groups::rpc_total_memory() {
    return subsystem_memory<memory_shares::rpc>();
}

size_t system_memory_groups::recovery_max_memory() {
    return subsystem_memory<memory_shares::recovery>();
}

size_t system_memory_groups::tiered_storage_max_memory() {
    return subsystem_memory<memory_shares::tiered_storage>();
}

size_t system_memory_groups::data_transforms_max_memory() {
    if (!_wasm_enabled) {
        return 0;
    }
    return subsystem_memory<memory_shares::data_transforms>();
}

template<size_t shares>
size_t system_memory_groups::subsystem_memory() {
    size_t remaining = total_memory() - chunk_cache_max_memory();
    size_t per_share_amount = remaining
                              / memory_shares::total_shares(_wasm_enabled);
    return per_share_amount * shares;
}

size_t system_memory_groups::total_memory() { return _total_system_memory; }

system_memory_groups memory_groups() {
    size_t total = ss::memory::stats().total_memory();
    if (wasm_enabled()) {
        size_t wasm_memory_reservation
          = config::shard_local_cfg().wasm_per_core_memory_reservation.value();
        total -= wasm_memory_reservation;
    }
    return {total, wasm_enabled()};
}
