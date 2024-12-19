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

#include "base/seastarx.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "utils/human.h"

#include <seastar/core/memory.hh>

#include <cstdint>
#include <stdexcept>

namespace {

bool wasm_enabled() {
    return config::shard_local_cfg().data_transforms_enabled.value()
           && !config::node().emergency_disable_data_transforms.value();
}

bool datalake_enabled() {
    return config::shard_local_cfg().iceberg_enabled.value();
}

struct memory_shares {
    constexpr static size_t chunk_cache = 15;
    constexpr static size_t kafka = 30;
    constexpr static size_t rpc = 20;
    constexpr static size_t recovery = 10;
    constexpr static size_t tiered_storage = 10;
    constexpr static size_t data_transforms = 10;
    constexpr static size_t datalake = 10;

    static size_t total_shares(bool with_wasm, bool with_datalake) {
        size_t total = chunk_cache + kafka + rpc + recovery + tiered_storage;
        if (with_wasm) {
            total += data_transforms;
        }
        if (with_datalake) {
            total += datalake;
        }
        return total;
    }
};

} // namespace

size_t
compaction_memory_reservation::reserved_bytes(size_t total_memory) const {
    size_t bytes_limit = total_memory * (max_limit_pct / 100.0);
    return std::min(max_bytes, bytes_limit);
}

size_t
partitions_memory_reservation::reserved_bytes(size_t total_memory) const {
    return total_memory * (max_limit_pct / 100.0);
}

system_memory_groups::system_memory_groups(
  size_t total_available_memory,
  compaction_memory_reservation compaction,
  bool wasm_enabled,
  bool datalake_enabled,
  partitions_memory_reservation partitions)
  : _compaction_reserved_memory(
      compaction.reserved_bytes(total_available_memory))
  , _partitions_reserved_memory(
      partitions.reserved_bytes(total_available_memory))
  , _total_system_memory(
      total_available_memory - _compaction_reserved_memory
      - _partitions_reserved_memory)
  , _wasm_enabled(wasm_enabled)
  , _datalake_enabled(datalake_enabled) {}

size_t system_memory_groups::chunk_cache_min_memory() const {
    return chunk_cache_max_memory() / 3;
}

size_t system_memory_groups::chunk_cache_max_memory() const {
    return subsystem_memory<memory_shares::chunk_cache>();
}

size_t system_memory_groups::kafka_total_memory() const {
    return subsystem_memory<memory_shares::kafka>();
}

size_t system_memory_groups::rpc_total_memory() const {
    return subsystem_memory<memory_shares::rpc>();
}

size_t system_memory_groups::recovery_max_memory() const {
    return subsystem_memory<memory_shares::recovery>();
}

size_t system_memory_groups::tiered_storage_max_memory() const {
    return subsystem_memory<memory_shares::tiered_storage>();
}

size_t system_memory_groups::data_transforms_max_memory() const {
    if (!_wasm_enabled) {
        return 0;
    }
    return subsystem_memory<memory_shares::data_transforms>();
}

size_t system_memory_groups::datalake_max_memory() const {
    if (!_datalake_enabled) {
        return 0;
    }
    return subsystem_memory<memory_shares::datalake>();
}

size_t system_memory_groups::partitions_max_memory() const {
    return _partitions_reserved_memory;
}

double system_memory_groups::partitions_max_memory_share() const {
    return _partitions_reserved_memory
           / static_cast<double>(ss::memory::stats().total_memory());
}

template<size_t shares>
size_t system_memory_groups::subsystem_memory() const {
    size_t per_share_amount = total_memory()
                              / memory_shares::total_shares(
                                _wasm_enabled, _datalake_enabled);
    return per_share_amount * shares;
}

size_t system_memory_groups::total_memory() const {
    return _total_system_memory;
}

void system_memory_groups::log_memory_group_allocations(seastar::logger& log) {
    log.info(
      "Per shard memory group allocations: total memory: {}, "
      "total memory minus pre-share reservations: {}, chunk cache: {}, kafka: "
      "{}, rpc: {}, recovery: {}, "
      "tiered storage: {}, data transforms: {}, compaction: {}, datalake: {}, "
      "partitions: {}",
      human::bytes(ss::memory::stats().total_memory()),
      human::bytes(total_memory()),
      human::bytes(chunk_cache_max_memory()),
      human::bytes(kafka_total_memory()),
      human::bytes(rpc_total_memory()),
      human::bytes(recovery_max_memory()),
      human::bytes(tiered_storage_max_memory()),
      human::bytes(data_transforms_max_memory()),
      human::bytes(compaction_reserved_memory()),
      human::bytes(datalake_max_memory()),
      human::bytes(partitions_max_memory()));
}

std::optional<system_memory_groups>& memory_groups_holder() {
    static thread_local std::optional<system_memory_groups> groups;
    return groups;
}

system_memory_groups& memory_groups() {
    auto& groups = memory_groups_holder();
    if (groups) {
        return *groups;
    }
    size_t total = ss::memory::stats().total_memory();
    bool wasm = wasm_enabled();
    const auto& cfg = config::shard_local_cfg();
    if (wasm) {
        size_t wasm_memory_reservation
          = cfg.data_transforms_per_core_memory_reservation.value();
        total -= wasm_memory_reservation;
    }
    compaction_memory_reservation compaction;
    if (cfg.log_compaction_use_sliding_window.value()) {
        compaction.max_bytes = cfg.storage_compaction_key_map_memory.value();
        compaction.max_limit_pct
          = cfg.storage_compaction_key_map_memory_limit_percent.value();
    }
    partitions_memory_reservation partitions{
      .max_limit_pct = cfg.topic_partitions_memory_allocation_percent()};
    groups.emplace(total, compaction, wasm, datalake_enabled(), partitions);
    return *groups;
}
