/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <cstddef>
#include <optional>

/**
 * Configurations to reserve memory for compaction.
 */
struct compaction_memory_reservation {
    /**
     * Computes the compaction memory to reserve, based on configs and amount
     * of reservable memory (i.e. not use by other subsystems like WASM).
     */
    size_t reserved_bytes(size_t total_memory) const;

    // Maximum amount of memory in bytes to reserve for compaction.
    size_t max_bytes{0};

    // Limit on compaction memory expressed as percent of total system memory.
    double max_limit_pct{100.0};
};

/**
 * Centralized unit for memory management.
 *
 * Works via a share system. First we subtract the amount of memory the user
 * decides to reserve for their WebAssembly functions and for compaction
 * buffers. The remaining subsystems are distributed memory via a share system.
 */
class system_memory_groups {
public:
    system_memory_groups(
      size_t total_available_memory,
      compaction_memory_reservation compaction,
      bool wasm_enabled,
      bool datalake_enabled);

    size_t kafka_total_memory() const;

    /// \brief includes raft & all services
    size_t rpc_total_memory() const;

    /**
     * The target allocation size for the chunk cache. This is a soft target,
     * and may be expanded as needed by segment appenders, or reclaimed from by
     * seastar under memory pressure.
     */
    size_t chunk_cache_min_memory() const;

    /**
     * Upper bound on the amount of outstanding memory for inflight write
     * requests. Requests above this limit will wait for an existing chunk to be
     * returned to the cache.
     */
    size_t chunk_cache_max_memory() const;

    size_t recovery_max_memory() const;

    /// Max memory that both cloud storage uploads and read-path could use
    size_t tiered_storage_max_memory() const;

    /// Max memory that data transform subsystem should use.
    size_t data_transforms_max_memory() const;

    size_t compaction_reserved_memory() const {
        return _compaction_reserved_memory;
    }

    size_t datalake_max_memory() const;

private:
    /**
     * Total memory for a core after the user's Wasm and compaction
     * reservations.
     */
    size_t total_memory() const;
    /**
     * The fraction of memory for this subsystem based on the number of shares
     * allotted to it.
     */
    template<size_t shares>
    size_t subsystem_memory() const;

    size_t _compaction_reserved_memory;
    size_t _total_system_memory;
    bool _wasm_enabled;
    bool _datalake_enabled;
};

/**
 * Grab the shard local, lazily initialized, system memory groups.
 */
system_memory_groups& memory_groups();
