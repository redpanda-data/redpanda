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

#include "config/configuration.h"
#include "config/node_config.h"
#include "seastarx.h"

#include <seastar/core/memory.hh>

// centralized unit for memory management
class memory_groups {
public:
    static size_t kafka_total_memory() {
        // 30%
        return total_memory() * .30; // NOLINT
    }

    /// \brief includes raft & all services
    static size_t rpc_total_memory() {
        // 20%
        return total_memory() * .20; // NOLINT
    }

    /**
     * The target allocation size for the chunk cache. This is a soft target,
     * and may be expanded as needed by segment appenders, or reclaimed from by
     * seastar under memory pressure.
     *
     * add upper bound of 30 pct of memory as a hard outstanding limit.
     */
    static size_t chunk_cache_min_memory() {
        return total_memory() * .10; // NOLINT
    }

    /**
     * Upper bound on the amount of outstanding memory for inflight write
     * requests. Requests above this limit will wait for an existing chunk to be
     * returned to the cache.
     */
    static size_t chunk_cache_max_memory() {
        return total_memory() * .30; // NOLINT
    }

    static size_t recovery_max_memory() {
        return total_memory() * .10; // NOLINT
    }

    /// Max memory that both cloud storage uploads and read-path could use
    static size_t tiered_storage_max_memory() {
        return total_memory() * .10; // NOLINT
    }

private:
    /**
     * Total memory for a core after the user's Wasm reservation.
     */
    static size_t total_memory() {
        size_t total = ss::memory::stats().total_memory();
        if (
          config::shard_local_cfg().data_transforms_enabled.value()
          && !config::node().emergency_disable_data_transforms.value()) {
            size_t wasm_memory_reservation
              = config::shard_local_cfg()
                  .wasm_per_core_memory_reservation.value();
            total -= wasm_memory_reservation;
        }
        return total;
    }
};
