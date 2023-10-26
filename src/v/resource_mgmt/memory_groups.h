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

// centralized unit for memory management
class memory_groups {
public:
    static size_t kafka_total_memory();

    /// \brief includes raft & all services
    static size_t rpc_total_memory();

    /**
     * The target allocation size for the chunk cache. This is a soft target,
     * and may be expanded as needed by segment appenders, or reclaimed from by
     * seastar under memory pressure.
     *
     * add upper bound of 30 pct of memory as a hard outstanding limit.
     */
    static size_t chunk_cache_min_memory();

    /**
     * Upper bound on the amount of outstanding memory for inflight write
     * requests. Requests above this limit will wait for an existing chunk to be
     * returned to the cache.
     */
    static size_t chunk_cache_max_memory();

    static size_t recovery_max_memory();

    /// Max memory that both cloud storage uploads and read-path could use
    static size_t tiered_storage_max_memory();

private:
    /**
     * Total memory for a core after the user's Wasm reservation.
     */
    static size_t total_memory();
};
