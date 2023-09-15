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

#include "seastarx.h"

#include <seastar/core/memory.hh>

// centralized unit for memory management
struct memory_groups {
    static size_t kafka_total_memory() {
        // 30%
        return ss::memory::stats().total_memory() * .30;
    }
    /// \brief includes raft & all services
    static size_t rpc_total_memory() {
        // 20%
        return ss::memory::stats().total_memory() * .20;
    }

    /**
     * The target allocation size for the chunk cache. This is a soft target,
     * and may be expanded as needed by segment appenders, or reclaimed from by
     * seastar under memory pressure.
     *
     * add upper bound of 30 pct of memory as a hard outstanding limit.
     */
    static size_t chunk_cache_min_memory() {
        return ss::memory::stats().total_memory() * .10; // NOLINT
    }

    /**
     * Upper bound on the amount of outstanding memory for inflight write
     * requests. Requests above this limit will wait for an existing chunk to be
     * returned to the cache.
     */
    static size_t chunk_cache_max_memory() {
        return ss::memory::stats().total_memory() * .30; // NOLINT
    }

    static size_t recovery_max_memory() {
        return ss::memory::stats().total_memory() * .10;
    }

    /// Max memory that both cloud storage uploads and read-path could use
    static size_t tiered_storage_max_memory() {
        return ss::memory::stats().total_memory() * .10;
    }
};
