/*
 * Copyright 2020 Vectorized, Inc.
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
        // 30%
        return ss::memory::stats().total_memory() * .30;
    }
    static size_t reserved_unused_total_memory() {
        return ss::memory::stats().total_memory() * .40;
    }
};
