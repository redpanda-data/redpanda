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
#include "config/property.h"
#include "seastarx.h"
#include "ssx/semaphore.h"

#include <seastar/util/noncopyable_function.hh>

namespace raft {
/**
 * Thread local memory quota for raft recovery
 */
class recovery_memory_quota {
public:
    struct configuration {
        config::binding<std::optional<size_t>> max_recovery_memory;
        config::binding<size_t> default_read_buffer_size;
    };
    using config_provider_fn = ss::noncopyable_function<configuration()>;

    explicit recovery_memory_quota(config_provider_fn);

    ss::future<ssx::semaphore_units> acquire_read_memory();

private:
    void on_max_memory_changed();

    configuration _cfg;
    size_t _current_max_recovery_mem;
    ssx::semaphore _memory;
};

} // namespace raft
