/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_topics/core/write_pipeline.h"
#include "cloud_topics/core/write_request.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/weak_ptr.hh>

namespace experimental::cloud_topics {

struct evictor_accessor;

/// Throttler
///
class evictor {
    friend struct evictor_accessor;

public:
    // TODO: add config property
    explicit evictor(size_t mem_limit, core::write_pipeline<>&);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> bg_evict_pipeline();

    ss::future<result<size_t>> evict_once(size_t prev_total_size_bytes);

    size_t do_evict(size_t prev_total_size_bytes, const core::event& event);

    /// Throttle write request by memory usage.
    ///
    /// Dispose the latest write requests and
    /// return number of bytes freed.
    size_t throttle_memory(size_t current_bytes_pending);

    core::write_pipeline<>& _pipeline;

    size_t _mem_limit;

    ss::abort_source _as;
    ss::gate _gate;
    core::pipeline_stage _my_stage;

    // Total number of events handled
    size_t _total_events{0};

    // Number of times the pipeline was throttled by memory
    size_t _throttle_by_mem{0};
};
} // namespace experimental::cloud_topics
