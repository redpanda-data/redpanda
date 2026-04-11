/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/common/level_zero_probe.h"
#include "cloud_topics/level_zero/pipeline/read_pipeline.h"
#include "cloud_topics/types.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_future.hh>

#include <optional>

namespace cloud_topics::l0 {

/// Merges concurrent read requests targeting the same L0 object.
///
/// Keys on exact object_id (no hash collisions) and has no fixed
/// timeout — the wait scales to actual download time.
template<class Clock = ss::lowres_clock>
class read_merge {
public:
    explicit read_merge(read_pipeline<Clock>::stage);
    ss::future<> start();
    ss::future<> stop();

private:
    using semaphore_units
      = seastar::semaphore_units<ss::named_semaphore_exception_factory, Clock>;
    ss::future<> bg_loop();
    ss::future<>
    process_single_request(read_request<Clock>* req, semaphore_units u);

    ss::gate _gate;
    read_pipeline<Clock>::stage _pipeline_stage;
    read_merge_probe _probe;

    /// Tracks in-flight downloads by object_id.
    /// The shared_promise resolves with std::nullopt on success,
    /// or an errc value on failure. Waiting requests use the
    /// shared_future to decide whether to proceed (cache hit)
    /// or propagate the error.
    chunked_hash_map<object_id, ss::shared_promise<std::optional<errc>>>
      _in_flight;

    ssx::named_semaphore<Clock> _in_flight_sem;
};
} // namespace cloud_topics::l0
