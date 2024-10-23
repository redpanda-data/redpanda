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
#include "utils/token_bucket.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/weak_ptr.hh>

namespace experimental::cloud_topics {

struct throttler_accessor;

/// Throttler
///
/// This component manages write throughput. It monitors
/// write_pipeline and adjusts the token bucket. If more
/// requests arrive than the the tb allows it removes
/// some write requests out of the pipeline temporarily
/// and then returns them back. The request which was withheld
/// by the throttler could expire.
template<class Clock>
class throttler {
    friend struct throttler_accessor;

public:
    // TODO: add config properties for limits
    explicit throttler(
      size_t tput_limit,
      core::write_pipeline<Clock>&); // TODO: add read_pipeline

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> bg_throttle_write_pipeline();

    /// Run throttling loop once
    /// Wait until there is something in the write pipeline and apply the
    /// throttling logic (account for mem/tput and apply throttling/culling
    /// if needed).
    ///
    /// \param prev_total_size_bytes is the last total received from the
    ///        write_pipeline
    /// \return updated size total
    ss::future<result<size_t>>
    throttle_write_pipeline_once(size_t prev_total_size_bytes);

    /// Apply throttling logic to the write pipeline
    size_t
    apply_throttle(size_t prev_total_size_bytes, const core::event& event);

    /// Throttle write throughput
    ///
    /// Move some write requests out of the pipeline.
    /// This makes them unavailable for other pipeline
    /// consumers and effectively throttles the workload.
    void throttle_tput(size_t);

    core::write_pipeline<Clock>& _pipeline;
    token_bucket<Clock> _write_tput_tb;

    using write_req_ptr = ss::weak_ptr<core::write_request<Clock>>;
    ss::abort_source _as;
    ss::gate _gate;
    core::pipeline_stage _my_stage;
    // Total number of events handled
    size_t _total_events{0};
    // Number of outstanding throttled write requests
    size_t _outstanding_throttled_requests{0};
    // Number of times the pipeline was throttled by tput
    size_t _throttle_by_tput{0};
};
} // namespace experimental::cloud_topics
