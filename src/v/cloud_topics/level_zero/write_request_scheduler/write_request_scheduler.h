/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_topics/level_zero/pipeline/write_pipeline.h"
#include "cloud_topics/level_zero/pipeline/write_request.h"
#include "config/property.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/weak_ptr.hh>

#include <chrono>
#include <expected>
#include <future>

namespace cloud_topics::l0 {

/// The scheduler decides the which shard to use to handle write
/// requests. It also batches requests.
///
/// The write_request_scheduler is a pipeline component that implements the
/// "parallel batcher" design. It is placed before the batcher in the pipeline
/// and acts as a peering_sharded_service, redirecting write requests to
/// batchers for upload. It can move write requests between shards to maximize
/// the size of the uploaded objects while keeping the latency bounded.
///
/// The scheduler uses two policies to decide when to trigger uploads:
///
/// - **Data threshold policy** — Forces a shard to upload immediately once its
/// local data reaches a set threshold. This operates per shard and does not
/// cross shard boundaries.
///
/// - **Time-based fallback policy** — Runs only on shard 0. After N ms, it
/// forces all shards to send write requests to a single target shard for
/// upload. The target shard is chosen based on having the most data in the
/// pipeline to minimize CPU cache invalidation.
class write_request_scheduler
  : public ss::peering_sharded_service<write_request_scheduler> {
    friend struct write_request_balancer_accessor;

public:
    explicit write_request_scheduler(write_pipeline<>::stage s);

    ss::future<> start();

    ss::future<> stop();

private:
    /// The fiber that runs on a shard 0 and schedules
    /// write requests based on timeout. It works
    /// across all shards at the same time. When it wakes up
    /// it collects write requests and triggers
    /// uploads.
    ss::future<> bg_time_based_fallback();
    /// This method implements the actual fallback mechanism.
    /// It is invoked by the 'bg_time_based_fallback' method.
    ss::future<> apply_time_based_fallback();

    /// This fiber runs on every shard and is triggered by
    /// the data accumulation threshold. If enough data is accumulated
    /// on a shard this shard will trigger the upload without going
    /// cross shard.
    ss::future<> bg_data_threshold();

    /// Run the load balancing for all available write requests
    ss::future<checked<bool, errc>> run_once() noexcept;

    /// Simply count number of bytes in the queue
    size_t shard_bytes() noexcept;

    using foreign_ptr_t
      = ss::foreign_ptr<ss::lw_shared_ptr<chunked_vector<extent_meta>>>;

    /// Make a copy of the write request and enqueue it
    ss::future<std::expected<foreign_ptr_t, errc>>
    proxy_write_request(write_request<>* req) noexcept;

    ss::future<>
    roundtrip(ss::shard_id shard, write_pipeline<>::write_requests_list list);

    void ack_write_response(
      write_request<>* req, std::expected<foreign_ptr_t, errc> resp);

    /// Forward all write requests to the target shard
    ss::future<> forward_to(ss::shard_id shard);

    write_pipeline<>::stage _stage;
    ss::abort_source _as;
    ss::gate _gate;

    // This field is used in tests to limit the uploads to time based fallback
    bool _test_only_disable_data_threshold{false};
    // This field is used in tests to limit the uploads to data threshold
    bool _test_only_disable_time_based_fallback{false};

    config::binding<size_t> _max_buffer_size;
    config::binding<size_t> _max_cardinality;
    config::binding<std::chrono::milliseconds> _scheduling_interval;
};

} // namespace cloud_topics::l0
