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
#include "cloud_topics/level_zero/pipeline/read_pipeline.h"
#include "cloud_topics/level_zero/pipeline/read_request.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>

namespace cloud_topics::l0 {
/// Read Request Scheduler
///
/// This is a simple fan-out scheduler for read requests.
/// It directs read requests to different shards based on the
/// object id. The requests that target the same object id will
/// always go to the same shard.
class read_request_scheduler
  : public ss::peering_sharded_service<read_request_scheduler> {
public:
    explicit read_request_scheduler(
      read_pipeline<ss::lowres_clock>::stage stage);

    ss::future<> start();

    ss::future<> stop();

private:
    ss::future<> bg_loop();

    /// Schedules request processing on the target shard.
    ///
    /// The method sends the request to the target shard.
    /// The 'target shard' is a shard that performs the processing of the
    /// request. The 'source shard' is a shard that owns the request. In some
    /// cases 'source shard' and 'target shard' can be the same. The source_req
    /// request is just propagated down the pipeline in this case.
    ///
    /// \param target Target shard (the shard that should process the request)
    /// \param source_req Request to process. The request is owned by the source
    /// shard.
    void schedule_on(
      read_request<ss::lowres_clock>& source_req, ss::shard_id target);

    ss::future<read_request<ss::lowres_clock>::response_t> proxy_read_request(
      const read_request<ss::lowres_clock>& source_req, ss::shard_id target);

    read_pipeline<ss::lowres_clock>::stage _stage;
    ss::gate _gate;
};
} // namespace cloud_topics::l0
