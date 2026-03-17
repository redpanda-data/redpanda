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
#include "cloud_topics/level_zero/pipeline/pipeline_actor.h"
#include "cloud_topics/level_zero/pipeline/read_pipeline.h"
#include "cloud_topics/level_zero/pipeline/read_request.h"

#include <seastar/core/sharded.hh>

namespace cloud_topics::l0 {
/// Read Request Scheduler
///
/// This is a simple fan-out scheduler for read requests.
/// It directs read requests to different shards based on the
/// object id. The requests that target the same object id will
/// always go to the same shard.
class read_request_scheduler
  : public ss::peering_sharded_service<read_request_scheduler>
  , public read_pipeline_actor<> {
    using actor_t = read_pipeline_actor<>;

public:
    explicit read_request_scheduler(
      read_pipeline<ss::lowres_clock>::stage stage);

    ss::future<> start() override;

    ss::future<> stop() override;

protected:
    ss::future<> process(pipeline_notification msg) override;
    void on_error(std::exception_ptr e) noexcept override;

private:
    /// Schedules request processing on the target shard.
    void schedule_on(
      read_request<ss::lowres_clock>& source_req, ss::shard_id target);

    ss::future<read_request<ss::lowres_clock>::response_t> proxy_read_request(
      const read_request<ss::lowres_clock>& source_req, ss::shard_id target);
};
} // namespace cloud_topics::l0
