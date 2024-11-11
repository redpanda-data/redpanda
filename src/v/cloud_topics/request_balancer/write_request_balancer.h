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

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/weak_ptr.hh>

#include <vector>

namespace experimental::cloud_topics {

/// Current resource utilization of the shard.
/// The instances of this struct are exchanged by all shards.
struct shard_resource_utilization {
    unsigned shard{ss::this_shard_id()};
    // TODO: implement
    // NOTE: currently, the monitoring of the 'remote' is implemented by
    // the 'cloud_storage::remote' and not available through the
    // 'cloud_io::remote'. We should move this mechanism to the 'cloud_io'
    // subsystem and then use it here to collect resource utilization
    // information.
};

class balancing_policy {
public:
    balancing_policy() = default;
    balancing_policy(const balancing_policy&) = default;
    balancing_policy(balancing_policy&&) = default;
    balancing_policy& operator=(const balancing_policy&) = default;
    balancing_policy& operator=(balancing_policy&&) = default;
    virtual ~balancing_policy() = default;

    /// Reorder the vector so the shard that should be chosen first
    /// goes first etc.
    virtual void rebalance(std::vector<shard_resource_utilization>&) = 0;
};

/// Balancing policy which choses shards based on shard ids (prefers pushing
/// write requests to shard 0).
class dummy_balancing_policy : public balancing_policy {
public:
    void rebalance(std::vector<shard_resource_utilization>&) override;
};

struct resource_balancer_accessor;

/// The resource balancer is monitoring the write_pipeline and redirects
/// write requests to the correct shard.
class write_request_balancer
  : public ss::peering_sharded_service<write_request_balancer> {
    using request_ptr = ss::weak_ptr<core::write_request<>>;

    friend struct resource_balancer_accessor;

public:
    write_request_balancer(
      core::write_pipeline<>& pipeline,
      std::unique_ptr<balancing_policy> policy);

    ss::future<> start();

    ss::future<> stop();

private:
    /// Run the load balancing for all available write requests
    ss::future<checked<bool, errc>> run_once() noexcept;

    /// Run background loop
    ss::future<> run_bg() noexcept;

    using foreign_resp_ptr = ss::foreign_ptr<
      ss::lw_shared_ptr<ss::circular_buffer<model::record_batch>>>;

    /// Make a copy of the write request and enqueue it
    ss::future<checked<foreign_resp_ptr, errc>>
    proxy_write_request(const core::write_request<>* req);

    void ack_write_response(
      core::write_request<>* req, checked<foreign_resp_ptr, errc> resp);

    ss::future<> roundtrip(ss::shard_id shard, core::write_request<>& req);

    std::vector<shard_resource_utilization> _shards;
    core::write_pipeline<>& _pipeline;
    core::pipeline_stage _my_stage;
    std::unique_ptr<balancing_policy> _policy;
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace experimental::cloud_topics
