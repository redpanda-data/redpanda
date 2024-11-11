/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/request_balancer/write_request_balancer.h"

#include "base/unreachable.h"
#include "cloud_topics/core/write_pipeline.h"
#include "cloud_topics/core/write_request.h"
#include "cloud_topics/logger.h"
#include "ssx/future-util.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/coroutine/as_future.hh>

#include <algorithm>
#include <chrono>
#include <limits>

namespace experimental::cloud_topics {

void dummy_balancing_policy::rebalance(
  std::vector<shard_resource_utilization>& shards) {
    std::sort(
      shards.begin(),
      shards.end(),
      [](
        const shard_resource_utilization& lhs,
        const shard_resource_utilization& rhs) {
          return lhs.shard < rhs.shard;
      });
}

write_request_balancer::write_request_balancer(
  core::write_pipeline<>& pipeline, std::unique_ptr<balancing_policy> policy)
  : _pipeline(pipeline)
  , _my_stage(_pipeline.register_pipeline_stage())
  , _policy(std::move(policy)) {
    for (auto cpu : ss::smp::all_cpus()) {
        _shards.push_back({.shard = cpu});
    }
}

ss::future<> write_request_balancer::start() {
    ssx::repeat_until_gate_closed_or_aborted(
      _gate, _as, [this] { return run_bg(); });
    co_return;
}

ss::future<> write_request_balancer::stop() {
    _as.request_abort();
    return _gate.close();
}

ss::future<> write_request_balancer::run_bg() noexcept {
    core::event_filter<ss::lowres_clock> filter(
      core::event_type::new_write_request, _my_stage);
    auto event = co_await _pipeline.subscribe(filter, _as);
    switch (event.type) {
    case core::event_type::shutting_down:
        co_return;
    case core::event_type::err_timedout:
    case core::event_type::new_read_request:
    case core::event_type::none:
        unreachable();
    case core::event_type::new_write_request:
        break;
    }
    auto fut = co_await ss::coroutine::as_future(run_once());
    if (fut.failed()) {
        auto ep = fut.get_exception();
        if (!ssx::is_shutdown_exception(ep)) {
            vlog(cd_log.error, "Failed to proxy write request: {}", ep);
        }
    } else if (fut.get().has_failure()) {
        vlog(
          cd_log.error, "Failed to proxy write request: {}", fut.get().error());
    }
}

static inline core::serialized_chunk
deep_copy(const core::serialized_chunk& chunk) {
    core::serialized_chunk copy;
    copy.batches = chunk.batches.copy();
    copy.payload = chunk.payload.copy();
    return copy;
}

static inline ss::circular_buffer<model::record_batch>
deep_copy(const ss::circular_buffer<model::record_batch>& batches) {
    ss::circular_buffer<model::record_batch> res;
    for (const auto& b : batches) {
        res.push_back(b.copy());
    }
    return res;
}

ss::future<checked<bool, errc>> write_request_balancer::run_once() noexcept {
    _policy->rebalance(_shards);
    if (_shards.front().shard == ss::this_shard_id()) {
        // Fast path, no need to rebalance requests.
        _pipeline.process_stage(
          [](core::write_request<>&) noexcept
          -> checked<core::write_request_process_result, errc> {
              return core::write_request_process_result{
                .stop_iteration = ss::stop_iteration::no,
                .advance_next_stage = true,
              };
          },
          _my_stage);
        co_return false;
    }
    auto requests = _pipeline.get_write_requests(
      std::numeric_limits<size_t>::max(), _my_stage);
    // For every request:
    // - make a proxy on a target shard
    // - submit it to the pipeline on that shard bypassing
    //   the load balancer
    // - extract result from the proxy request and put it
    //   into the original one
    for (auto& request : requests.ready) {
        // The request is stored on the stack of one of the fibers and its
        // lifetime is defined by the promise that it contains. The request will
        // be alive until the promise is set. After its set all bets are off.
        // Since the request is extracted out of the pipeline nothing can set
        // the promise except the resource_balancer.
        auto target_shard = _shards.front().shard;
        // TODO: use zero copy mechanism
        // NOTE: it's OK to background because the pipeline has throttler which
        // handles memory pressure
        ssx::background = roundtrip(target_shard, request);
    }
    co_return true;
}

ss::future<> write_request_balancer::roundtrip(
  ss::shard_id shard, core::write_request<>& req) {
    vassert(shard != ss::this_shard_id(), "Can't roundtrip using one shard");
    auto resp = co_await container().invoke_on(
      shard, [&req](write_request_balancer& balancer) {
          return balancer.proxy_write_request(&req);
      });
    ack_write_response(&req, std::move(resp));
}

ss::future<checked<write_request_balancer::foreign_resp_ptr, errc>>
write_request_balancer::proxy_write_request(const core::write_request<>* req) {
    // The request was created on another shard.
    auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
      req->expiration_time - req->ingestion_time);
    core::write_request<> proxy(
      req->ntp, deep_copy(req->data_chunk), timeout, req->stage);
    auto fut = proxy.response.get_future();
    _pipeline.reenqueue(proxy);
    auto batches_fut = co_await ss::coroutine::as_future(std::move(fut));
    if (batches_fut.failed()) {
        vlog(
          cd_log.error,
          "Proxy write request failed: {}",
          batches_fut.get_exception());
        co_return outcome::failure(errc::upload_failure);
    }
    auto batches = batches_fut.get();
    if (batches.has_error()) {
        // Normal errors (S3 upload failure or timeout)
        // are handled here
        vlog(cd_log.error, "Proxy write request failed: {}", batches.error());
        co_return outcome::failure(batches.error());
    }
    auto ptr = ss::make_lw_shared<ss::circular_buffer<model::record_batch>>(
      std::move(batches.value()));
    co_return outcome::success(ss::make_foreign(std::move(ptr)));
}

void write_request_balancer::ack_write_response(
  core::write_request<>* req, checked<foreign_resp_ptr, errc> resp) {
    // The response was created on another shard.
    // The req was created on this shard.
    // TODO: use zero copy
    if (resp.has_error()) {
        req->set_value(resp.error());
    } else {
        auto batches = deep_copy(*resp.value());
        req->set_value(std::move(batches));
    }
}

} // namespace experimental::cloud_topics
