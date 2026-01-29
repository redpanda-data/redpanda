/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/read_request_scheduler/read_request_scheduler.h"

#include "cloud_topics/logger.h"
#include "ssx/future-util.h"

#include <chrono>

using namespace std::chrono_literals;

namespace cloud_topics::l0 {

read_request_scheduler::read_request_scheduler(
  read_pipeline<ss::lowres_clock>::stage stage)
  : _stage(std::move(stage)) {}

ss::future<> read_request_scheduler::start() {
    vlog(cd_log.debug, "Read Request Scheduler start");
    ssx::spawn_with_gate(_gate, [this] { return bg_loop(); });
    co_return;
}

ss::future<> read_request_scheduler::stop() { co_await _gate.close(); }

namespace {
ss::shard_id shard_for(const read_request<ss::lowres_clock>& req) {
    std::hash<ss::sstring> hasher;
    // The request is generated from the placeholder batch.
    // The placeholder batch can't span multiple objects so it's safe
    // to check only the first extent.
    auto h = hasher(req.query.meta.front().id.name);
    auto shard = h % ss::smp::count;
    return static_cast<ss::shard_id>(shard);
}

std::unique_ptr<read_request<ss::lowres_clock>> make_proxy(
  ss::shard_id target_shard,
  const read_request<ss::lowres_clock>& req,
  ss::lowres_clock::time_point timeout,
  retry_chain_node* target_rtc,
  pipeline_stage id) {
    vassert(
      ss::this_shard_id() == target_shard,
      "make_proxy called on the wrong shard");
    dataplane_query query;
    query.output_size_estimate = req.query.output_size_estimate;
    query.meta = req.query.meta.copy();
    auto proxy = std::make_unique<read_request<ss::lowres_clock>>(
      req.ntp, std::move(query), timeout, target_rtc, id);
    return proxy;
}

} // namespace

void read_request_scheduler::schedule_on(
  read_request<ss::lowres_clock>& source_req, ss::shard_id target) {
    if (target == ss::this_shard_id()) {
        // Fast path, just push source_req down the pipeline
        _stage.push_next_stage(source_req);
        return;
    }

    // Check shutdown before launching cross-shard RPC
    if (_stage.stopped()) {
        source_req.set_value(errc::shutting_down);
        return;
    }

    auto proxy = container().invoke_on(
      target, [target, &source_req](read_request_scheduler& s) {
          return s.proxy_read_request(source_req, target);
      });

    auto ack = proxy.then(
      [&source_req](read_request<ss::lowres_clock>::response_t resp) {
          if (resp.has_value()) {
              source_req.set_value(std::move(resp.value()));
          } else {
              source_req.set_value(resp.error());
          }
      });

    // Note: We intentionally do NOT hold the gate here. The gate is only used
    // for the bg_loop fiber. These fire-and-forget continuations must be able
    // to complete even during shutdown to avoid deadlock.
    ssx::background = std::move(ack);
}

ss::future<read_request<ss::lowres_clock>::response_t>
read_request_scheduler::proxy_read_request(
  const read_request<ss::lowres_clock>& source_req, ss::shard_id target) {
    auto now = ss::lowres_clock::now();
    auto timeout = source_req.expiration_time;
    if (timeout < now) {
        co_return std::unexpected(errc::timeout);
    }
    // Use pipeline stage id from the _stage object and not from the request/
    // The request belongs to another pipeline and its stage id doesn't make
    // sense on the current shard.
    auto proxy = make_proxy(
      target, source_req, timeout, &_stage.get_root_rtc(), _stage.id());

    // Check if pipeline is shutting down before awaiting response
    if (_stage.stopped()) {
        co_return std::unexpected(errc::shutting_down);
    }

    auto f = proxy->response.get_future();
    _stage.push_next_stage(*proxy);
    auto res = co_await ss::coroutine::as_future(std::move(f));
    if (res.failed()) {
        auto ex = res.get_exception();
        // Check for shutdown exceptions explicitly
        if (ssx::is_shutdown_exception(ex)) {
            co_return std::unexpected(errc::shutting_down);
        }
        co_return std::unexpected(errc::unexpected_failure);
    }
    co_return std::move(res.get());
}

ss::future<> read_request_scheduler::bg_loop() {
    while (!_stage.stopped()) {
        // NOTE(1): requests are vectorized but it's not guaranteed
        // that all extents in the request target the same object.
        // If this is the case the scheduler will use first extent
        // to decide the target shard. This could lead to suboptimal
        // distribution of requests across shards and some edge cases.
        // To avoid this the caller of the 'materialize' must ensure
        // that the requests are split properly so that all extents
        // in the request target the same object. This is not a
        // correctness problem. The only side effect is that we may
        // download same objects on multiple shards in parallel in
        // cases.
        //
        // NOTE(2): cache locality is not a concern here because
        // unlike in cases of write path the read path is only used
        // when there is a cache miss. Normally, we will not hit this
        // code path if the cache is working well and there is no
        // leadership transfers. The goal here is to brute-force the
        // reconciliation of cache misses as fast as possible.
        auto res = co_await _stage.pull_fetch_requests(10_MiB);
        if (!res.has_value()) {
            if (res.error() == errc::shutting_down) {
                break;
            }
            vlog(
              _stage.logger().error,
              "Failed to pull fetch requests: {}",
              res.error());
            _stage.register_pipeline_error(res.error());
            continue;
        }
        auto list = std::move(res.value());
        while (!list.requests.empty()) {
            auto front = &list.requests.front();
            list.requests.pop_front();
            auto target_shard = shard_for(*front);
            schedule_on(*front, target_shard);
        }
    }
}

} // namespace cloud_topics::l0
