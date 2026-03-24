/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/gc/service.h"

#include "cloud_topics/level_zero/gc/epoch_barrier.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l0::gc::rpc {

service::service(
  ss::scheduling_group sg,
  ss::smp_service_group smp_sg,
  ss::sharded<epoch_barrier>* barrier)
  : impl::epoch_barrier_rpc_service(sg, smp_sg)
  , _barrier(barrier) {}

ss::future<barrier_response>
service::advance_barrier(barrier_request request, ::rpc::streaming_context&) {
    // Dispatch to shard 0 so handle_barrier's round state (_round,
    // _drain_future) and cross-shard coordination (invoke_on_all,
    // map_reduce0) always originate from the same shard.
    auto fut = co_await ss::coroutine::as_future(
      _barrier->invoke_on(0, [request = std::move(request)](epoch_barrier& b) {
          return b.handle_barrier(request.candidate, request.prev_safe_epoch);
      }));
    if (fut.failed()) {
        fut.ignore_ready_future();
        co_return barrier_response{.s = barrier_response::status::error};
    }
    auto result = fut.get();
    auto status = result == epoch_barrier::barrier_status::ready
                    ? barrier_response::status::ready
                    : barrier_response::status::pending;
    co_return barrier_response{
      .s = status,
    };
}

} // namespace cloud_topics::l0::gc::rpc
