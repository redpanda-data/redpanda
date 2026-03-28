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

#include "cloud_topics/level_zero/gc/epoch_barrier_coordinator.h"

namespace cloud_topics::l0::gc::rpc {

service::service(
  ss::scheduling_group sg,
  ss::smp_service_group smp_sg,
  ss::sharded<epoch_barrier_coordinator>* coordinator)
  : impl::epoch_barrier_rpc_service(sg, smp_sg)
  , _coordinator(coordinator) {}

ss::future<invalidate_epoch_response> service::invalidate_epoch(
  invalidate_epoch_request request, ::rpc::streaming_context&) {
    auto result = co_await _coordinator->local().invalidate(request.candidate);
    co_return invalidate_epoch_response{
      .ec = result.has_value() ? ::rpc::errc::success : result.error()};
}

ss::future<poll_drain_response>
service::poll_drain(poll_drain_request request, ::rpc::streaming_context&) {
    auto result = co_await _coordinator->local().poll_drain(request.candidate);
    if (result.has_value()) {
        co_return poll_drain_response{
          .drained = result.value(), .ec = ::rpc::errc::success};
    }
    co_return poll_drain_response{.ec = result.error()};
}

ss::future<publish_safe_epoch_response> service::publish_safe_epoch(
  publish_safe_epoch_request request, ::rpc::streaming_context&) {
    auto result = co_await _coordinator->local().publish_safe_epoch(
      request.safe_epoch);
    co_return publish_safe_epoch_response{
      .ec = result.has_value() ? ::rpc::errc::success : result.error()};
}

} // namespace cloud_topics::l0::gc::rpc
