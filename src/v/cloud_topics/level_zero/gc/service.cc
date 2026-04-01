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

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l0::gc::rpc {

service::service(ss::scheduling_group sg, ss::smp_service_group smp_sg)
  : impl::epoch_barrier_rpc_service(sg, smp_sg) {}

ss::future<barrier_response>
service::advance_barrier(barrier_request, ::rpc::streaming_context&) {
    co_return barrier_response{
      .s = barrier_response::status::error,
    };
}

} // namespace cloud_topics::l0::gc::rpc
