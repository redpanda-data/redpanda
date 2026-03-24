/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_topics/level_zero/gc/rpc_service.h"
#include "cloud_topics/level_zero/gc/rpc_types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace cloud_topics::l0::gc::rpc {

class service final : public impl::epoch_barrier_rpc_service {
public:
    service(ss::scheduling_group, ss::smp_service_group);

    ss::future<barrier_response>
    advance_barrier(barrier_request, ::rpc::streaming_context&) override;
};

} // namespace cloud_topics::l0::gc::rpc
