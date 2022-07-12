/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/partition_balancer_rpc_handler.h"

#include "cluster/partition_balancer_backend.h"

namespace cluster {

partition_balancer_rpc_handler::partition_balancer_rpc_handler(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<partition_balancer_backend>& backend)
  : partition_balancer_rpc_service(sg, ssg)
  , _backend(backend) {}

ss::future<partition_balancer_overview_reply>
partition_balancer_rpc_handler::overview(
  partition_balancer_overview_request&&, rpc::streaming_context&) {
    auto overview = co_await _backend.invoke_on(
      partition_balancer_backend::shard,
      [](partition_balancer_backend& backend) { return backend.overview(); });

    co_return overview;
}

} // namespace cluster
