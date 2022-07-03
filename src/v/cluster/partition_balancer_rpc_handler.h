/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/fwd.h"
#include "cluster/partition_balancer_rpc_service.h"
#include "cluster/partition_balancer_types.h"

#include <seastar/core/sharded.hh>

namespace cluster {

class partition_balancer_rpc_handler final
  : public partition_balancer_rpc_service {
public:
    partition_balancer_rpc_handler(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<partition_balancer_backend>&);

    virtual ss::future<partition_balancer_overview_reply> overview(
      partition_balancer_overview_request&&, rpc::streaming_context&) override;

private:
    ss::sharded<partition_balancer_backend>& _backend;
};

} // namespace cluster
