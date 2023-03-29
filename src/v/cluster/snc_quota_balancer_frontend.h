/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/fwd.h"
#include "cluster/snc_quota_balancer_types.h"
#include "model/metadata.h"
#include "rpc/fwd.h"

#include <seastar/core/sharded.hh>

namespace cluster {

class snc_quota_balancer_frontend final {
public:
    snc_quota_balancer_frontend(
      ss::sharded<rpc::connection_cache>& connection_cache,
      cluster::controller* controller)
      : _connection_cache(connection_cache)
      , _controller(*controller) {}

    ss::future<cluster::snc_quota_balancer_lend_reply> dispatch_lend_request(
      model::node_id to_node, cluster::snc_quota_balancer_lend_request req);

private:
    ss::sharded<rpc::connection_cache>& _connection_cache;
    cluster::controller& _controller;
};

} // namespace cluster
