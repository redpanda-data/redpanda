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
#include "snc_quota_balancer_frontend.h"

#include "cluster/controller.h"
#include "cluster/logger.h"
#include "cluster/snc_quota_balancer_rpc_service.h"
#include "cluster/snc_quota_balancer_types.h"
#include "model/timeout_clock.h"
#include "outcome.h"
#include "rpc/connection_cache.h"
#include "rpc/types.h"

namespace cluster {

ss::future<cluster::snc_quota_balancer_lend_reply>
snc_quota_balancer_frontend::dispatch_lend_request(
  model::node_id to_node, cluster::snc_quota_balancer_lend_request req) {
    constexpr auto timeout = 1s;
    auto ctx
      = co_await _connection_cache.local()
          .with_node_client<cluster::snc_quota_balancer_rpc_client_protocol>(
            _controller.self(),
            ss::this_shard_id(),
            to_node,
            timeout,
            [req = std::move(req), timeout](
              cluster::snc_quota_balancer_rpc_client_protocol cp) mutable {
                return cp.snc_quota_balancer_lend(
                  std::move(req),
                  rpc::client_opts(model::timeout_clock::now() + timeout));
            });
    result<cluster::snc_quota_balancer_lend_reply> r
      = rpc::get_ctx_data<cluster::snc_quota_balancer_lend_reply>(
        std::move(ctx));
    if (r.has_error()) {
        vlog(
          clusterlog.warn,
          "qm - got error {} on remote lend request",
          r.error());
        co_return cluster::snc_quota_balancer_lend_reply{};
    }

    co_return r.value();
}

} // namespace cluster
