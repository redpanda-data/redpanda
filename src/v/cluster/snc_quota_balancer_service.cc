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
#include "snc_quota_balancer_service.h"

#include "cluster/logger.h"
#include "kafka/server/snc_quota_manager.h"
#include "snc_quota_balancer_types.h"

namespace cluster {

snc_quota_balancer_service::snc_quota_balancer_service(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<kafka::snc_quota_manager>& quota_manager)
  : snc_quota_balancer_rpc_service(sg, ssg)
  , _quota_manager(quota_manager) {}

ss::future<snc_quota_balancer_lend_reply>
cluster::snc_quota_balancer_service::snc_quota_balancer_lend(
  snc_quota_balancer_lend_request&& req, rpc::streaming_context&) {
    const snc_quota_balancer_lend_reply rep
      = co_await _quota_manager.local().handle_lend_request(std::move(req));
    co_return rep;
}

} // namespace cluster
