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

#include "cluster/snc_quota_balancer_rpc_service.h"
#include "kafka/server/fwd.h"

namespace cluster {

class snc_quota_balancer_service final : public snc_quota_balancer_rpc_service {
public:
    snc_quota_balancer_service(
      ss::scheduling_group sg,
      ss::smp_service_group ssg,
      ss::sharded<kafka::snc_quota_manager>&);

    ss::future<struct snc_quota_balancer_lend_reply> snc_quota_balancer_lend(
      snc_quota_balancer_lend_request&&, rpc::streaming_context&) override;

private:
    ss::sharded<kafka::snc_quota_manager>& _quota_manager;
};

} // namespace cluster
