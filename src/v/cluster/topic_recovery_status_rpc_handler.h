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

#include "cluster/topic_recovery_status_rpc_service.h"

namespace cloud_storage {
struct topic_recovery_service;
}

namespace cluster {

status_response map_log_to_response(
  std::vector<cloud_storage::topic_recovery_service::recovery_status>
    status_log);
class topic_recovery_status_rpc_handler final
  : public topic_recovery_status_rpc_service {
public:
    explicit topic_recovery_status_rpc_handler(
      ss::scheduling_group sg,
      ss::smp_service_group ssg,
      ss::sharded<cloud_storage::topic_recovery_service>& service);

    ss::future<status_response>
    get_status(status_request&&, rpc::streaming_context&) final;

private:
    ss::sharded<cloud_storage::topic_recovery_service>& _topic_recovery_service;
};

} // namespace cluster
