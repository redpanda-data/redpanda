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

#include "cluster/topic_recovery_status_rpc_handler.h"

#include "cloud_storage/topic_recovery_service.h"

namespace cluster {

topic_recovery_status_rpc_handler::topic_recovery_status_rpc_handler(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<cloud_storage::topic_recovery_service>& service)
  : topic_recovery_status_rpc_service{sg, ssg}
  , _topic_recovery_service{service} {}

ss::future<status_response> topic_recovery_status_rpc_handler::get_status(
  status_request&&, rpc::streaming_context&) {
    if (!_topic_recovery_service.local_is_initialized()) {
        co_return status_response{.is_active = false};
    }

    co_return status_response{
      .is_active = _topic_recovery_service.local().is_active()};
}

} // namespace cluster
