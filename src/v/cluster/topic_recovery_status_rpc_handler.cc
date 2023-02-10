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
        co_return status_response{
          .state = cloud_storage::topic_recovery_service::state::inactive};
    }

    auto current_status
      = _topic_recovery_service.local().current_recovery_status();

    std::vector<topic_downloads> downloads;
    downloads.reserve(current_status.download_counts.size());

    for (const auto& [tp_ns, count] : current_status.download_counts) {
        downloads.push_back(
          {.tp_ns = tp_ns,
           .pending_downloads = count.pending_downloads,
           .successful_downloads = count.successful_downloads,
           .failed_downloads = count.failed_downloads});
    }

    recovery_request_params request_params;
    request_params.populate(current_status.request);
    co_return status_response{
      .state = current_status.state,
      .download_counts = downloads,
      .request = request_params};
}

} // namespace cluster
