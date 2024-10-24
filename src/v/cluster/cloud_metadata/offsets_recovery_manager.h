/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/cloud_metadata/offsets_recovery_router.h"
#include "cluster/fwd.h"

namespace kafka {
class coordinator_ntp_mapper;
} // namespace kafka

namespace cluster::cloud_metadata {

class offsets_recovery_manager : public offsets_recovery_requestor {
public:
    offsets_recovery_manager(
      ss::sharded<offsets_recovery_router>& recovery,
      ss::sharded<kafka::coordinator_ntp_mapper>& mapper,
      ss::sharded<cluster::members_table>& members,
      ss::sharded<cluster::controller_api>& controller_api,
      ss::sharded<topics_frontend>& topics_frontend)
      : _recovery_router(recovery)
      , _mapper(mapper)
      , _members(members)
      , _controller_api(controller_api)
      , _topics_frontend(topics_frontend) {}

    ss::future<error_outcome> recover(
      retry_chain_node& parent_retry,
      const cloud_storage_clients::bucket_name& bucket,
      std::vector<std::vector<cloud_storage::remote_segment_path>>
        snapshot_paths_per_pid) override;
    ~offsets_recovery_manager() override = default;

private:
    ss::sharded<offsets_recovery_router>& _recovery_router;
    ss::sharded<kafka::coordinator_ntp_mapper>& _mapper;
    ss::sharded<cluster::members_table>& _members;
    ss::sharded<cluster::controller_api>& _controller_api;
    ss::sharded<topics_frontend>& _topics_frontend;
};

} // namespace cluster::cloud_metadata
