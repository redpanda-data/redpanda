
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

#include "cluster/topic_recovery_status_types.h"
#include "model/metadata.h"

#include <seastar/core/sharded.hh>

namespace cloud_storage {
struct topic_recovery_service;
}

namespace rpc {
class connection_cache;
}

namespace cluster {

class members_table;

class topic_recovery_status_frontend {
public:
    explicit topic_recovery_status_frontend(
      model::node_id node_id,
      ss::sharded<rpc::connection_cache>& connections,
      ss::sharded<members_table>& members);

    using skip_this_node = ss::bool_class<struct skip_this_node_tag>;
    ss::future<bool> is_recovery_running(
      ss::sharded<cloud_storage::topic_recovery_service>&
        topic_recovery_service,
      skip_this_node skip_source_shard) const;

    ss::future<std::optional<status_response>>
    status(model::node_id node) const;

    ss::future<> stop() { return ss::make_ready_future<>(); }

private:
    model::node_id _self;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<members_table>& _members;
};

} // namespace cluster
