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
#include "base/seastarx.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/fwd.h"
#include "cluster/rpc_utils.h"
#include "config/node_config.h"
#include "model/record.h"
#include "rpc/connection_cache.h"

#include <seastar/core/sharded.hh>

namespace cluster::cloud_metadata {

class producer_id_recovery_manager {
public:
    producer_id_recovery_manager(
      ss::sharded<cluster::members_table>& members_table,
      ss::sharded<rpc::connection_cache>& conn_cache,
      ss::sharded<id_allocator_frontend>& id_allocator);

    ss::future<cloud_metadata::error_outcome> recover() const;

    /// The maximum producer ID over all partitions of every broker. Note:
    /// a default-constructed (0) result means either "the maximum really is
    /// 0" or "no producer state found anywhere" — producer ID 0 is valid, so
    /// callers that must not miss an existing producer cannot treat 0 as
    /// "nothing to do".
    ss::future<result<model::producer_id, cloud_metadata::error_outcome>>
    get_cluster_highest_pid() const;

private:
    ss::future<result<model::producer_id, cloud_metadata::error_outcome>>
    get_node_highest_pid(const model::broker&) const;

    ss::sharded<cluster::members_table>& members_table_;
    ss::sharded<rpc::connection_cache>& conn_cache_;
    ss::sharded<id_allocator_frontend>& id_allocator_;
    const model::broker self_{cluster::make_self_broker(config::node())};
    const config::tls_config rpc_tls_config_{config::node().rpc_server_tls()};
    const std::chrono::seconds timeout_{5};
};

} // namespace cluster::cloud_metadata
