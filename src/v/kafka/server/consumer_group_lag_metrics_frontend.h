/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "kafka/server/consumer_group_lag_metrics_rpc_types.h"
#include "kafka/server/fwd.h"
#include "rpc/fwd.h"

#include <seastar/core/gate.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

namespace experimental::cloud_topics {
class app;
} // namespace experimental::cloud_topics
namespace kafka {

/**
 * \brief Dispatch requests to partition leaders.
 */
class consumer_group_lag_metrics_frontend final
  : public ss::peering_sharded_service<consumer_group_lag_metrics_frontend> {
public:
    consumer_group_lag_metrics_frontend(
      model::node_id self,
      ss::sharded<rpc::connection_cache>& rpc_connections,
      ss::sharded<cluster::metadata_cache>& metadata,
      ss::sharded<cluster::partition_manager>& partition_manager,
      ss::sharded<experimental::cloud_topics::app>& ct_app)
      : _self{self}
      , _rpc_connections{rpc_connections}
      , _metadata{metadata}
      , _partition_manager(partition_manager)
      , _ct_app(ct_app) {}

    ss::future<> start();
    ss::future<> stop();

    ss::future<partition_offsets_reply>
    get_partition_offsets(partition_offsets_request req);

private:
    friend class consumer_group_lag_metrics_service;
    ss::future<partition_offsets_reply>
      get_local_partition_offsets(partition_offsets_request);

    model::node_id _self;
    ss::sharded<rpc::connection_cache>& _rpc_connections;
    ss::sharded<cluster::metadata_cache>& _metadata;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<experimental::cloud_topics::app>& _ct_app;

    ss::gate _gate;
    partition_offsets_reply::offsets _consumer_offsets;
};

} // namespace kafka
