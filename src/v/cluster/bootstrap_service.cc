// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/bootstrap_service.h"

#include "cluster/bootstrap_types.h"
#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "config/node_config.h"
#include "features/feature_table.h"
#include "storage/api.h"

namespace cluster {

ss::future<cluster_bootstrap_info_reply>
bootstrap_service::cluster_bootstrap_info(
  cluster_bootstrap_info_request, rpc::streaming_context&) {
    cluster_bootstrap_info_reply r{};
    r.broker = make_self_broker(config::node());
    r.version = features::feature_table::get_latest_logical_version();
    const std::vector<config::seed_server>& seed_servers
      = config::node().seed_servers();
    r.seed_servers.reserve(seed_servers.size());
    std::transform(
      seed_servers.cbegin(),
      seed_servers.cend(),
      std::back_inserter(r.seed_servers),
      [](const config::seed_server& seed_server) { return seed_server.addr; });
    r.empty_seed_starts_cluster = config::node().empty_seed_starts_cluster();
    r.cluster_uuid = _storage.local().get_cluster_uuid();
    r.node_uuid = _storage.local().node_uuid();

    vlog(clusterlog.debug, "Replying cluster_bootstrap_info: {}", r);
    co_return r;
}

} // namespace cluster
