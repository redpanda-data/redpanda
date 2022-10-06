// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_discovery.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "config/node_config.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <chrono>

using model::broker;
using model::node_id;
using std::vector;

namespace cluster {

cluster_discovery::cluster_discovery(const model::node_uuid& node_uuid)
  : _node_uuid(node_uuid) {}

ss::future<node_id> cluster_discovery::determine_node_id() {
    co_return *config::node().node_id();
}

vector<broker> cluster_discovery::initial_raft0_brokers() const {
    // If configured as the root node, we'll want to start the cluster with
    // just this node as the initial seed.
    if (is_cluster_founder()) {
        // TODO: we should only return non-empty seed list if our log is empty.
        return {make_self_broker(config::node())};
    }
    return {};
}

bool cluster_discovery::is_cluster_founder() const {
    return config::node().seed_servers().empty();
}

} // namespace cluster
