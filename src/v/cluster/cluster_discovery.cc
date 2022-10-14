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
#include "cluster/controller_service.h"
#include "cluster/logger.h"
#include "config/node_config.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"
#include "storage/kvstore.h"

#include <chrono>

using model::broker;
using model::node_id;
using std::vector;

namespace cluster {

cluster_discovery::cluster_discovery(
  const model::node_uuid& node_uuid,
  storage::kvstore& kvstore,
  ss::abort_source& as)
  : _node_uuid(node_uuid)
  , _join_retry_jitter(config::shard_local_cfg().join_retry_timeout_ms())
  , _join_timeout(std::chrono::seconds(2))
  , _kvstore(kvstore)
  , _as(as) {}

ss::future<node_id> cluster_discovery::determine_node_id() {
    // TODO: read from disk if empty.
    const auto& configured_node_id = config::node().node_id();
    if (configured_node_id != std::nullopt) {
        clusterlog.info("Using configured node ID {}", configured_node_id);
        co_return *configured_node_id;
    }
    static const bytes invariants_key("configuration_invariants");
    auto invariants_buf = _kvstore.get(
      storage::kvstore::key_space::controller, invariants_key);

    if (invariants_buf) {
        auto invariants = reflection::from_iobuf<configuration_invariants>(
          std::move(*invariants_buf));
        co_return invariants.node_id;
    }
    // TODO: once is_cluster_founder() refers to all seeds, verify that all the
    // seeds' seed_servers lists match and assign node IDs based on the
    // ordering.
    if (is_cluster_founder()) {
        // If this is the root node, assign node 0.
        co_return model::node_id(0);
    }
    model::node_id assigned_node_id;
    while (!_as.abort_requested()) {
        auto assigned = co_await dispatch_node_uuid_registration_to_seeds(
          assigned_node_id);
        if (assigned) {
            break;
        }
        co_await ss::sleep_abortable(_join_retry_jitter.next_duration(), _as);
    }
    co_return assigned_node_id;
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

ss::future<bool> cluster_discovery::dispatch_node_uuid_registration_to_seeds(
  model::node_id& assigned_node_id) {
    const auto& seed_servers = config::node().seed_servers();
    auto self = make_self_broker(config::node());
    for (const auto& s : seed_servers) {
        vlog(
          clusterlog.info,
          "Requesting node ID for UUID {} from {}",
          _node_uuid,
          s.addr);
        result<join_node_reply> r(join_node_reply{});
        try {
            r = co_await do_with_client_one_shot<controller_client_protocol>(
              s.addr,
              config::node().rpc_server_tls(),
              _join_timeout,
              [&self, this](controller_client_protocol c) {
                  return c
                    .join_node(
                      join_node_request(
                        features::feature_table::get_latest_logical_version(),
                        _node_uuid().to_vector(),
                        self),
                      rpc::client_opts(rpc::clock_type::now() + _join_timeout))
                    .then(&rpc::get_ctx_data<join_node_reply>);
              });
        } catch (...) {
            vlog(
              clusterlog.debug,
              "Error registering UUID {}, retrying: {}",
              _node_uuid,
              std::current_exception());
            continue;
        }
        if (r.has_error()) {
            vlog(
              clusterlog.debug,
              "Error registering UUID {}: {}, retrying",
              _node_uuid,
              r.error());
            continue;
        }
        if (!r.has_value() || !r.value().success) {
            vlog(
              clusterlog.debug,
              "Error registering UUID {}, retrying",
              _node_uuid);
            continue;
        }
        const auto& reply = r.value();
        if (reply.id < 0) {
            // Something else went wrong. Maybe duplicate UUID?
            vlog(clusterlog.debug, "Negative node ID {}", reply.id);
            continue;
        }
        assigned_node_id = reply.id;
        co_return true;
    }
    co_return false;
}

bool cluster_discovery::is_cluster_founder() const {
    return config::node().seed_servers().empty();
}

} // namespace cluster
