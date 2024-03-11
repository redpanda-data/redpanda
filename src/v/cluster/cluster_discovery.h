// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "random/simple_time_jitter.h"

#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>

#include <optional>
#include <vector>

namespace storage {
class kvstore;
class api;
} // namespace storage

namespace cluster {
struct cluster_bootstrap_info_reply;

// Provides metadata pertaining to initial cluster discovery. It is the
// entrypoint into the steps to join a cluster.
//
// Node ID assignment and joining a cluster
// ========================================
// When a node starts up, before it can initialize most of its subsystems, it
// must be made aware of its node ID. It can either get this from its config,
// the kv-store, or be assigned one by the controller leader. In all cases, the
// node ID and node UUID must be registered with the controller, after which
// the node can proceed join the cluster.
//
// The high level steps are as follows:
//
// When the node ID is unknown:
// 1. Generate or load node UUID
// 2. Get assigned a node ID by sending a request to the controller leader
// 3. Start subsystems with our known node ID
// 4. Join the cluster and get added to the controller Raft group by sending a
//    request to the controller leader
// 5. Once added to the cluster, open endpoints for user traffic
//
// When the node ID is known:
// 1. Generate or load node UUID
// 2. Load node ID from config or kv-store
// 3. Start subsystems with our known node ID
// 4. Register our UUID with our node ID and join the cluster by sending a
//    request to the controller leader
// 5. Once added to the cluster, open endpoints for user traffic
//
// These steps are implemented here, in redpanda/application.cc, and in
// cluster/members_manager.cc
//
// TODO: reconcile the RPC dispatch logic here with that in members_manager.
class cluster_discovery {
public:
    using brokers = std::vector<model::broker>;
    using node_ids_by_uuid
      = absl::flat_hash_map<model::node_uuid, model::node_id>;

    // After a successful join by a non-founder node to an existing cluster,
    // this is what we know about the cluster
    struct registration_result {
        // True if this is the result of registering the node with the
        // cluster, false if it is populated based on local state (i.e.
        // we are a founder or an already-registered node).
        bool newly_registered{false};

        model::node_id assigned_node_id;
        std::optional<iobuf> controller_snapshot;
    };

    cluster_discovery(
      const model::node_uuid& node_uuid, storage::api&, ss::abort_source&);

    // Register with the cluster:
    // - If we are a fresh cluster founder, broadcast to other founders
    //   to ensure we agree on the seed servers, then proceed.
    // - For non-founders, call out to a seed server to register, which
    //   will issue us with a node ID if we don't already have one, and
    //   provide a controller snapshot.
    //
    // This method is to be used before starting the controller for
    // the first time: after this method returns success, we have a node ID set,
    // the cluster has accepted our request to join, and we have a controller
    // snapshot that we can use to prime configuration/features state before
    // starting up the controller.
    ss::future<registration_result> register_with_cluster();

    // Returns brokers to be used to form a Raft group for a new cluster.
    //
    // If this node is a cluster founder, returns all seed servers, after
    // making sure that all founders are configured with identical seed servers
    // list. In case of root-driven bootstrap, that reflects to a list of just
    // the root broker.
    //
    // If this node is not a cluster founder, returns an empty list.
    brokers founding_brokers() const;

    // A cluster founder is a node that is configured as a seed server, and
    // whose local on-disk state along with the remote state from other seed
    // servers indicate that a cluster doesn't already exist. A cluster founder
    // will form the initial controller Raft group with all other seed servers,
    // to which non-seeds can join later.
    //
    // Upon completion of this call, if config::node().node_id was empty, it
    // will be set with an ID agreed upon by all seeds.
    ss::future<bool> is_cluster_founder();

    // Returns node_uuid to node_id map built during cluster discovery.
    // Non-const to allow moving the contents away, since it is supposed to be
    // a single use call.
    //
    // \pre is_cluster_founder() future has been completed
    // \pre get_node_ids_by_uuid() has never been called
    node_ids_by_uuid& get_node_ids_by_uuid();

private:
    // Sends requests to each seed server to register the local node UUID
    // until one succeeds. Returns nullopt if registration did not succeed.
    ss::future<std::optional<registration_result>>
    dispatch_node_uuid_registration_to_seeds();

    // Requests `cluster_bootstrap_info` from the given address, returning
    // early with a bogus result if it's already been determined if this node
    // is a cluster founder.
    ss::future<cluster_bootstrap_info_reply>
      request_cluster_bootstrap_info_single(net::unresolved_address) const;

    // Initializes founder state (whether a cluster already exists, whether
    // this node is a founder, etc). Requests cluster_bootstrap_info from all
    // seeds to determine whether the local node is a cluster founder, and if
    // so, populates `_founding_brokers` and `_node_ids_by_uuid`. Validates that
    // all seeds are consistent with one another and agree on the set of
    // founding brokers.
    //
    // Sets `_is_cluster_founder` upon completion.
    ss::future<> discover_founding_brokers();

    const model::node_uuid _node_uuid;
    simple_time_jitter<model::timeout_clock> _join_retry_jitter;
    const std::chrono::milliseconds _join_timeout;

    std::optional<bool> _is_cluster_founder;
    storage::api& _storage;
    ss::abort_source& _as;
    brokers _founding_brokers;
    node_ids_by_uuid _node_ids_by_uuid;
};

} // namespace cluster
