// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "random/simple_time_jitter.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#include <optional>
#include <vector>

namespace storage {
class kvstore;
} // namespace storage

namespace cluster {

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
    cluster_discovery(
      const model::node_uuid& node_uuid,
      storage::kvstore& kvstore,
      ss::abort_source&);

    // Determines what the node ID for this node should be. Once called, we can
    // proceed with initializing anything that depends on node ID (Raft
    // subsystem, etc).
    //
    // On a non-seed server with no node ID specified via config or on disk,
    // this sends a request to the controllers to register this node's UUID and
    // assign it a node ID.
    //
    // TODO: implement the below behavior.
    //
    // On a seed server with no data on it (i.e. a fresh node), this sends
    // requests to all other seed servers to determine if there is a valid
    // assignment of node IDs for the seeds.
    ss::future<model::node_id> determine_node_id();

    // Returns brokers to be used to form a Raft group for a new cluster.
    //
    // If this node is a cluster founder, returns all seed servers, assuming
    // all founders are configured with identical seed servers list.
    // If this node is not a cluster founder, returns an empty list.
    // In case of Emtpy Seed Cluster Bootstrap, that reflects to a list of the
    // root broker in root if cluster is not there yet, and empty otherwise.
    std::vector<model::broker> initial_seed_brokers() const;

private:
    // Returns index-based node_id if the local node is a founding member
    // of the cluster, as indicated by either us having an empty seed server
    // (we are the root node in a legacy config), or our node IP listed
    // as one of the seed servers.
    static std::optional<model::node_id> get_cluster_founder_node_id();

    // Sends requests to each seed server to register the local node UUID until
    // one succeeds. Upon success, sets `node_id` to the assigned node ID and
    // returns true.
    ss::future<bool> dispatch_node_uuid_registration_to_seeds(model::node_id&);

    const model::node_uuid _node_uuid;
    simple_time_jitter<model::timeout_clock> _join_retry_jitter;
    const std::chrono::milliseconds _join_timeout;

    storage::kvstore& _kvstore;
    ss::abort_source& _as;
};

} // namespace cluster
