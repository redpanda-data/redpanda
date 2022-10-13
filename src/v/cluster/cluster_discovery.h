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
    cluster_discovery(const model::node_uuid& node_uuid, ss::abort_source&);

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

    // If configured as the root, return this broker as the sole initial Raft0
    // broker.
    //
    // TODO: implement the below behavior.
    //
    // Returns brokers to be used to form a Raft group for a new cluster.
    //
    // If this node is a seed server, returns all seed servers, assuming seeds
    // are configured with identical seed servers.
    //
    // If this node is not a seed server returns an empty list.
    std::vector<model::broker> initial_raft0_brokers() const;

private:
    // Returns whether this node is the root node.
    //
    // TODO: implement the below behavior.
    //
    // Returns true if the local node is a founding member of the cluster, as
    // indicated by either us having an empty seed server (we are the root node
    // in a legacy config) or our node UUID matching one of those returned by
    // the seed servers.
    bool is_cluster_founder() const;

    // Sends requests to each seed server to register the local node UUID until
    // one succeeds. Upon success, sets `node_id` to the assigned node ID and
    // returns true.
    ss::future<bool> dispatch_node_uuid_registration_to_seeds(model::node_id&);

    const model::node_uuid _node_uuid;
    simple_time_jitter<model::timeout_clock> _join_retry_jitter;
    const std::chrono::milliseconds _join_timeout;

    ss::abort_source& _as;
};

} // namespace cluster
