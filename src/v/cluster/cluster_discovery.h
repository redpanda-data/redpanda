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
#include "utils/mutex.h"

#include <seastar/core/future.hh>

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

    cluster_discovery(
      const model::node_uuid& node_uuid, storage::api&, ss::abort_source&);

    // Determines what the node ID for this node should be. Once called, we can
    // proceed with initializing anything that depends on node ID (Raft
    // subsystem, etc).
    //
    // On a non-seed server with no node ID specified via config or on disk,
    // this sends a request to the controllers to register this node's UUID and
    // assign it a node ID.
    //
    // On a seed server with no data on it (i.e. a fresh node), this sends
    // requests to all other seed servers to determine if there is a valid
    // assignment of node IDs for the seeds.
    ss::future<model::node_id> determine_node_id();

    /**
     * Returns brokers to be used to form a Raft group for a new cluster.
     *
     * If this node is a cluster founder, returns all seed servers, assuming
     * all founders are configured with identical seed servers list.
     * If this node is not a cluster founder, returns an empty list.
     * In case of Emtpy Seed Cluster Bootstrap, that reflects to a list of the
     * root broker in root if cluster is not there yet, and empty otherwise.
     * \throw std::runtime_error if cluster needs a bootstrap but seed brokers
     * do not satisfy relevant validation
     */
    ss::future<brokers> initial_seed_brokers();

    ss::future<brokers> initial_seed_brokers_if_no_cluster();

    /**
     * A cluster founder is a node that is configured as a seed server, and
     * whose local on-disk state along with the remote state from other seed
     * servers indicate that a cluster doesn't already exist. A cluster founder
     * will form the initial controller Raft group with all other seed servers,
     * to which non-seeds can join later.
     */
    ss::future<bool> is_cluster_founder();

private:
    // Sends requests to each seed server to register the local node UUID until
    // one succeeds. Upon success, sets `node_id` to the assigned node ID and
    // returns true.
    ss::future<bool> dispatch_node_uuid_registration_to_seeds(model::node_id&);

    ss::future<cluster_bootstrap_info_reply>
      request_cluster_bootstrap_info_single(net::unresolved_address) const;

    /**
     * If not already present, requests cluster_bootstrap_info from all seeds
     * and stores it into _cluster_bootstrap_info_replies
     */
    ss::future<> fetch_cluster_bootstrap_info();

    /**
     * Validate cluster_bootstrap_info responses from seeds for the purpose
     * of new cluster formation
     * \throw std::runtime_error if valdation has failed
     * \return list of peer seed_brokes to start cluster with
     */
    brokers get_seed_brokers_for_bootstrap() const;

    // Returns index-based node_id if the local node is a founding member
    // of the cluster, as indicated by either us having an empty seed server
    // (we are the root node in a legacy config), or our node IP listed
    // as one of the seed servers.
    ss::future<std::optional<model::node_id>> get_cluster_founder_node_id();

    /**
     * Validate cluster_bootstap_info responses from seeds for the purpose
     * of at least being a wiped cluster member seed server, and check if it
     * actually can be such. There are other scenarios possible for that, e.g.
     * starting a new node as a seed.
     *
     * Validation failures: two or more distinct cluster_uuid reported by other
     * seeds; configuration mismatch of the local with other seeds.
     *
     * \return true if a cluster uuid is detected in other seeds, and the
     *    validation to join that cluster as a seed passed
     * \return false if the server is rather a cluster founder than a wiped
     *    cluster member or other kind of seed-joiner
     * \throw std::runtime_error if it cannot be either of them (validation
     *    has been failed)
     */
    bool remote_cluster_uuid_exists() const;

    const model::node_uuid _node_uuid;
    simple_time_jitter<model::timeout_clock> _join_retry_jitter;
    const std::chrono::milliseconds _join_timeout;

    const bool _has_stored_cluster_uuid;
    storage::kvstore& _kvstore;
    ss::abort_source& _as;
    std::vector<
      std::pair<net::unresolved_address, cluster_bootstrap_info_reply>>
      _cluster_bootstrap_info;
    bool _cluster_bootstrap_info_fetched{false};
    mutex _fetch_cluster_bootstrap_mx;
};

} // namespace cluster
