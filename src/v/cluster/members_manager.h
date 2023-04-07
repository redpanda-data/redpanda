/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/commands.h"
#include "cluster/controller_stm.h"
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "config/seed_server.h"
#include "config/tls_config.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/types.h"
#include "random/simple_time_jitter.h"
#include "rpc/fwd.h"
#include "storage/fwd.h"

namespace features {
class feature_table;
} // namespace features

namespace cluster {

// Implementation of a raft::mux_state_machine that is responsible for
// updating information about cluster members, joining the cluster, updating
// member states, and creating intra-cluster connections.
//
// Node state updates
// ==================
// This class receives updates from members_frontend by way of a Raft record
// batch being committed. In addition to various controller command batch
// types, it reacts to Raft configuration batch types, e.g. when a new node is
// added to the controller Raft group.
//
// All the updates are propagated to core-local cluster::members_table
// instances. There is only one instance of members_manager running on
// core-0. The members_manager is also responsible for validation of node
// configuration invariants.
//
// Node joining and node ID assignment
// ===================================
// This class may be called directly by the frontend when nodes request to
// register with or join the cluster, rather than responding to a Raft record
// batch. Joining a cluster is a two step process, both driven by
// join_node_requests:
//
// Registration: a node's UUID hasn't yet been registered with a node ID. A
//   node ID is either provided by the caller or assigned in the apply phase
//   (serially, so it is deterministic across nodes). In both cases, a
//   controller batch is written to record the existence of the node UUID.
//
// Joining: the node's UUID has already been registered with a node ID. The
//   node can be added to the controller Raft group.
//
// Node UUID registrations are tracked in an internal map. Cluster membership
// (completed joins) modifies the controller Raft group directly.
class members_manager {
public:
    static constexpr auto accepted_commands = make_commands_list<
      decommission_node_cmd,
      recommission_node_cmd,
      finish_reallocations_cmd,
      maintenance_mode_cmd,
      register_node_uuid_cmd>{};
    static constexpr ss::shard_id shard = 0;
    static constexpr size_t max_updates_queue_size = 100;

    // Update types, used for communication between the manager and backend.
    //
    // NOTE: maintenance mode doesn't interact with the members_backend,
    // instead interacting with each core via their respective drain_manager.
    enum class node_update_type : int8_t {
        // A node has been added to the cluster.
        added,

        // A node has been decommissioned from the cluster.
        decommissioned,

        // A node has been recommissioned after an incomplete decommission.
        recommissioned,

        // All reallocations associated with a given node update have completed
        // (e.g. it's been fully decommissioned, indicating it can no longer be
        // recommissioned).
        reallocation_finished,
    };

    // Node update information to be processed by the members_backend.
    struct node_update {
        model::node_id id;
        node_update_type type;
        model::offset offset;

        friend std::ostream& operator<<(std::ostream&, const node_update&);
    };

    using uuid_map_t = absl::flat_hash_map<model::node_uuid, model::node_id>;

    members_manager(
      consensus_ptr,
      ss::sharded<controller_stm>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<members_table>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<storage::api>&,
      ss::sharded<drain_manager>&,
      ss::sharded<ss::abort_source>&);

    // Initializes connections to all known members.
    ss::future<> start();

    // Sends a join RPC if we aren't already a member, else sends a node
    // configuration update if our local state differs from that stored in the
    // members table.
    //
    // This is separate to start() so that calling it can be delayed until
    // after our internal RPC listener is up: as soon as we send a join
    // message, the controller leader will expect us to be listening for its
    // raft messages, and if we're not ready it'll back off and make joining
    // take several seconds longer than it should.
    // (ref https://github.com/redpanda-data/redpanda/issues/3030)
    ss::future<> join_cluster();

    // Stop this manager. Only prevents new update requests; pending updates in
    // the queue are aborted separately.
    ss::future<> stop();

    // If the given request contains a node ID, adds a node to the controller
    // Raft group, dispatching to the leader if necessary. If the node already
    // exists, just updates the node config instead.
    //
    // If no node ID is provided (indicated by a negative value), replicates a
    // controller command to register the requested node UUID, responding with
    // a newly assigned node ID.
    ss::future<result<join_node_reply>>
    handle_join_request(join_node_request const r);

    // Applies a committed record batch, specializing handling based on the
    // batch type.
    ss::future<std::error_code> apply_update(model::record_batch);

    // Updates the configuration of a node in the existing controller Raft
    // config, dispatching to the leader if necessary.
    ss::future<result<configuration_update_reply>>
      handle_configuration_update_request(configuration_update_request);

    // Whether the given batch applies to this raft::mux_state_machine.
    bool is_batch_applicable(const model::record_batch& b) {
        return b.header().type == model::record_batch_type::node_management_cmd
               || b.header().type
                    == model::record_batch_type::raft_configuration;
    }

    // This API is backed by the seastar::queue. It can not be called
    // concurrently from multiple fibers.
    ss::future<std::vector<node_update>> get_node_updates();

    // Returns the node ID for the given node UUID. Callers must have a
    // guarantee that the UUID has already been registered before calling.
    model::node_id get_node_id(const model::node_uuid&);

    // Initialize `_id_by_uuid`. Should be called once only when bootstrapping a
    // cluster.
    void apply_initial_node_uuid_map(uuid_map_t);

private:
    using seed_iterator = std::vector<config::seed_server>::const_iterator;
    // Cluster join
    void join_raft0();
    bool is_already_member() const;

    ss::future<> initialize_broker_connection(const model::broker&);

    ss::future<result<join_node_reply>> replicate_new_node_uuid(
      const model::node_uuid&,
      const std::optional<model::node_id>& = std::nullopt);

    // Returns the node ID for a given node UUID, assigning one if one does not
    // already exist. Returns nullopt when there are no more node IDs left.
    std::optional<model::node_id>
    get_or_assign_node_id(const model::node_uuid&);

    // Attempts to register the given node ID with the given node UUID. If a
    // different node ID exists for the given node UUID, returns false.
    //
    // Does not check for duplicate node IDs.
    bool try_register_node_id(const model::node_id&, const model::node_uuid&);

    ss::future<result<join_node_reply>> dispatch_join_to_seed_server(
      seed_iterator it, join_node_request const& req);
    ss::future<result<join_node_reply>> dispatch_join_to_remote(
      const config::seed_server&, join_node_request&& req);

    ss::future<join_node_reply> dispatch_join_request();
    template<typename Func>
    auto dispatch_rpc_to_leader(rpc::clock_type::duration, Func&& f);

    // Raft 0 config updates
    ss::future<>
      handle_raft0_cfg_update(raft::group_configuration, model::offset);
    ss::future<> update_connections(patch<broker_ptr>);

    ss::future<> maybe_update_current_node_configuration();
    ss::future<> dispatch_configuration_update(model::broker);
    ss::future<result<configuration_update_reply>>
      do_dispatch_configuration_update(model::broker, model::broker);

    template<typename Cmd>
    ss::future<std::error_code> dispatch_updates_to_cores(model::offset, Cmd);

    ss::future<std::error_code>
      apply_raft_configuration_batch(model::record_batch);

    const std::vector<config::seed_server> _seed_servers;
    const model::broker _self;
    simple_time_jitter<model::timeout_clock> _join_retry_jitter;
    const std::chrono::milliseconds _join_timeout;
    const consensus_ptr _raft0;

    ss::sharded<controller_stm>& _controller_stm;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<members_table>& _members_table;

    ss::sharded<rpc::connection_cache>& _connection_cache;

    // Partition allocator to update when receiving node lifecycle commands.
    ss::sharded<partition_allocator>& _allocator;

    // Storage with which to look at the kv-store to store and verify
    // configuration invariants.
    //
    // TODO: since the members manager is only ever on shard-0, seems like we
    // should just be passing a single reference to the kv-store.
    ss::sharded<storage::api>& _storage;

    // Per-core management of operations that remove leadership away from a
    // node. Needs to be per-core since each shard is managed by a different
    // shard of the sharded partition_manager.
    ss::sharded<drain_manager>& _drain_manager;

    ss::sharded<ss::abort_source>& _as;

    const config::tls_config _rpc_tls_config;

    // Gate with which to guard new work (e.g. if stop() has been called).
    ss::gate _gate;

    // Cluster membership updates that have yet to be released via the call to
    // get_node_updates().
    ss::queue<node_update> _update_queue;

    uuid_map_t _id_by_uuid;

    model::node_id _next_assigned_id;

    // Subscription to _as with which to signal an abort to _update_queue.
    ss::abort_source::subscription _queue_abort_subscription;

    // The last config update controller log offset for which we successfully
    // updated our broker connections.
    model::offset _last_connection_update_offset;
};

std::ostream&
operator<<(std::ostream&, const members_manager::node_update_type&);
} // namespace cluster
