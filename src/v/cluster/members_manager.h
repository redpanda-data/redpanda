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
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "raft/group_configuration.h"
#include "random/simple_time_jitter.h"
#include "rpc/fwd.h"
#include "serde/envelope.h"
#include "storage/fwd.h"

#include <chrono>
#include <vector>

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
// batch being committed. It reacts to node operations commands like
// adding/updating/removing nodes.
//
// All the updates are propagated to core-local cluster::members_table
// instances. There is only one instance of members_manager running on
// core-0. The members_manager is also responsible for validation of node
// configuration invariants.
//
// Members manager stores a snapshot of cluster state in KV store to be able to
// access it immediately after it starts, this way it can initialize connections
// and send hello requests to other cluster members.
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
      register_node_uuid_cmd,
      add_node_cmd,
      remove_node_cmd,
      update_node_cfg_cmd>{};
    static constexpr ss::shard_id shard = 0;
    static constexpr size_t max_updates_queue_size = 100;

    // Node update information to be processed by the members_backend.
    struct node_update {
        model::node_id id;
        node_update_type type;
        model::offset offset;
        // indicates if command needs a raft 0 configuration update
        bool need_raft0_update = false;
        // revision of a related decommission command, present only in
        // recommission node_update
        std::optional<model::revision_id> decommission_update_revision;

        friend bool operator==(const node_update&, const node_update&)
          = default;

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
      ss::sharded<partition_balancer_state>&,
      ss::sharded<ss::abort_source>&,
      std::chrono::milliseconds);

    /**
     * Initializes connections to brokers. If provided a non-empty list, it's
     * expected that this is the first time the cluster is started, and the
     * given brokers are used to initialize the connections. Otherwise, the
     * brokers are determined from existing state (e.g. the KV-store or Raft)
     */
    ss::future<> start(std::vector<model::broker>);

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
    handle_join_request(const join_node_request r);

    // Applies a committed record batch, specializing handling based on the
    // batch type.
    ss::future<std::error_code> apply_update(model::record_batch);

    // Updates the configuration of a node in the existing controller Raft
    // config, dispatching to the leader if necessary.
    ss::future<result<configuration_update_reply>>
      handle_configuration_update_request(configuration_update_request);

    // Whether the given batch applies to this raft::mux_state_machine.
    bool is_batch_applicable(const model::record_batch& b) const;

    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&);

    // This API is backed by the seastar::queue. It can not be called
    // concurrently from multiple fibers.
    ss::future<std::vector<node_update>> get_node_updates();

    // Returns the node ID for the given node UUID. Callers must have a
    // guarantee that the UUID has already been registered before calling.
    model::node_id get_node_id(const model::node_uuid&);

    // Initialize `_id_by_uuid` and brokers list. Should be called once only
    // when bootstrapping a cluster.
    ss::future<>
      set_initial_state(std::vector<model::broker>, uuid_map_t, model::offset);

    // Returns a reference to a map containing mapping between node ids and node
    // uuids. Node UUID is node globally unique identifier which has an id
    // assigned during join.
    const uuid_map_t& get_id_by_uuid_map() const { return _id_by_uuid; }

private:
    using seed_iterator = std::vector<config::seed_server>::const_iterator;
    struct changed_nodes {
        std::vector<model::broker> added;
        std::vector<model::broker> updated;
        std::vector<model::node_id> removed;
    };

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
      seed_iterator it, const join_node_request& req);
    ss::future<result<join_node_reply>> dispatch_join_to_remote(
      const config::seed_server&, join_node_request&& req);

    ss::future<join_node_reply> dispatch_join_request();
    template<typename Func>
    auto dispatch_rpc_to_leader(rpc::clock_type::duration, Func&& f);

    // Raft 0 config updates
    ss::future<>
      handle_raft0_cfg_update(raft::group_configuration, model::offset);
    changed_nodes
    calculate_changed_nodes(const raft::group_configuration&) const;

    ss::future<> maybe_update_current_node_configuration();
    ss::future<> dispatch_configuration_update(model::broker);
    ss::future<result<configuration_update_reply>>
      do_dispatch_configuration_update(
        model::node_id, net::unresolved_address, model::broker);

    template<typename Cmd>
    ss::future<std::error_code> dispatch_updates_to_cores(model::offset, Cmd);

    ss::future<std::error_code>
      apply_raft_configuration_batch(model::record_batch);

    ss::future<std::error_code> do_apply_add_node(add_node_cmd, model::offset);
    ss::future<std::error_code>
      do_apply_update_node(update_node_cfg_cmd, model::offset);
    ss::future<std::error_code>
      do_apply_remove_node(remove_node_cmd, model::offset);

    ss::future<std::error_code> add_node(model::broker);
    ss::future<std::error_code> update_node(model::broker);

    ss::future<join_node_reply> make_join_node_success_reply(model::node_id id);

    bool command_based_membership_active() const {
        return _feature_table.local().is_active(
          features::feature::membership_change_controller_cmds);
    }

    struct members_snapshot
      : serde::envelope<
          members_snapshot,
          serde::version<0>,
          serde::compat_version<0>> {
        std::vector<model::broker> members;
        model::offset update_offset;
        auto serde_fields() { return std::tie(members, update_offset); }
    };
    /**
     * In order to be able to determine the current cluster configuration before
     * raft-0 log is replied or controller snapshot is applied we store
     * configuration in kv-store. Members are persisted every time the cluster
     * configuration changes.
     */
    ss::future<> persist_members_in_kvstore(model::offset);
    members_snapshot read_members_from_kvstore();

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

    ss::sharded<partition_balancer_state>& _pb_state;

    ss::sharded<ss::abort_source>& _as;

    // A set of node ids for which the removed command has already been
    // processed, but that are still members of the controller group. We can
    // close a connection to these nodes only after they leave the controller
    // group and are fully removed.
    absl::flat_hash_set<model::node_id> _removed_nodes_still_in_raft0;

    const config::tls_config _rpc_tls_config;

    // Gate with which to guard new work (e.g. if stop() has been called).
    ss::gate _gate;

    // Node membership updates that are currently processed by members_backend
    absl::flat_hash_map<model::node_id, node_update> _in_progress_updates;

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
    // Offset of first node operation command in controller log. This
    // information is used to prevent members manager fro applying controller
    // raft0 configuration after the node command was applied, we can not rely
    // on a feature being enabled as even when feature is active it may still be
    // required to apply raft-0 configuration as they contain cluster membership
    // state changes.
    model::offset _first_node_operation_command_offset = model::offset::max();
    std::chrono::milliseconds _application_start_time;
};

} // namespace cluster
