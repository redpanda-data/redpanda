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

#include "cluster/fwd.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "outcome.h"
#include "raft/group_configuration.h"
#include "storage/api.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/node_hash_map.h>

#include <ostream>

namespace cluster {

/**
 *
 * # Reconciliation
 *
 * Controller backend is responsible for making sure that the cluster state is
 * in align with the topic and partition state gathered in topic_table.
 *
 * Controller backend lives on each core on every node in the cluster. Each
 * instance of controller backend is responsible for dealing with core & node
 * local partition replicas (instances of `cluster::partition` object that are
 * supposed to be instantiated on given core and given node). Controller backend
 * manages partition replica lifecycle. It instantiates/deletes
 * `cluster::partition` instances and registers them in shard table.
 *
 * Controller backend operations are driven by deltas generated in topics table.
 * Backend waits for the new deltas using condition variable. Each delta
 * represent an operation that must be executed for ntp f.e. create, update
 * properties, move, etc.
 *
 * Each controller backend in the cluster (on each node and each core) process
 * all the deltas and based on the situation it either executes an operation or
 * ignore it (command pattern).
 *
 * Deltas vector for each NTP is processed in separate fiber in other words
 * deltas for different NTPs are executed concurrently but for the same NTP
 * sequentially.
 *
 * Each delta has revision assigned revision for the delta is assigned based on
 * the raft0 log offset of command that the delta is related with. The same
 * delta has the same revision globally.
 *
 * Deltas are executed in order from oldest revision up to the newest.
 *
 *
 * NTP_1
 *                                              Loop until finished or cancelled
 *
 *                                                    ┌──────────────────┐
 *                                                    │                  │
 *                                                    │                  │
 * ┌────────────┐ ┌────────────┐ ┌────────────┐       │  ┌────────────┐  │
 * │   delta    │ │   delta    │ │   delta    │       │  │   delta    │  │
 * │            │ │            │ │            ├──►    └─►│            ├──┘
 * │ revision: 3│ │ revision: 2│ │ revision: 1│          │ revision: 0│
 * └────────────┘ └────────────┘ └────────────┘          └────────────┘
 *
 *                            .
 *                            .
 *                            .
 * NTP_N
 *                                              Loop until finished or cancelled
 *
 *                                                    ┌──────────────────┐
 *                                                    │                  │
 *                                                    │                  │
 * ┌────────────┐ ┌────────────┐ ┌────────────┐       │  ┌────────────┐  │
 * │   delta    │ │   delta    │ │   delta    │       │  │   delta    │  │
 * │            │ │            │ │            ├──►    └─►│            ├──┘
 * │ revision: 3│ │ revision: 2│ │ revision: 1│          │ revision: 0│
 * └────────────┘ └────────────┘ └────────────┘          └────────────┘
 *
 * # Revisions
 *
 * As each reconciliation loops are not coordinated we must be able to recognize
 * epochs. Consider a situation in which a stream of deltas executed by the
 * backend leads to the state which is identical from end user perspective f.e.
 * topic with the same name and configuration was deleted and then created back
 * again. We must be able to recognize if the instance of partition replica that
 * has been created for the topic belongs to the original topic or the one that
 * was re created. In order to introduce differentiation between the two not
 * distinguishable states we use revision_id as an epoch. Revision is used
 * whenever partition is created or its replicas are moved. This way controller
 * backend is able to recognize if partition replicas have already been updated
 * or if action is required.
 *
 * ## Revisions and raft vnode
 *
 * Whenever a new replica is added to raft configuration it has new revision
 * assigned. In raft each raft group participant is described by a tuple of
 * model::node_id and model::revision_id. This way every time the node is re
 * added to the configuration (consider a situation in which partition with
 * single replica is moved back and forth between two nodes f.e. 1 -> 2 -> 1
 * -> 2...) it is recognized as a new node. This fencing mechanism prevents the
 * up to date raft group replicas from communicating with one from previous
 * epoch.
 *
 * # Partition movement
 *
 * Partition movement in Redpanda is based on the Raft protocol mechanism called
 * Joint Consensus. When requested Raft implementation is able to move data
 * between nodes in a safe and consistent way. However requesting Raft to
 * reconfigure a raft group is not enough to complete a partition move. When
 * partition move is requested based on the current situation some of the
 * controller backend may have to create new partition replica instances while
 * other have to delete the one that are not longer part of raft group.
 * Additionally there may be a need to move partition instance between cores on
 * the same node.
 *
 * Every time partition move is requested each reconciliation loop executes an
 * operation based on current and requested state and poll for its completion.
 *
 * Partition movement finish is coordinated using a designated finish command.
 *
 * Partition movement finish command is replicated from one of the replicas that
 * was changed during reconfiguration process.
 *
 * IMPORTANT:
 * Partition replicas are only deleted when executing delta for operation
 * finished command. This way when partition replica is deleted it is guaranteed
 * to not longer be needed.
 *
 * Example:
 *
 * Consider moving partition between a set of nodes:
 *
 *      replicas on nodes (1,2,3) -> replicas on nodes (2,3,4)
 *
 * (for simplicity we ignore core assignment in this example)
 *
 * Assumptions:
 *  - node 1 is a leader for the partition.
 *
 * Operations that has to be executed on every node:
 *
 * Node 1:
 * - node 1 is a leader, leader is the only one that can replicate data so it
 * will be asked for reconfiguration
 * - after partition replica is not longer needed on this node it may be removed
 *
 * Node 2 & 3:
 * - node 2 will wait until configuration will be up to date with requested. In
 * case leadership from node 1 moved it will ask for reconfiguration
 *
 * Node 4:
 * - node 4 will create a new instance of partition replica and wait for the
 * configuration to be up to date.
 * - after successful reconfiguration node 4 will dispatch finish update command
 *
 *
 * When finish update command will be received by node 1 it will remove the
 * partition replica instance.
 *
 *
 * ## Interrupting partition movement
 *
 * Partition movement interruption may only be accepted after topic table
 * processed move command but before the finish update command was processed. We
 * use topics table as a single source of truth to decide if the update may
 * still be canceled or if it has finished. This way we must be able to revert
 * configuration change even if raft already finished reconfiguration.
 *
 * Partition move interruption does not mark the reconfiguration process as
 * finished i.e. it will still be represented as in progress when queried from
 * topic table. Move interruption will only finish when reconfiguration is
 * finished in raft and finish move command is issued by the controller backend
 *
 * In general the interrupt may happen in the following situations:
 *
 * 1) before raft reconfiguration was requested
 * 2) when raft reconfiguration is in progress
 * 3) when raft reconfiguration has already finished but before finish command
 * was replicated
 *
 * In all of the situations we must move back to the raft group configuration
 * which was active before the move was scheduled. The set of actions that must
 * be taken to finish the interruption is different based on the situation in
 * which interruption happened.
 *
 * For 1) controller backend must simply update raft configuration revision to
 * be able to decide if action related with given revision_id has been executed.
 *
 * For 2) controller backend with request reconfiguration cancellation on a
 * leader and will wait until raft configuration is up to date with what was
 * observed before the move. Any replicas that were created for the purpose of
 * move will be removed when processing finished move command.
 *
 * For 3) controller backend must request reconfiguration with the same exact
 * replica set as before the move was requested. It is important to notice that
 * no partition replicas were yet removed as finish command wasn't yet
 * processed. Since cancelling partition move does not create new partition
 * replica instances (instances of `cluster::partition`) but reuse the existing
 * one we must reuse revision id of currently existing replica instances.
 *
 */
class controller_backend
  : public ss::peering_sharded_service<controller_backend> {
public:
    using results_t = std::vector<std::error_code>;
    controller_backend(
      ss::sharded<cluster::topic_table>&,
      ss::sharded<shard_table>&,
      ss::sharded<partition_manager>&,
      ss::sharded<members_table>&,
      ss::sharded<cluster::partition_leaders_table>&,
      ss::sharded<topics_frontend>&,
      ss::sharded<storage::api>&,
      ss::sharded<seastar::abort_source>&);

    ss::future<> stop();
    ss::future<> start();

    std::vector<topic_table::delta> list_ntp_deltas(const model::ntp&) const;

private:
    struct cross_shard_move_request {
        cross_shard_move_request(model::revision_id, raft::group_configuration);

        model::revision_id revision;
        raft::group_configuration initial_configuration;
        friend std::ostream& operator<<(
          std::ostream& o,
          const controller_backend::cross_shard_move_request& r) {
            fmt::print(
              o,
              "{{revision: {}, configuration: {}}}",
              r.revision,
              r.initial_configuration);
            return o;
        }
    };

    using deltas_t = std::vector<topic_table::delta>;
    using underlying_t = absl::flat_hash_map<model::ntp, deltas_t>;

    // Topics
    ss::future<> bootstrap_controller_backend();
    void start_topics_reconciliation_loop();

    ss::future<> fetch_deltas();

    ss::future<> reconcile_topics();
    ss::future<> reconcile_ntp(deltas_t&);

    ss::future<std::error_code> execute_partition_op(const topic_table::delta&);
    ss::future<std::error_code> process_partition_reconfiguration(
      topic_table_delta::op_type,
      model::ntp,
      const partition_assignment&,
      const std::vector<model::broker_shard>&,
      const topic_table_delta::revision_map_t&,
      model::revision_id);

    ss::future<std::error_code> execute_reconfiguration(
      topic_table_delta::op_type,
      const model::ntp&,
      const std::vector<model::broker_shard>&,
      const topic_table_delta::revision_map_t&,
      model::revision_id);

    ss::future<> finish_partition_update(
      model::ntp, const partition_assignment&, model::revision_id);

    ss::future<>
      process_partition_properties_update(model::ntp, partition_assignment);

    ss::future<std::error_code> create_partition(
      model::ntp,
      raft::group_id,
      model::revision_id,
      std::vector<model::broker>);
    ss::future<> add_to_shard_table(
      model::ntp, raft::group_id, ss::shard_id, model::revision_id);
    ss::future<>
      remove_from_shard_table(model::ntp, raft::group_id, model::revision_id);
    ss::future<>
    cleanup_orphan_files(const topic_table::delta&, model::revision_id);
    ss::future<> delete_partition(
      model::ntp, model::revision_id, partition_removal_mode mode);
    template<typename Func>
    ss::future<std::error_code> apply_configuration_change_on_leader(
      const model::ntp&,
      const std::vector<model::broker_shard>&,
      model::revision_id,
      Func&& f);
    ss::future<std::error_code> update_partition_replica_set(
      const model::ntp&,
      const std::vector<model::broker_shard>&,
      const topic_table_delta::revision_map_t&,
      model::revision_id);
    ss::future<std::error_code> cancel_replica_set_update(
      const model::ntp&,
      const std::vector<model::broker_shard>&,
      const topic_table_delta::revision_map_t&,
      model::revision_id);

    ss::future<std::error_code> force_abort_replica_set_update(
      const model::ntp&,
      const std::vector<model::broker_shard>&,
      const topic_table_delta::revision_map_t&,
      model::revision_id);

    ss::future<std::error_code>
      dispatch_update_finished(model::ntp, partition_assignment);

    ss::future<> do_bootstrap();
    ss::future<> bootstrap_ntp(const model::ntp&, deltas_t&);

    ss::future<std::error_code>
      shutdown_on_current_shard(model::ntp, model::revision_id);

    ss::future<std::optional<cross_shard_move_request>>
      acquire_cross_shard_move_request(model::ntp, ss::shard_id);

    ss::future<> release_cross_shard_move_request(
      model::ntp, ss::shard_id, cross_shard_move_request);

    ss::future<std::error_code> create_partition_from_remote_shard(
      model::ntp, ss::shard_id, partition_assignment);

    bool can_finish_update(
      topic_table_delta::op_type,
      const std::vector<model::broker_shard>&,
      const std::vector<model::broker_shard>&);

    void housekeeping();
    void setup_metrics();
    ss::sharded<topic_table>& _topics;
    ss::sharded<shard_table>& _shard_table;
    ss::sharded<partition_manager>& _partition_manager;
    ss::sharded<members_table>& _members_table;
    ss::sharded<partition_leaders_table>& _partition_leaders_table;
    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<storage::api>& _storage;
    model::node_id _self;
    ss::sstring _data_directory;
    std::chrono::milliseconds _housekeeping_timer_interval;
    ss::sharded<ss::abort_source>& _as;
    underlying_t _topic_deltas;
    ss::timer<> _housekeeping_timer;
    ssx::semaphore _topics_sem{1, "c/controller-be"};
    ss::gate _gate;
    /**
     * This map is populated by backend instance on shard that given NTP is
     * moved from. Map is then queried by the controller instance on target
     * shard. Partition is created on target shard with the same initial
     * revision and configuration as on originating shard, this way identity of
     * node i.e. raft vnode doesn't change.
     */
    absl::node_hash_map<model::ntp, cross_shard_move_request>
      _cross_shard_requests;
    /**
     * This map is populated when bootstrapping. If partition is moved cross
     * shard on the same node it has to be created with revision that it was
     * first created on current node before cross core move series
     */
    absl::node_hash_map<model::ntp, model::revision_id> _bootstrap_revisions;
    ss::metrics::metric_groups _metrics;
};

std::vector<topic_table::delta> calculate_bootstrap_deltas(
  model::node_id self, const std::vector<topic_table::delta>&);
} // namespace cluster
