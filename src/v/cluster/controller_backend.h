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

#include "base/outcome.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_placement_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/property.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/group_configuration.h"
#include "storage/api.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/btree_map.h>
#include <absl/container/node_hash_map.h>

#include <cstdint>
#include <iosfwd>
#include <optional>

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
 * represent a notification that something has changed for an ntp in question,
 * f.e. it was added, removed, its replica set was changed etc.
 *
 * Each controller backend in the cluster (on each node and each core) process
 * all the deltas. For each NTP it was notified of, it queries the topic table
 * state and if it diverges from the local state, it performs actions to
 * reconcile local state with the desired state (state reconciliation pattern).
 *
 * Reconciliation for each NTP is done in a separate fiber in other words
 * different NTPs are reconciled concurrently but for the same NTP
 * the reconciliation process is sequential.
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
 * distinguishable states we use revision_id as an epoch. Each partition tracks
 * two revisions: log_revision (the offset of the command that created a
 * replica on this node, it also appears in the partition log directory name)
 * and cmd_revision (sometimes referred to simply as revision) - the offset of
 * the command that last modified the replica configuration. This way controller
 * backend is able to recognize if partition replicas have already been updated
 * or if action is required.
 *
 * ## Revisions and raft vnode
 *
 * Whenever a new replica is added to raft configuration it has new log_revision
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
    controller_backend(
      ss::sharded<cluster::topic_table>&,
      ss::sharded<shard_placement_table>&,
      ss::sharded<shard_table>&,
      ss::sharded<partition_manager>&,
      ss::sharded<members_table>&,
      ss::sharded<cluster::partition_leaders_table>&,
      ss::sharded<topics_frontend>&,
      ss::sharded<storage::api>&,
      ss::sharded<features::feature_table>&,
      config::binding<std::chrono::milliseconds> housekeeping_interval,
      config::binding<std::optional<size_t>>
        initial_retention_local_target_bytes,
      config::binding<std::optional<std::chrono::milliseconds>>
        initial_retention_local_target_ms,
      config::binding<std::optional<size_t>>
        retention_local_target_bytes_default,
      config::binding<std::chrono::milliseconds>
        retention_local_target_ms_default,
      config::binding<bool> retention_local_strict,
      ss::sharded<seastar::abort_source>&);
    ~controller_backend();

    ss::future<> stop();
    ss::future<> start();

    struct in_progress_operation {
        model::revision_id revision;
        partition_operation_type type;
        partition_assignment assignment;

        uint64_t retries = 0;
        cluster::errc last_error = errc::success;

        friend std::ostream&
        operator<<(std::ostream&, const in_progress_operation&);
    };

    std::optional<in_progress_operation>
    get_current_op(const model::ntp&) const;

    void notify_reconciliation(const model::ntp&);

    /// Copy partition kvstore data from an extra shard (i.e. kvstore shard that
    /// is >= ss::smp::count). This method is expected to be called *before*
    /// start().
    ss::future<> transfer_partitions_from_extra_shard(
      storage::kvstore&, shard_placement_table&);

private:
    struct ntp_reconciliation_state;
    using force_reconfiguration
      = ss::bool_class<struct force_reconfiguration_tag>;

    // Topics
    ss::future<> bootstrap_controller_backend();
    /**
     * Function that will clean orphan topic files on redpanda startup
     * Orphan topic files is files that left on disk after some node
     * manipullations and redpanda doesn't know about these files, so it is
     * impossible to remove them with default approach. Currently we may leave
     * orphan topic files when we restart redpanda while partition deletion
     * operation was evaluating but hasn't finished yet.
     * We assume that we can leave orphan files only on redpanda restart
     * so we run clean process on startup, after bootstrap when redpanda
     * already knows about all topics that it should containt on disk
     **/
    ss::future<> clear_orphan_topic_files(
      model::revision_id bootstrap_revision,
      absl::flat_hash_map<model::ntp, model::revision_id> topic_table_snapshot);

    void process_delta(const topic_table::ntp_delta&);

    ss::future<> reconcile_ntp_fiber(
      model::ntp, ss::lw_shared_ptr<ntp_reconciliation_state>);
    ss::future<>
    try_reconcile_ntp(const model::ntp&, ntp_reconciliation_state&);
    ss::future<result<ss::stop_iteration>>
    reconcile_ntp_step(const model::ntp&, ntp_reconciliation_state&);

    ss::future<> stuck_ntp_watchdog_fiber();

    /**
     * Given the original and new replica set for a force configuration, splits
     * the new replica set into voters and learners and returns the equivalent
     * pair.
     */
    using vnodes = std::vector<raft::vnode>;
    std::pair<vnodes, vnodes> split_voters_learners_for_force_reconfiguration(
      const replicas_t& original,
      const replicas_t& new_replicas,
      const replicas_revision_map&,
      model::revision_id command_revision);

    ss::future<std::error_code> create_partition(
      model::ntp,
      raft::group_id,
      model::revision_id log_revision,
      replicas_t initial_replicas,
      force_reconfiguration is_force_reconfigured);

    ss::future<> add_to_shard_table(
      model::ntp,
      raft::group_id,
      ss::shard_id,
      model::revision_id log_revision);
    ss::future<> remove_from_shard_table(
      model::ntp, raft::group_id, model::revision_id log_revision);

    ss::future<xshard_transfer_state>
      shutdown_partition(ss::lw_shared_ptr<partition>);

    ss::future<std::error_code> transfer_partition(
      model::ntp, raft::group_id, model::revision_id log_revision);

    ss::future<std::error_code> delete_partition(
      model::ntp,
      std::optional<shard_placement_table::placement_state>,
      model::revision_id cmd_revision,
      partition_removal_mode mode);

    ss::future<> remove_partition_kvstore_state(
      model::ntp, raft::group_id, model::revision_id log_revision);

    ss::future<> transfer_partition_from_extra_shard(
      const model::ntp&,
      shard_placement_table::placement_state,
      storage::kvstore&,
      shard_placement_table&);

    ss::future<result<ss::stop_iteration>> reconcile_partition_reconfiguration(
      ntp_reconciliation_state&,
      ss::lw_shared_ptr<partition>,
      const topic_table::in_progress_update&,
      const replicas_revision_map& replicas_revisions);

    template<typename Func>
    ss::future<result<ss::stop_iteration>> apply_configuration_change_on_leader(
      ss::lw_shared_ptr<partition>,
      const replicas_t& target_replicas,
      model::revision_id cmd_revision,
      Func&&);
    ss::future<result<ss::stop_iteration>> update_partition_replica_set(
      ss::lw_shared_ptr<partition>,
      const replicas_t& target_replicas,
      const replicas_revision_map& initial_replicas_revisions,
      model::revision_id cmd_revision,
      reconfiguration_policy);
    ss::future<result<ss::stop_iteration>> cancel_replica_set_update(
      ss::lw_shared_ptr<partition>,
      const replicas_t& target_replicas,
      const replicas_revision_map& initial_replicas_revisions,
      const replicas_t& previous_replicas,
      model::revision_id cmd_revision);
    ss::future<result<ss::stop_iteration>> force_abort_replica_set_update(
      ss::lw_shared_ptr<partition>,
      const replicas_t& target_replicas,
      const replicas_revision_map& initial_replicas_revisions,
      const replicas_t& previous_replicas,
      model::revision_id cmd_revision);
    ss::future<result<ss::stop_iteration>> force_replica_set_update(
      ss::lw_shared_ptr<partition>,
      const replicas_t& previous_replicas,
      const replicas_t& new_replicas,
      const replicas_revision_map& initial_replicas_revisions,
      model::revision_id cmd_revision);

    bool can_finish_update(
      std::optional<model::node_id> current_leader,
      uint64_t current_retry,
      reconfiguration_state,
      const replicas_t& requested_replicas);

    ss::future<std::error_code>
      dispatch_update_finished(model::ntp, replicas_t);

    ss::future<std::error_code> dispatch_revert_cancel_move(model::ntp);

    void setup_metrics();

    bool command_based_membership_active() const;

    bool should_skip(const model::ntp&) const;

    std::optional<model::offset> calculate_learner_initial_offset(
      reconfiguration_policy policy,
      const ss::lw_shared_ptr<partition>& partition) const;

    ss::sharded<topic_table>& _topics;
    shard_placement_table& _shard_placement;
    ss::sharded<shard_table>& _shard_table;
    ss::sharded<partition_manager>& _partition_manager;
    ss::sharded<members_table>& _members_table;
    ss::sharded<partition_leaders_table>& _partition_leaders_table;
    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<storage::api>& _storage;
    ss::sharded<features::feature_table>& _features;
    model::node_id _self;
    ss::sstring _data_directory;
    config::binding<std::chrono::milliseconds> _housekeeping_interval;
    simple_time_jitter<ss::lowres_clock> _housekeeping_jitter;
    config::binding<std::optional<size_t>>
      _initial_retention_local_target_bytes;
    config::binding<std::optional<std::chrono::milliseconds>>
      _initial_retention_local_target_ms;
    config::binding<std::optional<size_t>>
      _retention_local_target_bytes_default;
    config::binding<std::chrono::milliseconds>
      _retention_local_target_ms_default;
    config::binding<bool> _retention_local_strict;
    ss::sharded<ss::abort_source>& _as;

    absl::btree_map<model::ntp, ss::lw_shared_ptr<ntp_reconciliation_state>>
      _states;
    // Will hold xshard_transfer_state for partitions that are being transferred
    // to this shard.
    chunked_hash_map<model::ntp, xshard_transfer_state> _xst_states;

    cluster::notification_id_type _topic_table_notify_handle
      = notification_id_type_invalid;
    // Limits the number of concurrently executing reconciliation fibers.
    // Initially reconciliation is blocked and we deposit a non-zero amount of
    // units when we are ready to start reconciling.
    ssx::semaphore _reconciliation_sem{0, "c/controller-be"};
    ss::gate _gate;

    metrics::internal_metric_groups _metrics;
};

} // namespace cluster
