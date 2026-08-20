/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/state_machine_registry.h"
#include "container/chunked_hash_map.h"
#include "kafka/server/consumer_group.h"
#include "kafka/server/group_data_parser.h"
#include "raft/persisted_stm.h"

#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>

namespace kafka {

/// \brief The consumer groups of one `__consumer_offsets` partition.
///
/// Consumer-protocol group state is owned here, on every replica, rather than
/// in a leader-side map the way classic group state is: one do_apply applies
/// each committed record to the group it belongs to, live and during recovery
/// alike, so a new leader already has current state. The leader reads groups
/// through the group manager to serve requests, computes the records a request
/// implies, replicates them, and waits for this state machine to apply them.
///
/// The state machine applies, per group it owns:
///
/// - the `ConsumerGroup*` records, into the group's epochs, members and
///   target assignment;
/// - offset commit records, into the group's composed offset_store;
/// - the group transaction control batches, into the same offset_store,
///   which is why it also owns the compaction bound that keeps an open
///   transaction's records in the log.
///
/// Everything else on the shared partition is ignored: classic group records
/// belong to the classic coordinator, and offset records are applied only for
/// group ids owned here. Apply is strictly in log order, and a tombstone for
/// state the machine has no record of is a no-op, which is what makes replay
/// of a compacted log converge. Records are applied as the log has them: the
/// writer is what checks a deletion is legal, and applied state that
/// contradicted the log could never converge back to it.
///
/// Recovery is full log replay; no local snapshot is taken. A snapshot loaded
/// over a log whose tombstones were since removed would resurrect the deleted
/// keys, so snapshotting waits for the storage layer to clamp tombstone
/// removal to this machine's snapshot offset.
class consumer_group_stm final
  : public raft::persisted_stm<>
  , public group_data_parser<consumer_group_stm> {
public:
    static constexpr std::string_view name = "consumer_group_stm";

    consumer_group_stm(
      ss::logger&, raft::consensus*, ss::sharded<features::feature_table>&);

    using groups_map
      = chunked_hash_map<kafka::group_id, ss::lw_shared_ptr<consumer_group>>;

    /// The committed offsets of every group id on the partition, whichever
    /// protocol owns it. Offset records carry nothing that says which
    /// protocol wrote them, and a group's own records can replay after its
    /// offsets, so ownership cannot decide whether to apply one.
    using offsets_map
      = chunked_hash_map<kafka::group_id, ss::lw_shared_ptr<offset_store>>;

    /// The group, or nullptr if this partition's log has not created one by
    /// that id. Only meaningful once the state machine is caught up: a leader
    /// serving reads gates on sync() first.
    ss::lw_shared_ptr<consumer_group> get_group(const kafka::group_id&) const;

    const groups_map& groups() const { return _groups; }

    /// The offsets of a group id, or nullptr if none have been applied for it.
    /// Held for ids this machine does not own, so that a group created after
    /// its offsets finds them.
    ss::lw_shared_ptr<offset_store> get_offsets(const kafka::group_id&) const;

    const offsets_map& offsets() const { return _offsets; }

    /// The catch-up gate for leader-side reads: resolves true once this
    /// machine has applied everything committed by previous terms. Holds the
    /// state machine's gate, which the base class requires of an external
    /// caller and which stop() then waits for.
    ss::future<bool> sync(model::timeout_clock::duration timeout);

    ss::future<> do_apply(const model::record_batch&) override;

    /// Clamped below the earliest open transaction across the owned groups,
    /// so a fence whose commit or abort has not landed stays in the log for
    /// replay to re-establish.
    model::offset max_removable_local_log_offset() override;

    ss::future<raft::local_snapshot_applied>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;

    ss::future<raft::stm_snapshot>
      take_local_snapshot(ssx::semaphore_units) override;

    ss::future<> apply_raft_snapshot(const iobuf&) final;
    ss::future<iobuf> take_raft_snapshot(model::offset) final;

    ss::future<> start() final;

    raft::stm_initial_recovery_policy
    get_initial_recovery_policy() const final {
        return raft::stm_initial_recovery_policy::read_everything;
    }

    ss::future<> stop() final;

    ss::future<> handle_raft_data(const model::record_batch&);
    ss::future<> apply_record(model::record, model::offset log_offset);
    ss::future<> handle_tx_offsets(
      model::record_batch_header, kafka::group_tx::offsets_metadata);
    ss::future<> handle_fence_v0(
      model::record_batch_header, kafka::group_tx::fence_metadata_v0);
    ss::future<> handle_fence_v1(
      model::record_batch_header, kafka::group_tx::fence_metadata_v1);
    ss::future<>
      handle_fence(model::record_batch_header, kafka::group_tx::fence_metadata);
    ss::future<>
      handle_abort(model::record_batch_header, kafka::group_tx::abort_metadata);
    ss::future<> handle_commit(
      model::record_batch_header, kafka::group_tx::commit_metadata);
    ss::future<> handle_version_fence(features::feature_table::version_fence);
    void handle_group_block(kafka::group_block);
    group_block_info_map& group_blocks() { return _group_blocks; }
    const group_block_info_map& group_blocks() const { return _group_blocks; }

private:
    /// take_local_snapshot cannot be opted out of, so it writes an empty
    /// placeholder that apply_local_snapshot rejects in favor of log replay.
    static constexpr int8_t local_snapshot_version = 1;

    ss::future<> apply_group_metadata(consumer_group_metadata_kv);
    void apply_member_metadata(consumer_group_member_metadata_kv);
    void apply_target_assignment_metadata(
      consumer_group_target_assignment_metadata_kv);
    void apply_target_assignment_member(
      consumer_group_target_assignment_member_kv);
    void apply_current_member_assignment(
      consumer_group_current_member_assignment_kv);
    void apply_offset_metadata(offset_metadata_kv, model::offset log_offset);

    consumer_group& get_or_create_group(const kafka::group_id&);
    ss::lw_shared_ptr<offset_store>
    get_or_create_offsets(const kafka::group_id&);

    groups_map _groups;
    offsets_map _offsets;
    group_block_info_map _group_blocks;
    /// Handed to every group's offset_store, which read-locks it around
    /// aborts. Never write-locked here: the classic loading gate it mirrors
    /// has no equivalent, since this state is never bulk-loaded outside apply.
    ss::lw_shared_ptr<ss::rwlock> _catchup_lock;
    ss::sharded<features::feature_table>& _feature_table;
};

class consumer_group_stm_factory : public cluster::state_machine_factory {
public:
    explicit consumer_group_stm_factory(ss::sharded<features::feature_table>&);
    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(
      raft::state_machine_manager_builder&,
      raft::consensus*,
      const cluster::stm_instance_config&) final;

private:
    ss::sharded<features::feature_table>& _feature_table;
};

} // namespace kafka
