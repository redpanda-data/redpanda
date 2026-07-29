// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller_utils.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/partition.h"
#include "cluster/snapshot.h"
#include "cluster/types.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "storage/disk_log_impl.h"

#include <seastar/core/future.hh>

#include <chrono>
#include <iterator>

namespace cluster {

std::optional<model::revision_id> log_revision_on_node(
  const topic_table::partition_replicas_view& replicas_view,
  model::node_id node) {
    if (contains_node(replicas_view.orig_replicas(), node)) {
        return replicas_view.revisions().at(node);
    }

    if (
      replicas_view.update
      && contains_node(replicas_view.update->get_target_replicas(), node)) {
        return replicas_view.update->get_update_revision();
    }

    return std::nullopt;
}

std::optional<shard_placement_target> placement_target_on_node(
  const topic_table::partition_replicas_view& replicas_view,
  model::node_id node) {
    // The desired partition placement is the following: if there is no replicas
    // update, the partition object should exist only on the assigned nodes and
    // shards. If there is an update, the rules are more complicated: the
    // partition should exist on both new nodes (those not present in the
    // original assignment) and the original nodes for the whole duration of the
    // update. On original nodes, if there is a cross-shard move, the shard is
    // determined by the end state of the move.
    //
    // Example: (x/y means node/shard) suppose there is an update
    // {1/0, 2/0, 3/0} -> {2/1, 3/0, 4/0} in progress. Then the partition object
    // must be present only on 1/0, 2/1, 3/0 and 4/0. If the update is then
    // cancelled, the partition object must be present only on 1/0, 2/0, 3/0,
    // 4/0 (note that the desired shard for node 2 changed).
    //
    // Note also that the partition on new nodes is always created, even if the
    // update is cancelled. This is due to a possibility of "revert_cancel"
    // scenario (i.e. when we cancel a move, but the raft layer has already
    // completed it, in this case we end up with the "updated" replica set, not
    // the original one).

    auto orig_shard_on_node = find_shard_on_node(
      replicas_view.orig_replicas(), node);
    if (orig_shard_on_node) {
        auto log_revision = replicas_view.revisions().at(node);
        auto resulting_shard_on_node = find_shard_on_node(
          replicas_view.resulting_replicas(), node);
        if (resulting_shard_on_node) {
            // partition stays on the node, possibly changing its shard.
            // expected shard is determined by the resulting assignment
            // (including cancellation effects).
            return shard_placement_target{
              replicas_view.assignment.group,
              log_revision,
              resulting_shard_on_node.value()};
        } else {
            // partition is moved away from this node, but we keep the original
            // replica until update is finished.
            return shard_placement_target{
              replicas_view.assignment.group,
              log_revision,
              orig_shard_on_node.value()};
        }
    } else if (replicas_view.update) {
        // if partition appears on the node as a result of the update, create
        // and keep it until the update is finished irrespective of
        // cancellations.
        auto updated_shard_on_node = find_shard_on_node(
          replicas_view.update->get_target_replicas(), node);
        if (updated_shard_on_node) {
            return shard_placement_target{
              replicas_view.assignment.group,
              replicas_view.update->get_update_revision(),
              updated_shard_on_node.value()};
        }
    }
    return std::nullopt;
}

partition_state get_partition_state(ss::lw_shared_ptr<partition> partition) {
    partition_state state{};
    if (!partition || !partition->log() || !partition->log()->stm_hookset())
      [[unlikely]] {
        return state;
    }
    state.start_offset = partition->raft_start_offset();
    state.committed_offset = partition->committed_offset();
    state.last_stable_offset = partition->last_stable_offset();
    state.high_water_mark = partition->high_watermark();
    state.dirty_offset = partition->dirty_offset();
    state.latest_configuration_offset
      = partition->get_latest_configuration_offset();
    state.revision_id = partition->get_revision_id();
    state.log_size_bytes = partition->size_bytes();
    state.non_log_disk_size_bytes = partition->non_log_disk_size_bytes();
    auto& coco = partition->raft()->get_compaction_coordinator();
    state.max_tombstone_removable_offset
      = coco.get_max_tombstone_remove_offset();
    state.max_transaction_removable_offset
      = coco.get_max_transaction_remove_offset();
    state.max_cleanly_compacted_offset
      = coco.get_local_max_cleanly_compacted_offset();
    state.max_transaction_free_offset
      = coco.get_local_max_transaction_free_offset();
    state.is_read_replica_mode_enabled
      = partition->is_read_replica_mode_enabled();
    state.is_remote_fetch_enabled = partition->is_remote_fetch_enabled();
    state.is_cloud_data_available = partition->cloud_data_available();
    state.read_replica_bucket = partition->is_read_replica_mode_enabled()
                                  ? partition->get_read_replica_bucket()()
                                  : "";
    if (state.is_cloud_data_available) {
        state.start_cloud_offset = partition->start_cloud_offset();
        state.next_cloud_offset = partition->next_cloud_offset();
    } else {
        state.start_cloud_offset = state.next_cloud_offset = model::offset{-1};
    }
    state.iceberg_mode = fmt::format(
      "{}", partition->get_ntp_config().iceberg_mode());
    state.raft_state = get_partition_raft_state(partition->raft());
    return state;
}

partition_raft_state get_partition_raft_state(consensus_ptr ptr) {
    partition_raft_state raft_state{};
    if (unlikely(!ptr)) {
        return raft_state;
    }
    raft_state.node = ptr->self().id();
    raft_state.term = ptr->term();
    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(*ptr->log());
    raft_state.offset_translator_state = fmt::format(
      "{}", *(disk_log.offset_translator().state()));
    raft_state.group_configuration = fmt::format("{}", ptr->config());
    raft_state.confirmed_term = ptr->confirmed_term();
    raft_state.flushed_offset = ptr->flushed_offset();
    raft_state.commit_index = ptr->committed_offset();
    raft_state.majority_replicated_index = ptr->majority_replicated_index();
    raft_state.visibility_upper_bound_index
      = ptr->visibility_upper_bound_index();
    raft_state.last_quorum_replicated_index
      = ptr->last_quorum_replicated_index();
    raft_state.last_snapshot_index = ptr->last_snapshot_index();
    raft_state.last_snapshot_term = ptr->last_snapshot_term();
    raft_state.received_snapshot_index = ptr->received_snapshot_index();
    raft_state.received_snapshot_bytes = ptr->received_snapshot_bytes();
    raft_state.has_pending_flushes = ptr->has_pending_flushes();
    raft_state.is_leader = ptr->is_leader();
    raft_state.is_elected_leader = ptr->is_elected_leader();
    raft_state.write_caching_enabled = ptr->write_caching_enabled();
    raft_state.flush_bytes = ptr->flush_bytes();
    raft_state.flush_ms = ptr->flush_ms();
    raft_state.time_since_last_flush = ptr->time_since_last_flush();
    raft_state.replication_monitor_state = fmt::format(
      "{}", ptr->get_replication_monitor());

    const auto& fstates = ptr->get_follower_states();
    if (ptr->is_elected_leader() && fstates.size() > 0) {
        using follower_state = partition_raft_state::follower_state;
        std::vector<follower_state> followers;
        followers.reserve(fstates.size());
        for (const auto& fstate : fstates) {
            const auto& md = fstate.second;
            follower_state state;
            state.node = md.node_id.id();
            state.last_dirty_log_index = md.last_dirty_log_index;
            state.last_flushed_log_index = md.last_flushed_log_index;
            state.match_index = md.match_index;
            state.next_index = md.next_index;
            state.expected_log_end_offset = md.expected_log_end_offset;
            state.heartbeats_failed = md.heartbeats_failed;
            state.is_learner = md.is_learner;
            state.is_recovering = md.is_recovering;
            state.ms_since_last_heartbeat
              = std::chrono::duration_cast<std::chrono::milliseconds>(
                  raft::clock_type::now() - md.last_received_reply_timestamp)
                  .count();
            state.last_sent_seq = md.last_sent_seq;
            state.last_received_seq = md.last_received_seq;
            state.last_successful_received_seq
              = md.last_successful_received_seq;
            state.suppress_heartbeats = md.has_inflight_appends();
            followers.push_back(std::move(state));
        }
        raft_state.followers = std::move(followers);
    }
    raft_state.stms = get_partition_stm_state(ptr);

    const auto& frs = ptr->get_follower_recovery_state();
    if (frs) {
        raft_state.recovery_state
          = partition_raft_state::follower_recovery_state{
            .is_active = frs->is_active(),
            .pending_offset_count = frs->pending_offset_count(),
        };
    }

    return raft_state;
}

std::vector<partition_stm_state> get_partition_stm_state(consensus_ptr ptr) {
    std::vector<partition_stm_state> result;
    if (unlikely(!ptr) || unlikely(!ptr->log()->stm_hookset())) {
        return result;
    }
    const auto& stms = ptr->log()->stm_hookset()->stms();
    result.reserve(stms.size());
    for (const auto& stm : stms) {
        partition_stm_state state;
        state.name = stm->name();
        state.last_applied_offset = stm->last_applied();
        state.max_removable_local_log_offset
          = stm->max_removable_local_log_offset();
        state.last_local_snapshot_offset
          = stm->last_locally_snapshotted_offset();
        result.push_back(std::move(state));
    }
    return result;
}

namespace {
const std::vector<ss::sstring>& stm_snapshot_names() {
    static const std::vector<ss::sstring> names{
      cluster::archival_stm_snapshot,
      cluster::tm_stm_snapshot,
      cluster::id_allocator_snapshot,
      cluster::rm_stm_snapshot,
      cluster::partition_properties_stm_snapshot,
      cluster::write_at_offset_stm_snapshot,
    };

    return names;
}

} // namespace

ss::future<> copy_persistent_stm_state(
  model::ntp ntp,
  storage::kvstore& source_kvs,
  ss::shard_id target_shard,
  ss::sharded<storage::api>& api) {
    return ss::parallel_for_each(
      stm_snapshot_names(),
      [ntp = std::move(ntp), &source_kvs, target_shard, &api](
        const ss::sstring& snapshot_name) {
          return raft::do_copy_persistent_stm_state(
            snapshot_name, ntp, source_kvs, target_shard, api);
      });
}

ss::future<>
remove_persistent_stm_state(model::ntp ntp, storage::kvstore& kvs) {
    return ss::parallel_for_each(
      stm_snapshot_names(),
      [ntp = std::move(ntp), &kvs](const ss::sstring& snapshot_name) {
          return raft::do_remove_persistent_stm_state(snapshot_name, ntp, kvs);
      });
}

ss::future<> copy_persistent_state(
  const model::ntp& ntp,
  raft::group_id group,
  storage::kvstore& source_kvs,
  ss::shard_id target_shard,
  ss::sharded<storage::api>& storage) {
    return ss::when_all_succeed(
             storage::disk_log_impl::copy_kvstore_state(
               ntp, source_kvs, target_shard, storage),
             raft::details::copy_persistent_state(
               group, source_kvs, target_shard, storage),
             storage::offset_translator::copy_persistent_state(
               group, source_kvs, target_shard, storage),
             copy_persistent_stm_state(ntp, source_kvs, target_shard, storage))
      .discard_result();
}

ss::future<> remove_persistent_state(
  const model::ntp& ntp, raft::group_id group, storage::kvstore& source_kvs) {
    return ss::when_all_succeed(
             storage::disk_log_impl::remove_kvstore_state(ntp, source_kvs),
             raft::details::remove_persistent_state(group, source_kvs),
             storage::offset_translator::remove_persistent_state(
               group, source_kvs),
             remove_persistent_stm_state(ntp, source_kvs))
      .discard_result();
}

} // namespace cluster
