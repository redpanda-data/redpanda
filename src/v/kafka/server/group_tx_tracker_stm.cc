/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/group_tx_tracker_stm.h"

#include "kafka/server/group.h"
#include "serde/rw/envelope.h"
#include "ssx/future-util.h"

#include <ranges>

namespace kafka {

group_tx_tracker_stm::group_tx_tracker_stm(
  ss::logger& logger,
  raft::consensus* raft,
  ss::sharded<features::feature_table>& feature_table)
  : raft::persisted_stm<>("group_tx_tracker_stm.snapshot", logger, raft)
  , group_data_parser<group_tx_tracker_stm>()
  , _feature_table(feature_table) {
    _stale_tx_fence_gc_timer.set_callback([this] {
        ssx::spawn_with_gate(
          _gate, [this] { return gc_expired_tx_fence_transactions(); });
    });
    _stale_tx_fence_gc_timer.arm_periodic(tx_fence_gc_frequency);
}

ss::future<> group_tx_tracker_stm::gc_expired_tx_fence_transactions() {
    auto it = _all_txs.begin();
    while (it != _all_txs.end()) {
        it->second.gc_expired_tx_fence_transactions();
        ++it;
        if (ss::need_preempt() && it != _all_txs.end()) {
            auto key_checkpoint = it->first;
            co_await ss::yield();
            it = _all_txs.lower_bound(key_checkpoint);
        }
    }
}

ss::future<> group_tx_tracker_stm::stop() {
    _stale_tx_fence_gc_timer.cancel();
    _as.request_abort();
    return raft::persisted_stm<>::stop();
}

void group_tx_tracker_stm::maybe_add_tx_begin_offset(
  model::record_batch_type fence_type,
  kafka::group_id group,
  model::producer_identity pid,
  model::offset offset,
  model::timestamp ts,
  model::timeout_clock::duration tx_timeout) {
    auto it = _all_txs.find(group);
    if (it == _all_txs.end()) {
        vlog(
          cg_klog.debug,
          "[{}] group not found, ignoring fence of type: {} at offset: {}",
          group,
          fence_type,
          offset);
        return;
    }
    it->second.maybe_add_tx_begin(
      group, fence_type, pid, offset, ts, tx_timeout);
}

void group_tx_tracker_stm::maybe_end_tx(
  kafka::group_id group, model::producer_identity pid, model::offset offset) {
    auto it = _all_txs.find(group);
    if (it == _all_txs.end()) {
        vlog(
          cg_klog.debug,
          "[{}] group not found, ignoring end transaction for pid: {} at "
          "offset: {}",
          group,
          pid,
          offset);
        return;
    }
    auto& group_data = it->second;
    auto p_it = group_data.producer_states.find(pid);
    if (p_it == group_data.producer_states.end()) {
        vlog(
          cg_klog.debug,
          "[{}] ignoring end transaction, no in progress transaction for pid: "
          "{} at offset: {}",
          group,
          pid,
          offset);
        return;
    }
    group_data.begin_offsets.erase(p_it->second.begin_offset);
    group_data.producer_states.erase(p_it);
    group_data.producer_to_begin_deprecated.erase(pid);
}

ss::future<> group_tx_tracker_stm::do_apply(const model::record_batch& b) {
    // fast path, check without a scheduling point.
    if (
      unlikely(!_feature_table.local().is_active(
        features::feature::group_tx_fence_dedicated_batch_type))) {
        // This is only relevant for upgrades from 24.1.x to 24.2.x where a
        // mixed mode cluster has this feature disabled until the upgrade is
        // done. Holding off stm updates ensures that max_removable offset
        // does not progress for the duration of the upgrade, which is ok since
        // group compaction was not considering any control batches in 24.1.x,
        // so nothing was being compacted.
        co_await _feature_table.local().await_feature(
          features::feature::group_tx_fence_dedicated_batch_type, _as);
    }
    co_await parse(b.copy());
}

model::offset group_tx_tracker_stm::max_removable_local_log_offset() {
    auto result = last_applied_offset();
    for (const auto& [_, group_state] : _all_txs) {
        if (!group_state.begin_offsets.empty()) {
            result = std::min(
              result, model::prev_offset(*group_state.begin_offsets.begin()));
        }
    }
    return result;
}

ss::future<raft::local_snapshot_applied>
group_tx_tracker_stm::apply_local_snapshot(
  raft::stm_snapshot_header header, iobuf&& snap_buf) {
    if (header.version != supported_local_snapshot_version) {
        // fall back to applying from the log
        co_return raft::local_snapshot_applied::no;
    }
    iobuf_parser parser(std::move(snap_buf));
    auto snap = co_await serde::read_async<snapshot>(parser);
    _all_txs = std::move(snap.transactions);
    if (!snap.blocked_groups.empty() && snap.group_blocks.empty())
      [[unlikely]] {
        // legacy snapshot from version 1, reconstruct the map
        _group_blocks = chunked_hash_map_from_range(
          snap.blocked_groups
          | std::views::transform([](const kafka::group_id& gid) {
                return std::make_pair(
                  gid,
                  group_block_info{
                    .is_blocked = true, .revision_id = model::revision_id{}});
            })
          | std::views::as_rvalue);
    } else {
        _group_blocks = chunked_hash_map_from_range(
          snap.group_blocks | std::views::as_rvalue);
    }
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
group_tx_tracker_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    // Snapshot at max_removable_local_log_offset with open transaction state
    // stripped. This prevents a bug where:
    //
    // 1. Snapshot captures an open tx (begin_offset in per_group_state)
    // 2. The tx later commits, max_removable advances
    // 3. Compaction removes the commit batch from the log
    // 4. On restart, the stale open tx is loaded but its commit is gone
    //    -> max_removable stuck permanently
    //
    // By definition, all open txs have begin_offset > max_removable, so they
    // are all past the snapshot offset. On replay from offset+1, their fence
    // batches are guaranteed to still be in the log (compaction is bounded by
    // max_removable while the STM is live) and will re-establish the open tx
    // state. Group existence (keys in _all_txs) must be preserved because
    // maybe_add_tx_begin_offset() ignores fences for unknown groups, and the
    // group_metadata batches that created them may be before the snapshot
    // offset.
    auto offset = max_removable_local_log_offset();

    all_txs_t snap_txs;
    for (const auto& [gid, _] : _all_txs) {
        snap_txs[gid] = per_group_state{};
    }

    snapshot snap{
      .transactions{std::move(snap_txs)},
      .blocked_groups{
        std::from_range, _group_blocks | std::views::filter([](const auto& e) {
                             return e.second.is_blocked;
                         }) | std::views::keys},
      .group_blocks{std::from_range, _group_blocks},
    };
    apply_units.return_all();
    iobuf snap_buf;
    co_await serde::write_async(snap_buf, std::move(snap));
    co_return raft::stm_snapshot::create(
      supported_local_snapshot_version, offset, std::move(snap_buf));
}

ss::future<> group_tx_tracker_stm::apply_raft_snapshot(const iobuf&) {
    // Transaction commit/abort ensures the data structures are cleaned
    // up and bounded in size and all the open transactions are only
    // in the non evicted part of the log, so nothing to do.
    return ss::now();
}

ss::future<iobuf> group_tx_tracker_stm::take_raft_snapshot(model::offset) {
    return ss::make_ready_future<iobuf>(iobuf());
}

ss::future<> group_tx_tracker_stm::handle_raft_data(model::record_batch batch) {
    co_await model::for_each_record(batch, [this](model::record& r) {
        auto record_type = group_metadata_serializer::get_metadata_type(
          r.key().copy());
        switch (record_type) {
        case offset_commit:
        case noop:
            return;
        case group_metadata:
            handle_group_metadata(
              group_metadata_serializer::decode_group_metadata(std::move(r)));
            return;
        }
        __builtin_unreachable();
    });
}

void group_tx_tracker_stm::handle_group_metadata(group_metadata_kv md) {
    if (is_group_blocked_verbose(md.key.group_id, "group metadata")) {
        return;
    }
    if (md.value) {
        vlog(cg_klog.trace, "[group: {}] update", md.key.group_id);
        // A group may checkpoint periodically as the member's state changes,
        // here we retain the group state if the group already exists.
        _all_txs.try_emplace(md.key.group_id, per_group_state{});
    } else {
        vlog(cg_klog.trace, "[group: {}] tombstone", md.key.group_id);
        // A tombstone indicates all the group state can be purged and
        // any transactions can be ignored. Although care must be taken
        // to ensure there are no open transactions before tombstoning
        // a group in the main state machine.
        _all_txs.erase(md.key.group_id);
    }
}

ss::future<> group_tx_tracker_stm::handle_tx_offsets(
  model::record_batch_header, kafka::group_tx::offsets_metadata) {
    // Transaction boundaries are determined by fence/commit or abort
    // batches
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_fence_v0(
  model::record_batch_header header, kafka::group_tx::fence_metadata_v0 fence) {
    // fence_v0 has no timeout, use a max permissible timeout.
    // fence_v0 has been deprecated for long, this is not a problem in practice
    auto timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
      config::shard_local_cfg().transaction_max_timeout_ms());
    maybe_add_tx_begin_offset(
      header.type,
      std::move(fence.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.base_offset,
      header.max_timestamp,
      timeout);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_fence_v1(
  model::record_batch_header header, kafka::group_tx::fence_metadata_v1 fence) {
    maybe_add_tx_begin_offset(
      header.type,
      std::move(fence.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.base_offset,
      header.max_timestamp,
      fence.transaction_timeout_ms);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_fence(
  model::record_batch_header header, kafka::group_tx::fence_metadata fence) {
    maybe_add_tx_begin_offset(
      header.type,
      std::move(fence.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.base_offset,
      header.max_timestamp,
      fence.transaction_timeout_ms);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_abort(
  model::record_batch_header header, kafka::group_tx::abort_metadata data) {
    maybe_end_tx(
      std::move(data.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.base_offset);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_commit(
  model::record_batch_header header, kafka::group_tx::commit_metadata data) {
    auto pid = model::producer_identity{
      header.producer_id, header.producer_epoch};
    maybe_end_tx(std::move(data.group_id), pid, header.base_offset);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_version_fence(
  features::feature_table::version_fence) {
    // ignore
    return ss::now();
}

void group_tx_tracker_stm::handle_group_block(kafka::group_block gb) {
    base_t::do_handle_group_block(gb);
    if (gb.info.is_blocked) {
        // there shouldn't be any transactions in progress, doing just in case
        _all_txs.erase(gb.group_id);
    }
}

group_block_info_map& group_tx_tracker_stm::group_blocks() {
    return _group_blocks;
}

const group_block_info_map& group_tx_tracker_stm::group_blocks() const {
    return _group_blocks;
}

bool group_tx_tracker_stm_factory::is_applicable_for(
  const storage::ntp_config& config) const {
    const auto& ntp = config.ntp();
    return ntp.ns == model::kafka_consumer_offsets_nt.ns
           && ntp.tp.topic == model::kafka_consumer_offsets_nt.tp;
}

group_tx_tracker_stm_factory::group_tx_tracker_stm_factory(
  ss::sharded<features::feature_table>& feature_table)
  : _feature_table(feature_table) {}

void group_tx_tracker_stm_factory::create(
  raft::state_machine_manager_builder& builder,
  raft::consensus* raft,
  const cluster::stm_instance_config&) {
    auto stm = builder.create_stm<kafka::group_tx_tracker_stm>(
      cg_klog, raft, _feature_table);
    raft->log()->stm_hookset()->add_stm(stm);
}

void group_tx_tracker_stm::per_group_state::maybe_add_tx_begin(
  const kafka::group_id& group,
  model::record_batch_type fence_type,
  model::producer_identity pid,
  model::offset offset,
  model::timestamp begin_ts,
  model::timeout_clock::duration tx_timeout) {
    auto it = producer_states.find(pid);
    if (it == producer_states.end()) {
        auto p_state = producer_tx_state{
          .fence_type = fence_type,
          .begin_offset = offset,
          .batch_ts = begin_ts,
          .timeout = tx_timeout};
        if (p_state.expired_deprecated_fence_tx()) {
            vlog(
              cg_klog.debug,
              "[{}] Ignoring stale tx_fence batch at offset: {}, considering "
              "it expired",
              group,
              offset);
            return;
        }
        vlog(
          cg_klog.debug,
          "[{}] Adding begin tx : {}, pid: {} at offset: {}, ts: {} with "
          "timeout: {}",
          group,
          fence_type,
          pid,
          offset,
          begin_ts,
          tx_timeout);
        begin_offsets.emplace(offset);
        producer_states[pid] = p_state;
        producer_to_begin_deprecated[pid] = offset;
    }
}

void group_tx_tracker_stm::per_group_state::gc_expired_tx_fence_transactions() {
    auto it = producer_states.begin();
    while (it != producer_states.end()) {
        if (it->second.expired_deprecated_fence_tx()) {
            vlog(
              cg_klog.warn,
              "Expiring stale tx_fence based begin tx at offset: {} for "
              "producer: {}",
              it->second.begin_offset,
              it->first);
            begin_offsets.erase(it->second.begin_offset);
            it = producer_states.erase(it);
            continue;
        }
        ++it;
    }
}

bool group_tx_tracker_stm::producer_tx_state::expired_deprecated_fence_tx()
  const {
    // A bug in 24.2.0 resulted in a situation where tx_fence
    // batches were retained _after_ compaction while their corresponding
    // data/commit/abort batches were compacted away. This applied to
    // only group transactions that used tx_fence to begin the
    // transaction.
    // After this buggy compaction, these uncleaned tx_fence batches are
    // accounted as open transactions when computing
    // max_removable_local_log_offset thus blocking further compaction after
    // upgrade to 24.2.x.
    if (fence_type != model::record_batch_type::tx_fence) {
        return false;
    }
    // note: this is a heuristic to ignore any transactions that have long been
    // expired and we do not want them to block max removable offset.
    // clamp the timeout, incase timeout is unset
    auto max_timeout
      = std::chrono::duration_cast<model::timeout_clock::duration>(
        2 * config::shard_local_cfg().transaction_max_timeout_ms());
    auto clamped_timeout = std::min(max_timeout, 2 * timeout);
    return model::timestamp_clock::now()
           > model::to_time_point(batch_ts) + clamped_timeout;
}

} // namespace kafka
