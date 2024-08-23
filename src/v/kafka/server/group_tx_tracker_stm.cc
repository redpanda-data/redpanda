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

namespace kafka {

group_tx_tracker_stm::group_tx_tracker_stm(
  ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>("group_tx_tracker_stm.snapshot", logger, raft)
  , group_data_parser<group_tx_tracker_stm>() {}

void group_tx_tracker_stm::maybe_add_tx_begin_offset(
  kafka::group_id group, model::producer_identity pid, model::offset offset) {
    auto [it, inserted] = _all_txs.try_emplace(group, pid, offset);
    if (!inserted) {
        // group already exists
        it->second.maybe_add_tx_begin(pid, offset);
    }
}

void group_tx_tracker_stm::maybe_end_tx(
  kafka::group_id group, model::producer_identity pid) {
    auto it = _all_txs.find(group);
    if (it == _all_txs.end()) {
        return;
    }
    auto& group_data = it->second;
    auto p_it = group_data.producer_to_begin.find(pid);
    if (p_it == group_data.producer_to_begin.end()) {
        return;
    }
    group_data.begin_offsets.erase(p_it->second);
    group_data.producer_to_begin.erase(p_it);
    if (group_data.producer_to_begin.empty()) {
        _all_txs.erase(group);
    }
}

ss::future<> group_tx_tracker_stm::do_apply(const model::record_batch& b) {
    auto holder = _gate.hold();
    co_await parse(b.copy());
}

model::offset group_tx_tracker_stm::max_collectible_offset() {
    auto result = last_applied_offset();
    for (const auto& [_, group_state] : _all_txs) {
        if (!group_state.begin_offsets.empty()) {
            result = std::min(
              result, model::prev_offset(*group_state.begin_offsets.begin()));
        }
    }
    return result;
}

ss::future<> group_tx_tracker_stm::apply_local_snapshot(
  raft::stm_snapshot_header, iobuf&& snap_buf) {
    auto holder = _gate.hold();
    iobuf_parser parser(std::move(snap_buf));
    auto snap = co_await serde::read_async<snapshot>(parser);
    _all_txs = std::move(snap.transactions);
}

ss::future<raft::stm_snapshot>
group_tx_tracker_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    auto holder = _gate.hold();
    // Copy over the snapshot state for a consistent view.
    auto offset = last_applied_offset();
    snapshot snap;
    snap.transactions = _all_txs;
    iobuf snap_buf;
    apply_units.return_all();
    co_await serde::write_async(snap_buf, snap);
    // snapshot versioning handled via serde.
    co_return raft::stm_snapshot::create(0, offset, std::move(snap_buf));
}

ss::future<> group_tx_tracker_stm::apply_raft_snapshot(const iobuf&) {
    // Transaction commit/abort ensures the data structures are cleaned
    // up and bounded in size and all the open transactions are only
    // in the non evicted part of the log, so nothing to do.
    return ss::now();
}

ss::future<iobuf> group_tx_tracker_stm::take_snapshot(model::offset) {
    return ss::make_ready_future<iobuf>(iobuf());
}

ss::future<> group_tx_tracker_stm::handle_raft_data(model::record_batch) {
    // No transactional data, ignore
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_tx_offsets(
  model::record_batch_header header, kafka::group_tx::offsets_metadata data) {
    // in case the fence got truncated, try to start the transaction from
    // this point on. This is not possible today but may help if delete
    // retention is implemented for consumer topics.
    maybe_add_tx_begin_offset(
      std::move(data.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.base_offset);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_fence_v0(
  model::record_batch_header header, kafka::group_tx::fence_metadata_v0 fence) {
    maybe_add_tx_begin_offset(
      std::move(fence.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.base_offset);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_fence_v1(
  model::record_batch_header header, kafka::group_tx::fence_metadata_v1 fence) {
    maybe_add_tx_begin_offset(
      std::move(fence.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.base_offset);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_fence(
  model::record_batch_header header, kafka::group_tx::fence_metadata fence) {
    maybe_add_tx_begin_offset(
      std::move(fence.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch},
      header.base_offset);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_abort(
  model::record_batch_header header, kafka::group_tx::abort_metadata data) {
    maybe_end_tx(
      std::move(data.group_id),
      model::producer_identity{header.producer_id, header.producer_epoch});
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_commit(
  model::record_batch_header header, kafka::group_tx::commit_metadata data) {
    auto pid = model::producer_identity{
      header.producer_id, header.producer_epoch};
    maybe_end_tx(std::move(data.group_id), pid);
    return ss::now();
}

ss::future<> group_tx_tracker_stm::handle_version_fence(
  features::feature_table::version_fence) {
    // ignore
    return ss::now();
}

bool group_tx_tracker_stm_factory::is_applicable_for(
  const storage::ntp_config& config) const {
    const auto& ntp = config.ntp();
    return ntp.ns == model::kafka_consumer_offsets_nt.ns
           && ntp.tp.topic == model::kafka_consumer_offsets_nt.tp;
}
void group_tx_tracker_stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto stm = builder.create_stm<kafka::group_tx_tracker_stm>(klog, raft);
    raft->log()->stm_manager()->add_stm(stm);
}

void group_tx_tracker_stm::per_group_state::maybe_add_tx_begin(
  model::producer_identity pid, model::offset offset) {
    auto it = producer_to_begin.find(pid);
    if (it == producer_to_begin.end()) {
        begin_offsets.emplace(offset);
        producer_to_begin[pid] = offset;
    }
}
} // namespace kafka
