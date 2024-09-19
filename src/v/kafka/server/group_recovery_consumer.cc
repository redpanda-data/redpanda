/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * as of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/group_recovery_consumer.h"

#include "kafka/protocol/wire.h"
#include "kafka/server/group.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/logger.h"

#include <exception>

namespace kafka {

ss::future<>
group_recovery_consumer::handle_raft_data(model::record_batch batch) {
    _batch_base_offset = batch.base_offset();
    co_await model::for_each_record(
      batch, [this](model::record& r) { return handle_record(std::move(r)); });
}

ss::future<> group_recovery_consumer::handle_tx_offsets(
  model::record_batch_header hdr, kafka::group_tx::offsets_metadata data) {
    vlog(
      klog.trace,
      "[group: {}] recovered update tx offsets: {}",
      data.group_id,
      data);
    auto [group_it, _] = _state.groups.try_emplace(data.group_id);
    group_it->second.update_tx_offset(hdr.last_offset(), data);
    co_return;
}

ss::future<> group_recovery_consumer::handle_fence_v0(
  model::record_batch_header header, kafka::group_tx::fence_metadata_v0 data) {
    auto pid = model::producer_identity{
      header.producer_id, header.producer_epoch};
    vlog(
      klog.trace,
      "[group: {}] recovered tx fence version: {} for producer: {}",
      data.group_id,
      group::fence_control_record_v0_version,
      pid);
    auto [group_it, _] = _state.groups.try_emplace(data.group_id);
    group_it->second.try_set_fence(pid.get_id(), pid.get_epoch());
    co_return;
}

ss::future<> group_recovery_consumer::handle_fence_v1(
  model::record_batch_header header, kafka::group_tx::fence_metadata_v1 data) {
    auto pid = model::producer_identity{
      header.producer_id, header.producer_epoch};
    vlog(
      klog.trace,
      "[group: {}] recovered tx fence version: {} for producer: {} - {}",
      data.group_id,
      group::fence_control_record_v1_version,
      pid,
      data);
    auto [group_it, _] = _state.groups.try_emplace(data.group_id);
    group_it->second.try_set_fence(
      pid.get_id(),
      pid.get_epoch(),
      data.tx_seq,
      data.transaction_timeout_ms,
      model::partition_id(0));
    co_return;
}

ss::future<> group_recovery_consumer::handle_fence(
  model::record_batch_header header, kafka::group_tx::fence_metadata data) {
    auto pid = model::producer_identity{
      header.producer_id, header.producer_epoch};
    vlog(
      klog.trace,
      "[group: {}] recovered tx fence version: {} for producer: {} - {}",
      data.group_id,
      group::fence_control_record_version,
      pid,
      data);
    auto [group_it, _] = _state.groups.try_emplace(data.group_id);
    group_it->second.try_set_fence(
      pid.get_id(),
      pid.get_epoch(),
      data.tx_seq,
      data.transaction_timeout_ms,
      data.tm_partition);
    co_return;
}

ss::future<> group_recovery_consumer::handle_abort(
  model::record_batch_header header, kafka::group_tx::abort_metadata data) {
    vlog(
      klog.trace,
      "[group: {}] recovered abort tx_seq: {}",
      data.group_id,
      data.tx_seq);
    auto pid = model::producer_identity{
      header.producer_id, header.producer_epoch};
    auto [group_it, _] = _state.groups.try_emplace(data.group_id);
    group_it->second.abort(pid, data.tx_seq);
    co_return;
}

ss::future<> group_recovery_consumer::handle_commit(
  model::record_batch_header header, kafka::group_tx::commit_metadata data) {
    vlog(klog.trace, "[group: {}] recovered commit tx", data.group_id);
    auto pid = model::producer_identity{
      header.producer_id, header.producer_epoch};
    auto [group_it, _] = _state.groups.try_emplace(data.group_id);
    group_it->second.commit(pid);
    co_return;
}

ss::future<> group_recovery_consumer::handle_version_fence(
  features::feature_table::version_fence fence) {
    vlog(klog.trace, "recovered version fence");
    if (
      fence.active_version
      >= to_cluster_version(features::release_version::v23_1_1)) {
        _state.has_offset_retention_feature_fence = true;
    }
    co_return;
}

ss::future<ss::stop_iteration>
group_recovery_consumer::operator()(model::record_batch batch) {
    if (_as.abort_requested()) {
        co_return ss::stop_iteration::yes;
    }
    _state.last_read_offset = batch.last_offset();
    co_await base_t::parse(std::move(batch));
    co_return ss::stop_iteration::no;
}

void group_recovery_consumer::handle_record(model::record r) {
    try {
        auto record_type = _serializer.get_metadata_type(r.key().copy());
        switch (record_type) {
        case offset_commit:
            handle_offset_metadata(
              _serializer.decode_offset_metadata(std::move(r)));
            return;
        case group_metadata:
            handle_group_metadata(
              _serializer.decode_group_metadata(std::move(r)));
            return;
        case noop:
            // ignore noops, they are handled for backward compatibility
            return;
        }
        __builtin_unreachable();
    } catch (...) {
        vlog(
          klog.error,
          "error handling group metadata record - {}",
          std::current_exception());
    }
}

void group_recovery_consumer::handle_group_metadata(group_metadata_kv md) {
    vlog(klog.trace, "[group: {}] recovered group metadata", md.key.group_id);

    if (md.value) {
        // until we switch over to a compacted topic or use raft snapshots,
        // always take the latest entry in the log.

        auto [group_it, _] = _state.groups.try_emplace(
          md.key.group_id, group_stm());
        group_it->second.overwrite_metadata(std::move(*md.value));
    } else {
        // tombstone
        _state.groups.erase(md.key.group_id);
    }
}

void group_recovery_consumer::handle_offset_metadata(offset_metadata_kv md) {
    model::topic_partition tp(md.key.topic, md.key.partition);
    if (md.value) {
        vlog(
          klog.trace,
          "[group: {}] recovered {}/{} committed offset: {}",
          md.key.group_id,
          md.key.topic,
          md.key.partition,
          *md.value);
        // until we switch over to a compacted topic or use raft snapshots,
        // always take the latest entry in the log.
        auto [group_it, _] = _state.groups.try_emplace(
          md.key.group_id, group_stm());
        if (_state.has_offset_retention_feature_fence) {
            md.value->non_reclaimable = false;
        }
        group_it->second.update_offset(
          tp, _batch_base_offset, std::move(*md.value));
    } else {
        vlog(
          klog.trace,
          "[group: {}] recovered {}/{} committed offset tombstone",
          md.key.group_id,
          md.key.topic,
          md.key.partition);
        // tombstone
        auto group_it = _state.groups.find(md.key.group_id);
        if (group_it != _state.groups.end()) {
            group_it->second.remove_offset(tp);
        }
    }
}

} // namespace kafka
