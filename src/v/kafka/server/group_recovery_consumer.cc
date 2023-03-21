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
#include "kafka/server/group_metadata.h"

#include <exception>

namespace kafka {

/*
 * This batch consumer is used during partition recovery to read, index, and
 * deduplicate both group and commit metadata snapshots.
 */
namespace {
template<typename T>
struct group_tx_cmd {
    model::producer_identity pid;
    T cmd;
};

template<typename T>
static group_tx_cmd<T>
parse_tx_batch(const model::record_batch& batch, int8_t version) {
    vassert(batch.record_count() == 1, "tx batch must contain a single record");
    auto r = batch.copy_records();
    auto& record = *r.begin();
    auto key_buf = record.release_key();
    auto val_buf = record.release_value();

    iobuf_parser val_reader(std::move(val_buf));
    auto tx_version = reflection::adl<int8_t>{}.from(val_reader);
    vassert(
      tx_version == version,
      "unknown group inflight tx record version: {} expected: {}",
      tx_version,
      version);
    auto cmd = reflection::adl<T>{}.from(val_reader);

    iobuf_parser key_reader(std::move(key_buf));
    auto batch_type = reflection::adl<model::record_batch_type>{}.from(
      key_reader);
    const auto& hdr = batch.header();
    vassert(
      hdr.type == batch_type,
      "broken tx group message. expected batch type {} got: {}",
      hdr.type,
      batch_type);
    auto p_id = model::producer_id(reflection::adl<int64_t>{}.from(key_reader));
    auto bid = model::batch_identity::from(hdr);
    vassert(
      p_id == bid.pid.id,
      "broken tx group message. expected pid/id {} got: {}",
      bid.pid.id,
      p_id);

    return group_tx_cmd<T>{.pid = bid.pid, .cmd = std::move(cmd)};
}
} // namespace

ss::future<ss::stop_iteration>
group_recovery_consumer::operator()(model::record_batch batch) {
    if (_as.abort_requested()) {
        co_return ss::stop_iteration::yes;
    }
    if (batch.header().type == model::record_batch_type::raft_data) {
        _batch_base_offset = batch.base_offset();
        co_await model::for_each_record(batch, [this](model::record& r) {
            return handle_record(std::move(r));
        });

        co_return ss::stop_iteration::no;
    } else if (
      batch.header().type == model::record_batch_type::group_prepare_tx) {
        auto val = parse_tx_batch<group_log_prepared_tx>(
                     batch, group::prepared_tx_record_version)
                     .cmd;

        auto [group_it, _] = _state.groups.try_emplace(val.group_id);
        group_it->second.update_prepared(batch.last_offset(), val);

        co_return ss::stop_iteration::no;
    } else if (
      batch.header().type == model::record_batch_type::group_commit_tx) {
        auto cmd = parse_tx_batch<group_log_commit_tx>(
          batch, group::commit_tx_record_version);

        auto [group_it, _] = _state.groups.try_emplace(cmd.cmd.group_id);
        group_it->second.commit(cmd.pid);

        co_return ss::stop_iteration::no;
    } else if (
      batch.header().type == model::record_batch_type::group_abort_tx) {
        auto cmd = parse_tx_batch<group_log_aborted_tx>(
          batch, group::aborted_tx_record_version);

        auto [group_it, _] = _state.groups.try_emplace(cmd.cmd.group_id);
        group_it->second.abort(cmd.pid, cmd.cmd.tx_seq);

        co_return ss::stop_iteration::no;
    } else if (batch.header().type == model::record_batch_type::tx_fence) {
        apply_tx_fence(std::move(batch));
        co_return ss::stop_iteration::no;
    } else if (batch.header().type == model::record_batch_type::version_fence) {
        auto fence = features::feature_table::decode_version_fence(
          std::move(batch));
        if (fence.active_version >= cluster::cluster_version{9}) {
            _state.has_offset_retention_feature_fence = true;
        }
        co_return ss::stop_iteration::no;
    } else {
        vlog(klog.trace, "ignoring batch with type {}", batch.header().type);
        co_return ss::stop_iteration::no;
    }
}

void group_recovery_consumer::apply_tx_fence(model::record_batch&& batch) {
    vassert(
      batch.record_count() == 1, "tx fence batch must contain a single record");
    auto r = batch.copy_records();
    auto& record = *r.begin();
    auto key_buf = record.release_key();
    auto val_buf = record.release_value();

    iobuf_parser key_reader(std::move(key_buf));
    auto batch_type = reflection::adl<model::record_batch_type>{}.from(
      key_reader);
    const auto& hdr = batch.header();
    vassert(
      hdr.type == batch_type,
      "broken tx group message. expected batch type {} got: {}",
      hdr.type,
      batch_type);
    auto p_id = model::producer_id(reflection::adl<int64_t>{}.from(key_reader));
    auto bid = model::batch_identity::from(hdr);
    vassert(
      p_id == bid.pid.id,
      "broken tx group message. expected pid/id {} got: {}",
      bid.pid.id,
      p_id);

    iobuf_parser val_reader(std::move(val_buf));
    auto fence_version = reflection::adl<int8_t>{}.from(val_reader);

    if (fence_version == group::fence_control_record_v0_version) {
        auto cmd = reflection::adl<group_log_fencing_v0>{}.from(val_reader);
        auto [group_it, _] = _state.groups.try_emplace(cmd.group_id);
        group_it->second.try_set_fence(bid.pid.get_id(), bid.pid.get_epoch());
    } else if (fence_version == group::fence_control_record_v1_version) {
        auto cmd = reflection::adl<group_log_fencing_v1>{}.from(val_reader);
        auto [group_it, _] = _state.groups.try_emplace(cmd.group_id);
        group_it->second.try_set_fence(
          bid.pid.get_id(),
          bid.pid.get_epoch(),
          cmd.tx_seq,
          cmd.transaction_timeout_ms,
          model::partition_id(0));
    } else if (fence_version == group::fence_control_record_version) {
        auto cmd = reflection::adl<group_log_fencing>{}.from(val_reader);
        auto [group_it, _] = _state.groups.try_emplace(cmd.group_id);
        group_it->second.try_set_fence(
          bid.pid.get_id(),
          bid.pid.get_epoch(),
          cmd.tx_seq,
          cmd.transaction_timeout_ms,
          cmd.tm_partition);
    } else {
        vassert(
          false,
          "unknown group fence record version: {} expected at most: {}",
          fence_version,
          group::fence_control_record_version);
    }
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
    vlog(klog.trace, "Recovering group metadata {}", md.key.group_id);

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
          "Recovering offset {}/{} with metadata {}",
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
        // tombstone
        auto group_it = _state.groups.find(md.key.group_id);
        if (group_it != _state.groups.end()) {
            group_it->second.remove_offset(tp);
        }
    }
}

} // namespace kafka
