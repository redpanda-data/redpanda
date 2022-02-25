/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * as of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/group_recovery_consumer.h"

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
        auto cmd = parse_tx_batch<group_log_fencing>(
          batch, group::fence_control_record_version);

        auto [group_it, _] = _state.groups.try_emplace(cmd.cmd.group_id);
        group_it->second.try_set_fence(cmd.pid.get_id(), cmd.pid.get_epoch());
        co_return ss::stop_iteration::no;
    } else {
        vlog(
          klog.trace, "ignorning batch with type {}", int(batch.header().type));
        co_return ss::stop_iteration::no;
    }
}

ss::future<> group_recovery_consumer::handle_record(model::record r) {
    auto key = reflection::adl<old::group_log_record_key>{}.from(r.share_key());
    auto value = r.has_value() ? r.release_value() : std::optional<iobuf>();

    switch (key.record_type) {
    case old::group_log_record_key::type::group_metadata:
        return handle_group_metadata(std::move(key.key), std::move(value));

    case old::group_log_record_key::type::offset_commit:
        return handle_offset_metadata(std::move(key.key), std::move(value));

    case old::group_log_record_key::type::noop:
        // skip control structure
        return ss::make_ready_future<>();

    default:
        return ss::make_exception_future<>(std::runtime_error(fmt::format(
          "Unknown record type={} in group recovery", int(key.record_type))));
    }
}

ss::future<> group_recovery_consumer::handle_group_metadata(
  iobuf key_buf, std::optional<iobuf> val_buf) {
    auto group_id = kafka::group_id(
      reflection::from_iobuf<kafka::group_id::type>(std::move(key_buf)));

    vlog(klog.trace, "Recovering group metadata {}", group_id);

    if (val_buf) {
        // until we switch over to a compacted topic or use raft snapshots,
        // always take the latest entry in the log.

        auto metadata = reflection::from_iobuf<old::group_log_group_metadata>(
          std::move(*val_buf));

        auto [group_it, _] = _state.groups.try_emplace(group_id, group_stm());
        group_it->second.overwrite_metadata(std::move(metadata));
    } else {
        // tombstone
        auto [group_it, _] = _state.groups.try_emplace(group_id, group_stm());
        group_it->second.remove();
    }

    co_return;
}

ss::future<> group_recovery_consumer::handle_offset_metadata(
  iobuf key_buf, std::optional<iobuf> val_buf) {
    auto key = reflection::from_iobuf<old::group_log_offset_key>(
      std::move(key_buf));
    model::topic_partition tp(key.topic, key.partition);
    if (val_buf) {
        auto metadata = reflection::from_iobuf<old::group_log_offset_metadata>(
          std::move(*val_buf));
        vlog(
          klog.trace, "Recovering offset {} with metadata {}", key, metadata);
        // until we switch over to a compacted topic or use raft snapshots,
        // always take the latest entry in the log.
        auto [group_it, _] = _state.groups.try_emplace(key.group, group_stm());
        group_it->second.update_offset(
          tp, _batch_base_offset, std::move(metadata));
    } else {
        // tombstone
        auto group_it = _state.groups.find(key.group);
        if (group_it != _state.groups.end()) {
            group_it->second.remove_offset(tp);
        }
    }

    co_return;
}

} // namespace kafka
