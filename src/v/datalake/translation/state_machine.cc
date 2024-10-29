/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/state_machine.h"

#include "datalake/logger.h"
#include "datalake/translation/types.h"

namespace {
raft::replicate_options make_replicate_options() {
    auto opts = raft::replicate_options(raft::consistency_level::quorum_ack);
    opts.set_force_flush();
    return opts;
}

model::record_batch_reader make_translation_state_batch(kafka::offset offset) {
    auto val = datalake::translation::translation_state{
      .highest_translated_offset = offset};
    storage::record_batch_builder builder(
      model::record_batch_type::datalake_translation_state, model::offset(0));
    builder.add_raw_kv(std::nullopt, serde::to_iobuf(val));
    auto batch = std::move(builder).build();
    return model::make_memory_record_batch_reader(std::move(batch));
}

} // namespace

namespace datalake::translation {

translation_stm::translation_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>("datalake_translation_stm.snapshot", logger, raft) {}

ss::future<> translation_stm::do_apply(const model::record_batch& batch) {
    if (
      batch.header().type
      != model::record_batch_type::datalake_translation_state) {
        co_return;
    }
    auto record_iterator = model::record_batch_iterator::create(batch);
    while (record_iterator.has_next()) {
        auto record = record_iterator.next();
        auto value = serde::from_iobuf<translation_state>(
          record.release_value());
        vlog(
          _log.trace,
          "updating highest translated offset to {}",
          value.highest_translated_offset);
        _highest_translated_offset = std::max(
          _highest_translated_offset, value.highest_translated_offset);
    }
}

ss::future<std::optional<kafka::offset>>
translation_stm::highest_translated_offset(
  model::timeout_clock::duration timeout) {
    if (!_raft->log_config().iceberg_enabled() || !co_await sync(timeout)) {
        co_return std::nullopt;
    }
    co_return _highest_translated_offset;
}

ss::future<std::error_code> translation_stm::reset_highest_translated_offset(
  kafka::offset new_translated_offset,
  model::term_id term,
  model::timeout_clock::duration timeout,
  ss::abort_source& as) {
    if (!co_await sync(timeout) || _insync_term != term) {
        co_return raft::errc::not_leader;
    }
    vlog(
      _log.debug,
      "Reset-ing highest translated offset to {} from {} in term: {}",
      new_translated_offset,
      _highest_translated_offset,
      term);
    auto current_term = _insync_term;
    // We are at a newer or equal term than the entry, so likely the
    // stm has gotten out of sync
    if (_highest_translated_offset >= new_translated_offset) {
        co_return raft::errc::success;
    }
    auto result = co_await _raft->replicate(
      current_term,
      make_translation_state_batch(new_translated_offset),
      make_replicate_options());
    auto deadline = model::timeout_clock::now() + timeout;
    if (
      result
      && co_await wait_no_throw(
        result.value().last_offset, deadline, std::ref(as))) {
        co_return raft::errc::success;
    }
    if (!as.abort_requested() && _raft->term() == current_term) {
        co_await _raft->step_down("datalake coordinator sync error");
    }
    auto error = result.has_error() ? result.error() : raft::errc::timeout;
    vlog(
      _log.warn,
      "error {} reseting highest translated offset to {}",
      error,
      new_translated_offset);
    co_return error;
}

model::offset translation_stm::max_collectible_offset() {
    if (!_raft->log_config().iceberg_enabled()) {
        return model::offset::max();
    }
    // if offset is not initialized, do not attempt translation.
    if (_highest_translated_offset == kafka::offset{}) {
        return model::offset{};
    }
    return _raft->log()->to_log_offset(
      kafka::offset_cast(_highest_translated_offset));
}

ss::future<> translation_stm::apply_local_snapshot(
  raft::stm_snapshot_header, iobuf&& bytes) {
    _highest_translated_offset
      = serde::from_iobuf<snapshot>(std::move(bytes)).highest_translated_offset;
    co_return;
}

ss::future<raft::stm_snapshot>
translation_stm::take_local_snapshot(ssx::semaphore_units apply_units) {
    auto snapshot_offset = last_applied_offset();
    snapshot snap{.highest_translated_offset = _highest_translated_offset};
    apply_units.return_all();
    iobuf result;
    co_await serde::write_async(result, snap);
    co_return raft::stm_snapshot::create(0, snapshot_offset, std::move(result));
}

ss::future<> translation_stm::apply_raft_snapshot(const iobuf&) { co_return; }

ss::future<iobuf> translation_stm::take_snapshot(model::offset) {
    co_return iobuf{};
}

bool stm_factory::is_applicable_for(const storage::ntp_config& config) const {
    return model::is_user_topic(config.ntp());
}

void stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto stm = builder.create_stm<translation_stm>(datalake_log, raft);
    raft->log()->stm_manager()->add_stm(stm);
}

} // namespace datalake::translation
