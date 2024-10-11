/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/dl_stm/dl_stm.h"

#include "cloud_topics/dl_stm/command_builder.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/consensus.h"
#include "raft/persisted_stm.h"
#include "serde/rw/rw.h"
#include "serde/rw/uuid.h"
#include "storage/offset_translator_state.h"
#include "utils/uuid.h"

namespace experimental::cloud_topics {

dl_stm::dl_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>(name, logger, raft)
  , _state() {}

ss::future<> dl_stm::stop() {
    _as.request_abort();
    return _gate.close().then([this] { return raft::persisted_stm<>::stop(); });
}

model::offset dl_stm::max_collectible_offset() {
    // The LRO tracked by the in-memory state is a Kafka offset but
    // max collectible offset is a log offset. We need to translate
    // the offset first. It's safe to assume that the offset can be
    // translated because max_collectible offset can only be moved
    // forward by advancing LRO.
    auto ko = _state.get_last_reconciled();
    auto ot = _raft->log()->get_offset_translator_state();
    auto lo = ot->to_log_offset(kafka::offset_cast(ko));
    // TODO: take _last_persisted_offset into account
    return std::min(_state.get_insync_offset(), lo);
}

ss::future<bool>
dl_stm::replicate(model::term_id term, command_builder&& builder) {
    auto rb = std::move(builder).build();
    co_return co_await replicate(term, std::move(rb));
}

ss::future<bool>
dl_stm::replicate(model::term_id term, model::record_batch rb) {
    // TODO: synchronization
    auto opts = raft::replicate_options(raft::consistency_level::quorum_ack);
    opts.set_force_flush(); // needed for in-memory writes
    auto result = co_await _raft->replicate(
      term, model::make_memory_record_batch_reader(std::move(rb)), opts);
    if (result.has_error()) {
        vlog(
          _log.info,
          "Failed to replicate record batch, error: {}",
          result.error());
        co_return false;
    }
    co_return true;
}

ss::future<> dl_stm::do_apply(const model::record_batch& batch) {
    // Apply record batches in offset order.
    // Propagate offsets forward.
    vlog(_log.debug, "Applying record batch: {}", batch.header());
    if (
      batch.header().type
      != model::record_batch_type::version_fence /*TDOO: use dl_stm_command*/) {
        co_return;
    }
    batch.for_each_record([this](model::record&& r) {
        auto key = serde::from_iobuf<dl_stm_key>(r.release_key());
        switch (key) {
        case dl_stm_key::overlay: {
            vlog(_log.debug, "Decoding dl_overlay batch: {}", key);
            auto value = serde::from_iobuf<dl_overlay>(r.release_value());
            _state.register_overlay_cmd(value);
        } break;
        };
    });
    co_return;
}

std::optional<dl_overlay> dl_stm::lower_bound(kafka::offset o) const noexcept {
    return _state.lower_bound(o);
}

std::optional<kafka::offset>
dl_stm::get_term_last_offset(model::term_id t) const noexcept {
    return _state.get_term_last_offset(t);
}

kafka::offset dl_stm::get_last_reconciled() const noexcept {
    return _state.get_last_reconciled();
}

ss::future<> dl_stm::apply_raft_snapshot(const iobuf&) {
    co_return; // TODO: fixme
}

ss::future<> dl_stm::apply_local_snapshot(
  raft::stm_snapshot_header header, iobuf&& snapshot_data) {
    // The 'data' contains snapshot from the local file.
    auto state = serde::from_iobuf<dl_stm_state>(std::move(snapshot_data));
    //
    vlog(
      _log.info,
      "applying snapshot, snapshot offset: {}, insync offset: {}, LRO: {}",
      header.offset,
      state.get_insync_offset(),
      state.get_last_reconciled());

    // Install snapshot
    _state = std::move(state);
    co_return;
}

ss::future<raft::stm_snapshot>
dl_stm::take_local_snapshot([[maybe_unused]] ssx::semaphore_units u) {
    // Take local file snapshot
    auto snapshot_data = serde::to_iobuf(_state);
    vlog(
      _log.debug,
      "creating snapshot at offset: {}, insync offset: {}, LRO: {}",
      last_applied_offset(),
      _state.get_insync_offset(),
      get_last_reconciled());
    co_return raft::stm_snapshot::create(
      0, last_applied_offset(), std::move(snapshot_data));
}

ss::future<iobuf> dl_stm::take_snapshot(model::offset) {
    // Take 'raft' snapshot that will be consumed by another
    // replica through the 'apply_raft_snapshot' method.
    // The implementation disrespects the offset and the apply
    // method can apply any snapshot as long as its insync
    // offset is available locally.
    co_return iobuf{}; // TODO
}

bool dl_stm_factory::is_applicable_for(
  const storage::ntp_config& ntp_cfg) const {
    return ntp_cfg.ntp().ns == model::kafka_namespace
           && is_shadow_topic(ntp_cfg);
}

bool dl_stm_factory::is_shadow_topic(const storage::ntp_config& ntp_cfg) const {
    return ntp_cfg.cloud_topic_enabled();
}

void dl_stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto stm = builder.create_stm<cloud_topics::dl_stm>(cd_log, raft);
    raft->log()->stm_manager()->add_stm(stm);
}

} // namespace experimental::cloud_topics
