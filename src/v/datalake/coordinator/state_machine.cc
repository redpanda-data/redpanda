/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/coordinator/state_machine.h"

#include "datalake/coordinator/state.h"
#include "datalake/coordinator/state_update.h"
#include "datalake/logger.h"
#include "model/record_batch_types.h"

namespace datalake::coordinator {

namespace {
void maybe_log_update_error(
  update_key key,
  model::offset o,
  const checked<std::nullopt_t, stm_update_error>& r) {
    if (r.has_value()) {
        return;
    }
    // NOTE: inability to update the STM is not necessarily a bug! It indicates
    // that this update's construction raced with another update that broke an
    // invariant required to apply update. Expectation is that this update's
    // caller constructs a new update and tries again if needed.
    vlog(
      datalake_log.debug,
      "Coordinator STM {} update at offset {} didn't apply: {}",
      key,
      o,
      r.error());
}
} // namespace

coordinator_stm::coordinator_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>("datalake_coordinator_stm.snapshot", logger, raft) {}

ss::future<> coordinator_stm::do_apply(const model::record_batch& b) {
    if (b.header().type != model::record_batch_type::datalake_coordinator) {
        return ss::now();
    }
    auto iter = model::record_batch_iterator::create(b);
    while (iter.has_next()) {
        auto r = iter.next();
        auto key = serde::from_iobuf<update_key>(r.release_key());
        auto offset = b.base_offset() + model::offset_delta{r.offset_delta()};
        switch (key) {
        case update_key::add_files: {
            auto update = serde::from_iobuf<add_files_update>(
              r.release_value());
            auto res = update.apply(state_);
            maybe_log_update_error(key, offset, res);
            continue;
        }
        case update_key::mark_files_committed: {
            auto update = serde::from_iobuf<mark_files_committed_update>(
              r.release_value());
            auto res = update.apply(state_);
            maybe_log_update_error(key, offset, res);
            continue;
        }
        }
        vlog(
          datalake_log.error,
          "Unknown datalake coordinator STM record type: {}",
          key);
    }
    return ss::now();
}

model::offset coordinator_stm::max_collectible_offset() { return {}; }

ss::future<>
coordinator_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) {
    co_return;
}

ss::future<raft::stm_snapshot>
coordinator_stm::take_local_snapshot(ssx::semaphore_units) {
    return ss::make_exception_future<raft::stm_snapshot>(
      std::runtime_error{"not implemented exception"});
}

ss::future<> coordinator_stm::apply_raft_snapshot(const iobuf&) { co_return; }

ss::future<iobuf> coordinator_stm::take_snapshot(model::offset) {
    co_return iobuf{};
}

ss::future<add_translated_data_files_reply>
coordinator_stm::add_translated_data_file(add_translated_data_files_request) {
    co_return add_translated_data_files_reply{coordinator_errc::ok};
}

ss::future<fetch_latest_data_file_reply>
coordinator_stm::fetch_latest_data_file(fetch_latest_data_file_request) {
    co_return fetch_latest_data_file_reply{coordinator_errc::ok};
}

bool stm_factory::is_applicable_for(const storage::ntp_config& config) const {
    const auto& ntp = config.ntp();
    return (ntp.ns == model::datalake_coordinator_nt.ns)
           && (ntp.tp.topic == model::datalake_coordinator_topic);
}

void stm_factory::create(
  raft::state_machine_manager_builder& builder, raft::consensus* raft) {
    auto stm = builder.create_stm<coordinator_stm>(datalake_log, raft);
    raft->log()->stm_manager()->add_stm(stm);
}

} // namespace datalake::coordinator
