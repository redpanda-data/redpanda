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
#include "serde/async.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

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

ss::future<checked<model::term_id, coordinator_stm::errc>>
coordinator_stm::sync(model::timeout_clock::duration timeout) {
    auto sync_res = co_await ss::coroutine::as_future(
      raft::persisted_stm<>::sync(timeout));
    if (sync_res.failed()) {
        auto eptr = sync_res.get_exception();
        auto msg = fmt::format("Exception caught while syncing: {}", eptr);
        if (ssx::is_shutdown_exception(eptr)) {
            vlog(datalake_log.debug, "Ignoring shutdown error: {}", msg);
            co_return errc::shutting_down;
        }
        vlog(datalake_log.warn, "{}", msg);
        co_return errc::raft_error;
    }

    if (!sync_res.get()) {
        co_return errc::not_leader;
    }
    // At this point we're guaranteed that this node is the leader and that the
    // STM has been caught up in the current term before (this doesn't mean the
    // STM is currently caught up right now though!)
    co_return _insync_term;
}

ss::future<checked<std::nullopt_t, coordinator_stm::errc>>
coordinator_stm::replicate_and_wait(
  model::term_id term, model::record_batch batch, ss::abort_source& as) {
    auto opts = raft::replicate_options{raft::consistency_level::quorum_ack};
    opts.set_force_flush();
    auto res = co_await _raft->replicate(
      term, model::make_memory_record_batch_reader(std::move(batch)), opts);
    if (res.has_error()) {
        co_return errc::raft_error;
    }
    auto replicated_offset = res.value().last_offset;
    co_await wait_no_throw(
      replicated_offset, ss::lowres_clock::now() + 30s, as);
    co_return std::nullopt;
}

ss::future<> coordinator_stm::do_apply(const model::record_batch& b) {
    if (b.header().type != model::record_batch_type::datalake_coordinator) {
        co_return;
    }
    auto iter = model::record_batch_iterator::create(b);
    while (iter.has_next()) {
        auto r = iter.next();
        iobuf_parser key_p{r.release_key()};
        auto key = co_await serde::read_async<update_key>(key_p);

        iobuf_parser val_p{r.release_value()};
        auto offset = b.base_offset() + model::offset_delta{r.offset_delta()};
        switch (key) {
        case update_key::add_files: {
            auto update = co_await serde::read_async<add_files_update>(val_p);
            auto res = update.apply(state_);
            maybe_log_update_error(key, offset, res);
            continue;
        }
        case update_key::mark_files_committed: {
            auto update
              = co_await serde::read_async<mark_files_committed_update>(val_p);
            auto res = update.apply(state_);
            maybe_log_update_error(key, offset, res);
            continue;
        }
        }
        vlog(
          datalake_log.error,
          "Unknown datalake coordinator STM record type: {}",
          key,
          b.header());
    }
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
