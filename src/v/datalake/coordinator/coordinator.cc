/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/coordinator.h"

#include "base/vlog.h"
#include "container/fragmented_vector.h"
#include "datalake/coordinator/state_update.h"
#include "datalake/logger.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/record_batch_builder.h"

namespace datalake::coordinator {

namespace {
coordinator::errc convert_stm_errc(coordinator_stm::errc e) {
    switch (e) {
    case coordinator_stm::errc::not_leader:
        return coordinator::errc::not_leader;
    case coordinator_stm::errc::shutting_down:
        return coordinator::errc::shutting_down;
    case coordinator_stm::errc::apply_error:
        return coordinator::errc::stm_apply_error;
    case coordinator_stm::errc::raft_error:
        return coordinator::errc::timedout;
    }
}
} // namespace

std::ostream& operator<<(std::ostream& o, coordinator::errc e) {
    switch (e) {
    case coordinator::errc::not_leader:
        return o << "coordinator::errc::not_leader";
    case coordinator::errc::shutting_down:
        return o << "coordinator::errc::shutting_down";
    case coordinator::errc::stm_apply_error:
        return o << "coordinator::errc::stm_apply_error";
    case coordinator::errc::timedout:
        return o << "coordinator::errc::timedout";
    }
}

ss::future<> coordinator::stop_and_wait() {
    as_.request_abort();
    return gate_.close();
}

checked<ss::gate::holder, coordinator::errc> coordinator::maybe_gate() {
    ss::gate::holder h;
    if (as_.abort_requested() || gate_.is_closed()) {
        return errc::shutting_down;
    }
    return gate_.hold();
}

ss::future<checked<std::nullopt_t, coordinator::errc>>
coordinator::sync_add_files(
  model::topic_partition tp, chunked_vector<translated_offset_range> entries) {
    if (entries.empty()) {
        vlog(datalake_log.debug, "Empty entry requested {}", tp);
        co_return std::nullopt;
    }
    auto gate = maybe_gate();
    if (gate.has_error()) {
        co_return gate.error();
    }
    vlog(
      datalake_log.debug,
      "Sync add files requested {}: [{}, {}], {} files",
      tp,
      entries.begin()->start_offset,
      entries.back().last_offset,
      entries.size());
    auto sync_res = co_await stm_->sync(10s);
    if (sync_res.has_error()) {
        co_return convert_stm_errc(sync_res.error());
    }
    auto added_last_offset = entries.back().last_offset;
    auto update_res = add_files_update::build(
      stm_->state(), tp, std::move(entries));
    if (update_res.has_error()) {
        // NOTE: rejection here is just an optimization -- the operation would
        // fail to be applied to the STM anyway.
        vlog(
          datalake_log.debug,
          "Rejecting request to add files for {}: {}",
          tp,
          update_res.error());
        co_return errc::stm_apply_error;
    }
    storage::record_batch_builder builder(
      model::record_batch_type::datalake_coordinator, model::offset{0});
    builder.add_raw_kv(
      serde::to_iobuf(add_files_update::key),
      serde::to_iobuf(std::move(update_res.value())));
    auto repl_res = co_await stm_->replicate_and_wait(
      sync_res.value(), std::move(builder).build(), as_);
    if (repl_res.has_error()) {
        co_return convert_stm_errc(repl_res.error());
    }

    // Check that the resulting state matches that expected by the caller.
    // NOTE: a mismatch here just means there was a race to update the STM, and
    // this should be handled by callers.
    // TODO: would be nice to encapsulate this in some update validator.
    auto prt_opt = stm_->state().partition_state(tp);
    if (
      !prt_opt.has_value() || prt_opt->get().pending_entries.empty()
      || prt_opt->get().pending_entries.back().last_offset
           != added_last_offset) {
        vlog(
          datalake_log.debug,
          "Resulting last offset for {} does not match expected {}",
          tp,
          added_last_offset);
        co_return errc::stm_apply_error;
    }
    co_return std::nullopt;
}

ss::future<checked<std::optional<kafka::offset>, coordinator::errc>>
coordinator::sync_get_last_added_offset(model::topic_partition tp) {
    auto gate = maybe_gate();
    if (gate.has_error()) {
        co_return gate.error();
    }
    auto sync_res = co_await stm_->sync(10s);
    if (sync_res.has_error()) {
        co_return convert_stm_errc(sync_res.error());
    }
    auto prt_state_opt = stm_->state().partition_state(tp);
    if (!prt_state_opt.has_value()) {
        co_return std::nullopt;
    }
    const auto& prt_state = prt_state_opt->get();
    if (prt_state.pending_entries.empty()) {
        co_return prt_state.last_committed;
    }
    co_return prt_state.pending_entries.back().last_offset;
}

} // namespace datalake::coordinator
