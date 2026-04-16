/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/ctp_stm_api.h"

#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_commands.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"

#include <seastar/coroutine/as_future.hh>

#include <exception>

namespace cloud_topics {

std::ostream& operator<<(std::ostream& o, ctp_stm_api_errc errc) {
    switch (errc) {
    case ctp_stm_api_errc::timeout:
        return o << "timeout";
    case ctp_stm_api_errc::not_leader:
        return o << "not_leader";
    case ctp_stm_api_errc::shutdown:
        return o << "shutdown";
    case ctp_stm_api_errc::failure:
        return o << "failure";
    }
}

ctp_stm_api::ctp_stm_api(ss::shared_ptr<ctp_stm> stm)
  : _stm(std::move(stm))
  , _log(_stm->log()) {}

ss::future<std::expected<model::offset, ctp_stm_api_errc>>
ctp_stm_api::replicated_apply(
  model::record_batch&& batch,
  std::optional<model::term_id> expected_term,
  model::timeout_clock::time_point deadline,
  ss::abort_source& as) {
    model::term_id term = expected_term.value_or(_stm->_raft->term());

    vlog(_log.debug, "Replicating batch {} in term {}", batch.header(), term);

    auto opts = raft::replicate_options(
      raft::consistency_level::quorum_ack,
      /*expected_term=*/term,
      /*timeout=*/std::nullopt,
      as);
    opts.set_force_flush();
    auto res = co_await _stm->_raft->replicate(std::move(batch), opts);

    if (!res.has_value()) {
        vlog(_log.debug, "Failed to replicate batch: {}", res.error());
        if (res.error() == raft::errc::not_leader) {
            co_return std::unexpected(ctp_stm_api_errc::not_leader);
        }
        co_return std::unexpected(ctp_stm_api_errc::timeout);
    }

    try {
        co_await _stm->wait(res.value().last_offset, deadline, as);
    } catch (...) {
        vlog(
          _log.warn,
          "Failed to wait for replicated command to to be applied: {}",
          std::current_exception());
        co_return std::unexpected(ctp_stm_api_errc::timeout);
    }

    co_return res.value().last_offset;
}

ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
ctp_stm_api::sync_to_next_placeholder(
  model::timeout_clock::time_point deadline, ss::abort_source& as) {
    kafka::offset curr_lro = get_last_reconciled_offset();
    model::offset curr_lrlo = get_last_reconciled_log_offset();

    // there might be an advance_epoch batch somewhere beyond LRO, as yet
    // unreconciled, so we advance the LRLO as far as we safely can (i.e. up to
    // the offset just before the next unreconciled placeholder batch). as a
    // result, a previously applied epoch should make its way into the
    // min_epoch_lower_bound, the estimate for inactive epoch.
    model::offset max_safe_lrlo = model::prev_offset(
      _stm->_raft->log()->to_log_offset(
        kafka::offset_cast(kafka::next_offset(curr_lro))));

    if (max_safe_lrlo <= curr_lrlo) {
        vlog(
          _log.debug,
          "{}: No non-data batches between curr_lro {} and the next "
          "placeholder batch. Nothing to do.",
          _stm->ntp(),
          curr_lro);
        co_return std::monostate{};
    }

    vlog(
      _log.debug,
      "Replicating ctp_stm_cmd::advance_reconciled_offset to advance "
      "LRLO {} -> {}",
      curr_lrlo,
      max_safe_lrlo);

    storage::record_batch_builder builder(
      model::record_batch_type::ctp_stm_command, model::offset{0});

    builder.add_raw_kv(
      serde::to_iobuf(advance_reconciled_offset_cmd::key),
      serde::to_iobuf(advance_reconciled_offset_cmd(curr_lro, max_safe_lrlo)));

    auto batch = std::move(builder).build();
    auto apply_result = co_await replicated_apply(
      std::move(batch), std::nullopt /* expected_term */, deadline, as);
    if (!apply_result.has_value()) {
        co_return std::unexpected{apply_result.error()};
    }

    co_return std::monostate{};
}

ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
ctp_stm_api::advance_reconciled_offset(
  kafka::offset lro,
  model::timeout_clock::time_point deadline,
  ss::abort_source& as) {
    if (lro <= get_last_reconciled_offset()) {
        co_return std::monostate{};
    }

    auto lrlo = _stm->_raft->log()->to_log_offset(kafka::offset_cast(lro));

    vlog(
      _log.debug,
      "Replicating ctp_stm_cmd::advance_reconciled_offset{{lro:{} lrlo:{}}}",
      lro,
      lrlo);

    storage::record_batch_builder builder(
      model::record_batch_type::ctp_stm_command, model::offset(0));
    builder.add_raw_kv(
      serde::to_iobuf(advance_reconciled_offset_cmd::key),
      serde::to_iobuf(advance_reconciled_offset_cmd(lro, lrlo)));

    auto batch = std::move(builder).build();
    auto apply_result = co_await replicated_apply(
      std::move(batch), std::nullopt /* expected_term */, deadline, as);

    if (!apply_result.has_value()) {
        co_return std::unexpected(apply_result.error());
    }

    co_return std::monostate{};
}

ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
ctp_stm_api::set_start_offset(
  kafka::offset start_offset,
  model::timeout_clock::time_point deadline,
  ss::abort_source& as) {
    if (start_offset <= get_start_offset()) {
        co_return std::monostate{};
    }

    vlog(
      _log.debug,
      "Replicating ctp_stm_cmd::set_new_start_offset{{{}}}",
      start_offset);

    storage::record_batch_builder builder(
      model::record_batch_type::ctp_stm_command, model::offset(0));

    builder.add_raw_kv(
      serde::to_iobuf(set_start_offset_cmd::key),
      serde::to_iobuf(set_start_offset_cmd(start_offset)));

    auto batch = std::move(builder).build();
    auto apply_result = co_await replicated_apply(
      std::move(batch), std::nullopt /* expected_term */, deadline, as);

    if (!apply_result.has_value()) {
        co_return std::unexpected(apply_result.error());
    }

    co_return std::monostate{};
}

ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
ctp_stm_api::advance_epoch(
  cluster_epoch new_epoch,
  model::timeout_clock::time_point deadline,
  ss::abort_source& as) {
    if (new_epoch < get_max_epoch()) {
        co_return std::monostate{};
    }
    auto fence_fut = co_await ss::coroutine::as_future(fence_epoch(new_epoch));
    if (fence_fut.failed()) {
        auto e = fence_fut.get_exception();
        vlogl(
          _log,
          ssx::is_shutdown_exception(e) ? ss::log_level::debug
                                        : ss::log_level::warn,
          "Failed to fence epoch {} for ntp {}, error: {}",
          new_epoch,
          _stm->ntp(),
          e);
        co_return std::unexpected{ctp_stm_api_errc::failure};
    }
    auto fence = std::move(fence_fut).get();
    if (!fence.has_value()) {
        vlog(
          _log.warn,
          "Failed to fence epoch {} for ntp {}, ctp latest seen epoch is [{}, "
          "{}]",
          new_epoch,
          _stm->ntp(),
          fence.error().window_min,
          fence.error().window_max);
        co_return std::unexpected{ctp_stm_api_errc::failure};
    }

    vlog(_log.debug, "Replicating ctp_stm_cmd::advance_epoch{{{}}}", new_epoch);

    storage::record_batch_builder builder(
      model::record_batch_type::ctp_stm_command, model::offset(0));

    builder.add_raw_kv(
      serde::to_iobuf(advance_epoch_cmd::key),
      serde::to_iobuf(advance_epoch_cmd(new_epoch)));

    auto batch = std::move(builder).build();
    auto apply_result = co_await replicated_apply(
      std::move(batch), fence.value().term, deadline, as);

    if (!apply_result.has_value()) {
        co_return std::unexpected(apply_result.error());
    }
    co_return std::monostate{};
}

kafka::offset ctp_stm_api::get_last_reconciled_offset() const {
    return _stm->state().get_last_reconciled_offset().value_or(kafka::offset());
}

model::offset ctp_stm_api::get_last_reconciled_log_offset() const {
    return _stm->state().get_last_reconciled_log_offset().value_or(
      model::offset{});
}

kafka::offset ctp_stm_api::get_start_offset() const {
    return _stm->state().start_offset();
}

ss::future<std::expected<std::optional<cluster_epoch>, ctp_stm_api_errc>>
ctp_stm_api::get_inactive_epoch() const {
    auto res = co_await ss::coroutine::as_future(_stm->get_inactive_epoch());
    if (res.failed()) {
        auto e = res.get_exception();
        if (ssx::is_shutdown_exception(e)) {
            co_return std::unexpected(ctp_stm_api_errc::shutdown);
        }
        vlog(_log.error, "Failed to get minimum epoch from ctp_stm: {}", e);
        co_return std::unexpected(ctp_stm_api_errc::failure);
    }
    co_return res.get();
}

std::optional<cluster_epoch>
ctp_stm_api::estimate_inactive_epoch() const noexcept {
    return _stm->estimate_inactive_epoch();
}

std::optional<model::offset>
ctp_stm_api::get_epoch_window_offset() const noexcept {
    return _stm->state().current_epoch_window_offset();
}

ss::future<bool> ctp_stm_api::sync_in_term(
  model::timeout_clock::time_point deadline, ss::abort_source& as) {
    vlog(_log.debug, "Syncing ctp_stm in term {}", _stm->_raft->term());
    co_return co_await _stm->sync_in_term(deadline, as);
}

ss::future<std::expected<cluster_epoch_fence, stale_cluster_epoch>>
ctp_stm_api::fence_epoch(cluster_epoch e) {
    vlog(_log.debug, "Fencing epoch {} in term {}", e, _stm->_raft->term());
    auto res = co_await _stm->fence_epoch(e);
    vlog(
      _log.debug,
      "Fence acquired = {} for epoch {} in term {}",
      res.has_value(),
      e,
      res.has_value() ? res->term : model::term_id{-1});
    co_return std::move(res);
}

std::optional<cluster_epoch> ctp_stm_api::get_max_epoch() const {
    return _stm->state().get_max_applied_epoch();
}

std::optional<cluster_epoch>
ctp_stm_api::get_max_seen_epoch(model::term_id term) const {
    return _stm->state().get_max_seen_epoch(term);
}

l0::producer_queue& ctp_stm_api::producer_queue() {
    return _stm->producer_queue();
}

uint64_t ctp_stm_api::estimated_data_size() const noexcept {
    return _stm->state().estimated_data_size();
}

void ctp_stm_api::register_reader(active_reader_state* state) {
    return _stm->register_reader(state);
}

}; // namespace cloud_topics
