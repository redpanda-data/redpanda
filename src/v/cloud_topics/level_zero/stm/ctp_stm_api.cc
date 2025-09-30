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

#include "base/outcome.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_commands.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "serde/rw/uuid.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"
#include "utils/retry_chain_node.h"

#include <seastar/coroutine/as_future.hh>

#include <exception>
#include <stdexcept>

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

ctp_stm_api::ctp_stm_api(retry_chain_node& rtc, ss::shared_ptr<ctp_stm> stm)
  : _rtc(&rtc)
  , _rtclog(cd_log, _rtc, stm->ntp().path())
  , _stm(std::move(stm)) {}

ss::future<std::expected<model::offset, ctp_stm_api_errc>>
ctp_stm_api::replicated_apply(
  model::record_batch&& batch, model::timeout_clock::time_point deadline) {
    model::term_id term = _stm->_raft->term();

    vlog(
      _rtclog.debug, "Replicating batch {} in term {}", batch.header(), term);

    auto opts = raft::replicate_options(
      raft::consistency_level::quorum_ack, _rtc.root_abort_source());
    opts.set_force_flush();
    auto res = co_await _stm->_raft->replicate(term, std::move(batch), opts);

    if (res.has_error()) {
        vlog(_rtclog.debug, "Failed to replicate batch: {}", res.error());
        if (res.error() == raft::errc::not_leader) {
            co_return std::unexpected(ctp_stm_api_errc::not_leader);
        }
        co_return std::unexpected(ctp_stm_api_errc::timeout);
    }

    try {
        co_await _stm->wait(
          res.value().last_offset, deadline, _rtc.root_abort_source());
    } catch (...) {
        vlog(
          _rtclog.warn,
          "Failed to wait for replicated command to to be applied: {}",
          std::current_exception());
        co_return std::unexpected(ctp_stm_api_errc::timeout);
    }

    co_return res.value().last_offset;
}

ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
ctp_stm_api::advance_reconciled_offset(
  kafka::offset last_reconciled_offset,
  model::timeout_clock::time_point deadline) {
    if (last_reconciled_offset <= get_last_reconciled_offset()) {
        co_return std::monostate{};
    }

    vlog(_rtclog.debug, "Replicating ctp_stm_cmd::advance_reconciled_offset");

    storage::record_batch_builder builder(
      model::record_batch_type::ctp_stm_command, model::offset(0));

    builder.add_raw_kv(
      serde::to_iobuf(advance_reconciled_offset_cmd::key),
      serde::to_iobuf(advance_reconciled_offset_cmd(last_reconciled_offset)));

    auto batch = std::move(builder).build();
    auto apply_result = co_await replicated_apply(std::move(batch), deadline);

    if (!apply_result.has_value()) {
        co_return std::unexpected(apply_result.error());
    }

    co_return std::monostate{};
}

ss::future<std::expected<std::monostate, ctp_stm_api_errc>>
ctp_stm_api::set_start_offset(
  kafka::offset start_offset, model::timeout_clock::time_point deadline) {
    if (start_offset <= get_start_offset()) {
        co_return std::monostate{};
    }

    vlog(
      _rtclog.debug,
      "Replicating ctp_stm_cmd::set_new_start_offset{{{}}}",
      start_offset);

    storage::record_batch_builder builder(
      model::record_batch_type::ctp_stm_command, model::offset(0));

    builder.add_raw_kv(
      serde::to_iobuf(set_start_offset_cmd::key),
      serde::to_iobuf(set_start_offset_cmd(start_offset)));

    auto batch = std::move(builder).build();
    auto apply_result = co_await replicated_apply(std::move(batch), deadline);

    if (!apply_result.has_value()) {
        co_return std::unexpected(apply_result.error());
    }

    co_return std::monostate{};
}

kafka::offset ctp_stm_api::get_last_reconciled_offset() const {
    return _stm->state().get_last_reconciled_offset().value_or(kafka::offset());
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
        vlog(_rtclog.error, "Failed to get minimum epoch from ctp_stm: {}", e);
        co_return std::unexpected(ctp_stm_api_errc::failure);
    }
    co_return res.get();
}

std::optional<cluster_epoch>
ctp_stm_api::estimate_inactive_epoch() const noexcept {
    return _stm->estimate_inactive_epoch();
}

ss::future<bool>
ctp_stm_api::sync_in_term(model::timeout_clock::time_point deadline) {
    vlog(_rtclog.debug, "Syncing ctp_stm in term {}", _stm->_raft->term());
    co_return co_await _stm->sync_in_term(deadline);
}

ss::future<cluster_epoch_fence> ctp_stm_api::fence_epoch(cluster_epoch e) {
    vlog(_rtclog.debug, "Fencing epoch {} in term {}", e, _stm->_raft->term());
    auto res = co_await _stm->fence_epoch(e);
    vlog(
      _rtclog.debug,
      "Fence acquired = {} in term {}",
      res.unit.has_value(),
      res.term);
    co_return std::move(res);
}

std::optional<cluster_epoch> ctp_stm_api::get_max_epoch() const {
    return _stm->state().get_max_epoch();
}

std::optional<cluster_epoch> ctp_stm_api::get_max_seen_epoch() const {
    return _stm->state().get_max_seen_epoch();
}

}; // namespace cloud_topics
