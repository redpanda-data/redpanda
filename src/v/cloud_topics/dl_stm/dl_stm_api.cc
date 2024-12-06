// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_stm/dl_stm_api.h"

#include "base/outcome.h"
#include "cloud_topics/dl_stm/dl_stm.h"
#include "cloud_topics/dl_stm/dl_stm_commands.h"
#include "model/fundamental.h"
#include "raft/consensus.h"
#include "serde/rw/uuid.h"
#include "storage/record_batch_builder.h"

#include <stdexcept>

namespace experimental::cloud_topics {

std::ostream& operator<<(std::ostream& o, dl_stm_api_errc errc) {
    switch (errc) {
    case dl_stm_api_errc::timeout:
        return o << "timeout";
    case dl_stm_api_errc::not_leader:
        return o << "not_leader";
    }
}

dl_stm_api::dl_stm_api(ss::logger& logger, ss::shared_ptr<dl_stm> stm)
  : _logger(logger)
  , _stm(std::move(stm)) {}

ss::future<> dl_stm_api::stop() { co_await _gate.close(); }

ss::future<result<bool, dl_stm_api_errc>>
dl_stm_api::push_overlay(dl_overlay overlay) {
    vlog(_logger.debug, "Replicating dl_stm_cmd::push_overlay_cmd");
    auto h = _gate.hold();

    // TODO: Sync state and consider whether we need to encode invariants in the
    // command.

    storage::record_batch_builder builder(
      model::record_batch_type::dl_stm_command, model::offset(0));
    builder.add_raw_kv(
      serde::to_iobuf(dl_stm_key::push_overlay),
      serde::to_iobuf(push_overlay_cmd(std::move(overlay))));

    auto batch = std::move(builder).build();
    auto apply_result = co_await replicated_apply(std::move(batch));
    if (apply_result.has_failure()) {
        co_return apply_result.error();
    }

    co_return outcome::success(true);
}

std::optional<dl_overlay> dl_stm_api::lower_bound(kafka::offset offset) const {
    return _stm->_state.lower_bound(offset);
}

ss::future<checked<dl_snapshot_id, dl_stm_api_errc>>
dl_stm_api::start_snapshot() {
    vlog(_logger.debug, "Replicating dl_stm_cmd::start_snapshot_cmd");
    auto h = _gate.hold();

    storage::record_batch_builder builder(
      model::record_batch_type::dl_stm_command, model::offset(0));
    builder.add_raw_kv(
      serde::to_iobuf(dl_stm_key::start_snapshot),
      serde::to_iobuf(start_snapshot_cmd()));

    auto batch = std::move(builder).build();

    auto apply_result = co_await replicated_apply(std::move(batch));
    if (apply_result.has_failure()) {
        co_return apply_result.error();
    }

    // We abuse knowledge of implementation detail here to construct the
    // dl_snapshot_id without having to setup listeners and notifiers of command
    // apply.
    auto expected_id = dl_snapshot_id(dl_version(apply_result.value()));

    // Ensure that the expected snapshot was created.
    if (!_stm->_state.snapshot_exists(expected_id)) {
        throw std::runtime_error(fmt::format(
          "Snapshot with expected id not found after waiting for command to be "
          "applied: {}",
          expected_id));
    }

    co_return outcome::success(expected_id);
}

std::optional<dl_snapshot_payload>
dl_stm_api::read_snapshot(dl_snapshot_id id) {
    return _stm->_state.read_snapshot(id);
}

ss::future<checked<void, dl_stm_api_errc>>
dl_stm_api::remove_snapshots_before(dl_version last_version_to_keep) {
    vlog(_logger.debug, "Replicating dl_stm_cmd::remove_snapshots_cmd");
    auto h = _gate.hold();

    storage::record_batch_builder builder(
      model::record_batch_type::dl_stm_command, model::offset(0));
    builder.add_raw_kv(
      serde::to_iobuf(dl_stm_key::remove_snapshots_before_version),
      serde::to_iobuf(
        remove_snapshots_before_version_cmd(last_version_to_keep)));

    auto batch = std::move(builder).build();
    auto apply_result = co_await replicated_apply(std::move(batch));
    if (apply_result.has_failure()) {
        co_return apply_result.error();
    }

    co_return outcome::success();
}

ss::future<checked<model::offset, dl_stm_api_errc>>
dl_stm_api::replicated_apply(model::record_batch&& batch) {
    model::term_id term = _stm->_raft->term();

    auto opts = raft::replicate_options(raft::consistency_level::quorum_ack);
    opts.set_force_flush();
    auto res = co_await _stm->_raft->replicate(term, std::move(batch), opts);

    if (res.has_error()) {
        throw std::runtime_error(
          fmt::format("Failed to replicate overlay: {}", res.error()));
    }

    co_await _stm->wait(
      res.value().last_offset, model::timeout_clock::now() + 30s);

    co_return res.value().last_offset;
}

}; // namespace experimental::cloud_topics
