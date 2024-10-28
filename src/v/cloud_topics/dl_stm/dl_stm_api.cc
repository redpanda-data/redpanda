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

ss::future<result<bool, dl_stm_api_errc>>
dl_stm_api::push_overlay(dl_overlay overlay) {
    // TODO: Sync state and consider whether we need to encode invariants in the
    // command.
    model::term_id term = _stm->_raft->term();

    vlog(_logger.debug, "Replicating dl_stm_cmd::push_overlay_cmd");

    storage::record_batch_builder builder(
      model::record_batch_type::dl_stm_command, model::offset(0));
    builder.add_raw_kv(
      serde::to_iobuf(dl_stm_key::push_overlay),
      serde::to_iobuf(push_overlay_cmd(std::move(overlay))));

    auto batch = std::move(builder).build();
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    auto opts = raft::replicate_options(raft::consistency_level::quorum_ack);
    opts.set_force_flush();
    auto res = co_await _stm->_raft->replicate(term, std::move(reader), opts);

    if (res.has_error()) {
        throw std::runtime_error(
          fmt::format("Failed to replicate overlay: {}", res.error()));
    }

    co_await _stm->wait_no_throw(
      res.value().last_offset, model::timeout_clock::now() + 30s);

    co_return outcome::success(true);
}

std::optional<dl_overlay> dl_stm_api::lower_bound(kafka::offset offset) const {
    return _stm->_state.lower_bound(offset);
}

}; // namespace experimental::cloud_topics
