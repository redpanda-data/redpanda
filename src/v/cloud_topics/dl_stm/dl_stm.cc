// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_stm/dl_stm.h"

#include "cloud_topics/dl_stm/dl_stm_commands.h"
#include "cloud_topics/dl_stm/dl_stm_state.h"
#include "serde/rw/map.h"
#include "serde/rw/uuid.h"

namespace experimental::cloud_topics {

dl_stm::dl_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>(name, logger, raft) {}

ss::future<> dl_stm::do_apply(const model::record_batch& batch) {
    if (batch.header().type != model::record_batch_type::dl_stm_command) {
        co_return;
    }
    vlog(_log.debug, "Applying record batch: {}", batch.header());

    // Note: do_apply will be called multiple times with the same batch if it
    // throws. The method must be idempotent. Because of this we use the batch
    // base offset as dl_version rather than record offset. This also means that
    // in the same batch we cannot add an overlay and then remove it. Other
    // caveats may exist as well.
    //
    // The version can't go backwards but in case of a partial apply and a retry
    // it could.
    //
    // Consider building a delta instead of applying the batch directly and
    // implement a noexcept dl_stm_state::apply_delta which is atomic.
    auto new_dl_version = dl_version(batch.base_offset());

    batch.for_each_record([new_dl_version, this](model::record&& r) {
        auto key = serde::from_iobuf<dl_stm_key>(r.release_key());
        switch (key) {
        case dl_stm_key::push_overlay: {
            auto cmd = serde::from_iobuf<push_overlay_cmd>(r.release_value());
            _state.push_overlay(new_dl_version, std::move(cmd.overlay));
            break;
        }
        }
    });

    co_return;
}

ss::future<> dl_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) {
    co_return;
}

ss::future<raft::stm_snapshot>
dl_stm::take_local_snapshot(ssx::semaphore_units) {
    co_return raft::stm_snapshot{};
}

ss::future<> dl_stm::apply_raft_snapshot(const iobuf&) { co_return; }

ss::future<iobuf> dl_stm::take_snapshot(model::offset) { co_return iobuf(); }

}; // namespace experimental::cloud_topics
