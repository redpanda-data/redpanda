// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_stm/dl_stm.h"

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_topics/dl_stm/dl_stm_commands.h"
#include "cloud_topics/dl_stm/dl_stm_state.h"
#include "serde/rw/map.h"
#include "serde/rw/uuid.h"
#include "serde/rw/vector.h"

#include <stdexcept>

namespace experimental::cloud_topics {

dl_stm::dl_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>(name, logger, raft) {}

ss::future<> dl_stm::do_apply(const model::record_batch& batch) {
    _state.get_offsets().advance_insync_offset(batch.base_offset());
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
    auto new_dl_version = dl_version(batch.base_offset());

    if (!_state.get_offsets().can_apply(new_dl_version)) {
        vlog(
          _log.warn,
          "Record batch at offset {} is applied out of order",
          new_dl_version);
        co_return;
    }

    batch.for_each_record([new_dl_version, this](model::record&& r) {
        auto key = serde::from_iobuf<dl_stm_key>(r.release_key());
        switch (key) {
        case dl_stm_key::push_overlay: {
            auto cmd = serde::from_iobuf<push_overlay_cmd>(r.release_value());
            // Noexcept
            _state.push_overlay(new_dl_version, std::move(cmd.overlay));
            break;
        }
        case dl_stm_key::start_snapshot: {
            std::ignore = serde::from_iobuf<start_snapshot_cmd>(
              r.release_value());
            // Noexcept
            _state.start_snapshot(new_dl_version);
            break;
        }
        case dl_stm_key::remove_snapshots_before_version:
            auto cmd = serde::from_iobuf<remove_snapshots_before_version_cmd>(
              r.release_value());
            try {
                _state.remove_snapshots_before(cmd.last_version_to_keep);
            } catch (const std::runtime_error& e) {
                // We don't have any other option but to ignore the error.
                // The STM behaves deterministically so retrying will result
                // in the same exception. The exception can only be caused
                // by the incorrect command in the log (invalid version that
                // can't be found).
                vlog(
                  _log.error,
                  "'remove_snapshots_before_version command at @{} can't be "
                  "applied because of error: {}",
                  new_dl_version,
                  e);
            }
            break;
        }
    });

    // Close the gap between the insync offset and applied offset.
    // After this method is called the call to 'can_apply' using
    // the same version will always return false. This guarantees
    // that any command can only be applied twice even if log replay
    // is not idempotent (which is luckily not the case).
    _state.get_offsets().advance_applied_offset();

    co_return;
}

ss::future<raft::local_snapshot_applied>
dl_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&& buf) {
    _state = serde::from_iobuf<dl_stm_state>(std::move(buf));
    co_return raft::local_snapshot_applied::yes;
}

ss::future<raft::stm_snapshot>
dl_stm::take_local_snapshot(ssx::semaphore_units) {
    auto buf = serde::to_iobuf(_state);
    co_return raft::stm_snapshot::create(
      0, _state.get_offsets().get_insync_offset(), std::move(buf));
}

ss::future<> dl_stm::apply_raft_snapshot(const iobuf& buf) {
    _state = serde::from_iobuf<dl_stm_state>(buf.copy());
    co_return;
}

ss::future<iobuf> dl_stm::take_snapshot(model::offset snapshot_at) {
    auto st = _state.get_state_at(snapshot_at);
    co_return serde::to_iobuf(std::move(st));
}

}; // namespace experimental::cloud_topics
