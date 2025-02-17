// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cloud_topics/dl_version.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

namespace experimental::cloud_topics {

/// Set of offsets tracked by dl_stm_state.
class dl_stm_offsets
  : public serde::
      envelope<dl_stm_offsets, serde::version<0>, serde::compat_version<0>> {
public:
    kafka::offset get_last_reconciled_offset() const noexcept {
        return _last_reconciled_offset;
    }

    void advance_last_reconciled_offset(kafka::offset new_offset) noexcept {
        _last_reconciled_offset = std::max(_last_reconciled_offset, new_offset);
    }

    model::offset get_insync_offset() const noexcept { return _insync_offset; }

    /// Advance insync offset of the dl_stm.
    void advance_insync_offset(model::offset new_offset) noexcept {
        _insync_offset = std::max(_insync_offset, new_offset);
    }

    model::offset get_applied_offset() const noexcept {
        return _applied_offset;
    }

    /// Advance last applied offset to be equal to insync offset
    void advance_applied_offset() noexcept {
        vassert(
          check_invariant(),
          "Can't advance applied offset, invariant is broken, last-reconciled: "
          "{}, insync: {}, applied: {}",
          _last_reconciled_offset,
          _insync_offset,
          _applied_offset);
        _applied_offset = _insync_offset;
    }

    auto serde_fields() {
        return std::tie(
          _last_reconciled_offset, _insync_offset, _applied_offset);
    }

    bool check_invariant() { return _insync_offset >= _applied_offset; }

    /// This method gates all command batch applications for the dl_stm.
    /// If this method returned 'true' for the 'version' then the command
    /// could be safely applied.
    bool can_apply(dl_version version) const noexcept {
        // Invariant: the version can only be applied if it's equal to
        // insync_offset. If this is not the case the dl_stm batch
        // is applied out of order and should be rejected. If the version
        // is equal to applied_offset then the batch was already applied
        // and should be rejected. This is necessary because we're always
        // using base_offset of the command batch to advance the offsets.
        return _applied_offset < _insync_offset && _insync_offset == version();
    }

private:
    kafka::offset _last_reconciled_offset;
    model::offset _insync_offset;
    model::offset _applied_offset;
};

}; // namespace experimental::cloud_topics
