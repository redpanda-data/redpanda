/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/dl_stm/commands.h"
#include "cloud_topics/dl_stm/overlay_collection.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <absl/container/btree_map.h>

namespace experimental::cloud_topics {

/// This is an im-memory state of the dl_stm.
/// It supports all basic state transitions and serialization/deserialization
/// but it's not hooked up to the persisted_stm. There is an implementation of
/// the dl_stm which uses this dl_stm_state.
///
/// The dl_stm_state applies dl_overlay batches to its state and manages
/// the current view of the partition's data layout.
class dl_stm_state
  : public serde::
      envelope<dl_stm_state, serde::version<0>, serde::compat_version<0>> {
public:
    bool operator==(const dl_stm_state&) const noexcept = default;

    bool register_overlay_cmd(const dl_overlay& o) noexcept;

    std::optional<dl_overlay> lower_bound(kafka::offset o) const noexcept;

    std::optional<kafka::offset>
    get_term_last_offset(model::term_id t) const noexcept;

    auto serde_fields() {
        return std::tie(
          _overlays, _insync_offset, _last_reconciled_offset, _term_map);
    }

    model::offset get_insync_offset() const noexcept { return _insync_offset; }

    kafka::offset get_last_reconciled() const noexcept {
        return _last_reconciled_offset;
    }

    void propagate_last_reconciled_offset(
      kafka::offset base, kafka::offset last) noexcept;

private:
    overlay_collection _overlays;
    model::offset _insync_offset;
    kafka::offset _last_reconciled_offset;
    absl::btree_map<model::term_id, kafka::offset> _term_map;
};

} // namespace experimental::cloud_topics
