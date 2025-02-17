// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cloud_topics/dl_overlay.h"
#include "cloud_topics/dl_snapshot.h"
#include "cloud_topics/dl_stm/dl_stm_offsets.h"
#include "cloud_topics/dl_version.h"
#include "serde/envelope.h"

#include <deque>

namespace experimental::cloud_topics {

struct dl_overlay_entry
  : serde::
      envelope<dl_overlay_entry, serde::version<0>, serde::compat_version<0>> {
    dl_overlay overlay;

    dl_version added_at;
    dl_version removed_at;

    auto serde_fields() { return std::tie(overlay, added_at, removed_at); }
};

class dl_version_monotonic_invariant
  : public serde::envelope<
      dl_version_monotonic_invariant,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    void set_version(dl_version version) noexcept {
        // Greater or equal for `_last_version` is required to handle retries.
        // Greater for `_last_snapshot_version` to avoid mutating an existing
        // snapshot.
        vassert(
          version >= _last_version && version > _last_snapshot_version,
          "Version can't go backwards. Current version: {}, new version: {}, "
          "last snapshot version: {}",
          _last_version,
          version,
          _last_snapshot_version);
        _last_version = version;
    }

    void set_last_snapshot_version(dl_version version) noexcept {
        // Greater or equal is required to handle retries.
        vassert(
          version >= _last_snapshot_version,
          "Snapshot version can't go backwards. Current snapshot version: {}, "
          "new snapshot version: {}",
          _last_snapshot_version,
          version);
        set_version(version);
        _last_snapshot_version = version;
    }

    auto serde_fields() {
        return std::tie(_last_version, _last_snapshot_version);
    }

private:
    dl_version _last_version;
    dl_version _last_snapshot_version;
};

/// In-memory state of the data layout state machine (dl_stm).
///
/// Separating the state from the state machine allows the state to be
/// checkpointed and restored independently of the state machine.
class dl_stm_state
  : public serde::
      envelope<dl_stm_state, serde::version<0>, serde::compat_version<0>> {
    friend class dl_stm_state_accessor;

public:
    /// Add a new overlay to the state. The overlay becomes visible
    /// starting with the current version.
    void push_overlay(dl_version version, dl_overlay overlay);

    /// Find an overlay that contains the given offset. If no overlay
    /// contains the offset, find the overlay covering the next closest
    /// available offset.
    std::optional<dl_overlay> lower_bound(kafka::offset offset) const;

    /// Create a handle to a snapshot of the state at the current version.
    /// The snapshot id can be used later to read snapshot contents.
    dl_snapshot_id start_snapshot(dl_version version) noexcept;

    bool snapshot_exists(dl_snapshot_id id) const noexcept;

    /// Snapshot of the state at the given version.
    std::optional<dl_snapshot_payload> read_snapshot(dl_snapshot_id id) const;

    /// Remove all snapshots with version less than the given version.
    void remove_snapshots_before(dl_version last_version_to_keep);

    /// Get collection of offsets maintained by the STM
    dl_stm_offsets& get_offsets() noexcept;
    const dl_stm_offsets& get_offsets() const noexcept;

    auto serde_fields() {
        return std::tie(_overlays, _snapshots, _version_invariant, _offsets);
    }

private:
    // A list of overlays that are stored in the cloud storage.
    // The order of elements is undefined.
    std::deque<dl_overlay_entry> _overlays;

    // A list of snapshot handles that are currently open.
    // The list is ordered by version in ascending order to efficiently find the
    // oldest snapshot when running state garbage collection and to remove
    // closed snapshots.
    std::deque<dl_snapshot_id> _snapshots;

    dl_version_monotonic_invariant _version_invariant;
    dl_stm_offsets _offsets;
};

}; // namespace experimental::cloud_topics
