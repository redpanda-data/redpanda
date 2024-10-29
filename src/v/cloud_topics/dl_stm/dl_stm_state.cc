// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_stm/dl_stm_state.h"

#include "cloud_topics/dl_overlay.h"
#include "cloud_topics/dl_snapshot.h"
#include "model/fundamental.h"

#include <algorithm>

namespace experimental::cloud_topics {

void dl_stm_state::push_overlay(dl_version version, dl_overlay overlay) {
    _version_invariant.set_version(version);

    auto entry_it = std::find_if(
      _overlays.begin(),
      _overlays.end(),
      [&overlay](const dl_overlay_entry& entry) {
          return entry.overlay == overlay;
      });
    if (entry_it != _overlays.end()) {
        // A duplicate push_overlay is tolerated if this is a retry. A retry can
        // only happen if the version is the same.
        // Otherwise it's an error.
        if (entry_it->added_at == version) {
            return;
        }

        throw std::runtime_error(fmt::format(
          "Overlay already exists but added at a different version. Overlay: "
          "{}, added_at: {}, "
          "current_version: {}",
          overlay,
          entry_it->added_at,
          version));
    }

    _overlays.push_back(dl_overlay_entry{
      .overlay = std::move(overlay),
      // The overlay becomes visible starting with the current version.
      .added_at = version,
    });
}

std::optional<dl_overlay>
dl_stm_state::lower_bound(kafka::offset offset) const {
    std::optional<dl_overlay> best_match;

    for (auto& entry : _overlays) {
        // Skip over removed overlays.
        if (entry.removed_at != dl_version{}) {
            continue;
        }

        if (entry.overlay.last_offset >= offset) {
            if (
              !best_match.has_value()
              || entry.overlay.base_offset < best_match->base_offset) {
                best_match = entry.overlay;
            }
        }
    }

    return best_match;
}

dl_snapshot_id dl_stm_state::start_snapshot(dl_version version) noexcept {
    _version_invariant.set_last_snapshot_version(version);

    auto id = dl_snapshot_id(version);
    _snapshots.push_back(id);

    return id;
}

bool dl_stm_state::snapshot_exists(dl_snapshot_id id) const noexcept {
    return std::binary_search(
      _snapshots.begin(),
      _snapshots.end(),
      id,
      [](const dl_snapshot_id& a, const dl_snapshot_id& b) {
          return a.version < b.version;
      });
}

std::optional<dl_snapshot_payload>
dl_stm_state::read_snapshot(dl_snapshot_id id) const {
    auto it = std::find_if(
      _snapshots.begin(), _snapshots.end(), [&id](const dl_snapshot_id& s) {
          return s.version == id.version;
      });

    // Snapshot not found.
    if (it == _snapshots.end()) {
        return std::nullopt;
    }

    // Collect overlays that are visible at the snapshot version.
    fragmented_vector<dl_overlay> overlays;
    for (const auto& entry : _overlays) {
        if (
          entry.added_at <= id.version
          && (entry.removed_at == dl_version{} || entry.removed_at > id.version)) {
            overlays.push_back(entry.overlay);
        }
    }

    return dl_snapshot_payload{
      .id = *it,
      .overlays = std::move(overlays),
    };
}

void dl_stm_state::remove_snapshots_before(dl_version last_version_to_keep) {
    if (_snapshots.empty()) {
        throw std::runtime_error(fmt::format(
          "Attempt to remove snapshots before version {} but no snapshots "
          "exist",
          last_version_to_keep));
    }

    // Find the first snapshot to keep. It is the first snapshot with a version
    // equal or greater than the version to keep.
    auto it = std::lower_bound(
      _snapshots.begin(),
      _snapshots.end(),
      last_version_to_keep,
      [](const dl_snapshot_id& a, dl_version b) { return a.version < b; });

    if (it == _snapshots.begin()) {
        // Short circuit if there are no snapshots to remove
        return;
    } else if (it == _snapshots.end()) {
        throw std::runtime_error(fmt::format(
          "Trying to remove snapshots before an non-existent snapshot",
          last_version_to_keep));
    } else {
        _snapshots.erase(_snapshots.begin(), it);
    }
}

} // namespace experimental::cloud_topics
