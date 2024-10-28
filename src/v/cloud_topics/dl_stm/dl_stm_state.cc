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

} // namespace experimental::cloud_topics
