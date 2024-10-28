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
#include "cloud_topics/dl_version.h"
#include "container/fragmented_vector.h"

namespace experimental::cloud_topics {

struct dl_overlay_entry {
    dl_overlay overlay;

    dl_version added_at;
    dl_version removed_at;
};

class dl_version_monotonic_invariant {
public:
    void set_version(dl_version version) noexcept {
        // Greater or equal is required to handle retries.
        vassert(
          version >= _last_version,
          "Version can't go backwards. Current version: {}, new version: {}",
          _last_version,
          version);
        _last_version = version;
    }

private:
    dl_version _last_version;
};

/// In-memory state of the data layout state machine (dl_stm).
///
/// Separating the state from the state machine allows the state to be
/// checkpointed and restored independently of the state machine.
class dl_stm_state {
    friend class dl_stm_state_accessor;

public:
    /// Add a new overlay to the state. The overlay becomes visible
    /// starting with the current version.
    void push_overlay(dl_version version, dl_overlay overlay);

    /// Find an overlay that contains the given offset. If no overlay
    /// contains the offset, find the overlay covering the next closest
    /// available offset.
    std::optional<dl_overlay> lower_bound(kafka::offset offset) const;

private:
    // A list of overlays that are stored in the cloud storage.
    // The order of elements is undefined.
    std::deque<dl_overlay_entry> _overlays;

    dl_version_monotonic_invariant _version_invariant;
};

}; // namespace experimental::cloud_topics
