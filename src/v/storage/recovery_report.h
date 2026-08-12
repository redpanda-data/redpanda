/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <cstddef>
#include <string_view>
#include <utility>

namespace storage {

/// Where a segment sat in its log when recovery dropped it. Recovery renames
/// the segment out of the log, so nothing can work this out afterward.
enum class segment_position {
    /// No segment that recovery kept or dropped had a higher base offset.
    tail,
    /// One of them did, so the offsets this segment claimed sit inside the log.
    mid_log,
    /// Nothing recorded the position.
    unknown,
};

/// The name recovery gives an unrecoverable segment file carries this, so a
/// change here changes a name on disk.
constexpr std::string_view to_string_view(segment_position p) {
    switch (p) {
    case segment_position::tail:
        return "tail";
    case segment_position::mid_log:
        return "mid_log";
    case segment_position::unknown:
        return "unknown";
    }
    std::unreachable();
}

/// Counts that recovery collects while it opens one log.
struct recovery_report {
    /// Recovery renamed these segments to `.cannotrecover` and dropped them
    /// from the log, because replay could not validate the first batch. Each
    /// one had the highest base offset among the segments recovery kept or
    /// dropped.
    size_t dropped_at_tail{0};

    // The same for segments whose base offset was not the highest among the
    // segments recovery kept or dropped. Dropping a segment here can leave the
    // log with offsets not covered by any segment.
    size_t dropped_mid_log{0};

    /// The same, where nothing recorded the position.
    size_t dropped_position_unknown{0};

    size_t& count_at(segment_position p) {
        switch (p) {
        case segment_position::tail:
            return dropped_at_tail;
        case segment_position::mid_log:
            return dropped_mid_log;
        case segment_position::unknown:
            return dropped_position_unknown;
        }
        std::unreachable();
    }

    bool empty() const {
        return dropped_at_tail == 0 && dropped_mid_log == 0
               && dropped_position_unknown == 0;
    }
};

} // namespace storage
