/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "model/fundamental.h"
#include "model/record_batch_reader.h"

namespace storage {

// Utility to analyze offset gaps in logs, for testing compaction logic.

/**
 * Results of analysis of message offset gaps within a log instance.
 * This struct is used to return the total `num_gaps` detected, as well as the
 * minimum and maximum offsets that were missing.
 * This is useful for observing which offsets were affected by compaction.
 */
struct log_gap_analysis {
    // For the first gap detected, stores the first offset that of that gap
    // (offset sequence numbers that were expected, but missing).
    model::offset first_gap_start{-1};
    // For the last gap detected, stores the last offset that was missing.
    model::offset last_gap_end{-1};
    size_t num_gaps{0};
    friend std::ostream& operator<<(std::ostream& o, const log_gap_analysis& r);
};

/**
 * Return info about gaps in message offsets.
 * This is useful for testing compaction; key-based compaction preserves the
 * original message offsets, causing gaps where previous messages were deleted.
 * If `expected_start` is set, a gap at beginning of log will be detected if the
 * first message doesn't match this offset. This analysis assumes message
 * offsets form a perfect contiguous range, starting with the value
 * `expected_start`.
 */
log_gap_analysis make_log_gap_analysis(
  model::record_batch_reader&& reader,
  std::optional<model::offset> expected_start);

} // namespace storage
