/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "serde/rw/envelope.h"

#include <seastar/util/bool_class.hh>

#include <fmt/core.h>

#include <optional>

namespace cloud_topics::l1 {

// A contiguous range of offsets in a partition that should be rewritten,
// together with per-range stats the scheduler can use to plan work.
struct levelable_range
  : serde::
      envelope<levelable_range, serde::version<1>, serde::compat_version<0>> {
    bool operator==(const levelable_range&) const = default;

    auto serde_fields() {
        return std::tie(base_offset, last_offset, size_bytes, extent_count);
    }

    kafka::offset base_offset;
    kafka::offset last_offset;
    // Sum of the undersized extents' bytes within this range.
    size_t size_bytes{0};
    // Number of undersized extents within this range.
    size_t extent_count{0};

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{base:{}, last:{}, size_bytes:{}, extent_count:{}}}",
          base_offset,
          last_offset,
          size_bytes,
          extent_count);
    }
};

// Builds a sequence of `levelable_range`s eligible for leveling from extents
// provided via `process_extent`, returning them from `finalize()`. An extent
// is considered undersized when its length is strictly less than
// `min_acceptable_extent_bytes`.
//
// We only consider rewriting a *range* of consecutive undersized extents.
// Healthy extents close the active range. We never include a healthy extent in
// a leveling range, as those have per-byte rewrite cost without corresponding
// extent-count savings.
//
//   - Undersized extent: extend (or open) the active range.
//   - Healthy extent: close the active range.
//   - When the active range reaches `max_acceptable_range_bytes` after
//     extending, commit it and start a fresh range with the next undersized
//     extent. This caps each leveling job's work to a bounded size so it
//     fits within a single output L1 object, runs in seconds rather than
//     minutes, and can be soft-stopped at coarse-grained boundaries without
//     wasting hours of upload work.
//   - On close: commit the range only if it contains more than one
//     extent (K > 1), as singleton ranges can't reduce extent count.
//   - The range still open when the extents run out is the partition's tail;
//     unlike a range closed by a healthy extent, it keeps growing as new data
//     is reconciled. It is only committed once its total reaches
//     `min_acceptable_extent_bytes`, so that its output is itself a healthy
//     extent. Rewriting it earlier would produce yet another undersized
//     extent that gets re-leveled on every pass as new small extents land
//     behind it — quadratic write amplification at low produce rates.
class leveling_range_builder {
public:
    explicit leveling_range_builder(size_t min_acceptable_extent_bytes)
      : _min_acceptable_extent_bytes(min_acceptable_extent_bytes)
      , _max_acceptable_range_bytes(
          config::shard_local_cfg().cloud_topics_leveling_max_range_bytes())
      , _max_ranges(
          config::shard_local_cfg()
            .cloud_topics_leveling_max_ranges_per_partition()) {}

    // Whether the cap on ranges per partition has been reached. Callers should
    // stop feeding extents once this is true; the scan reply is bounded and
    // the remaining ranges are picked up by a later scan.
    bool is_full() const { return _ranges.size() >= _max_ranges; }

    // Processes a single extent.
    void process_extent(kafka::offset base, kafka::offset last, size_t len) {
        if (len >= _min_acceptable_extent_bytes) {
            // A healthy extent closes any active range.
            maybe_commit_range(is_tail_range::no);
            return;
        }
        if (!_range.has_value()) {
            _range.emplace(base, last, len, 1);
            return;
        }
        _range->last = last;
        _range->bytes += len;
        _range->extent_count += 1;

        // Cap per-range bytes so each leveling job stays bounded.
        if (_range->bytes >= _max_acceptable_range_bytes) {
            maybe_commit_range(is_tail_range::no);
        }
    }

    // Commit any pending range and return the accumulated ranges. Must
    // be called after all extents have been processed.
    chunked_vector<levelable_range> finalize() && {
        maybe_commit_range(is_tail_range::yes);
        return std::move(_ranges);
    }

private:
    struct in_progress_range {
        kafka::offset base;
        kafka::offset last;
        size_t bytes;
        size_t extent_count;
    };

    using is_tail_range = ss::bool_class<struct is_tail_range_tag>;

    void maybe_commit_range(is_tail_range tail) {
        if (!_range.has_value()) {
            return;
        }
        // The tail range keeps growing as new data is reconciled; hold it
        // back until leveling it can produce a healthy output extent (see
        // class comment).
        const bool undersized_tail = tail == is_tail_range::yes
                                     && _range->bytes
                                          < _min_acceptable_extent_bytes;
        if (
          _range->extent_count > 1 && !undersized_tail
          && _ranges.size() < _max_ranges) {
            _ranges.push_back(
              levelable_range{
                .base_offset = _range->base,
                .last_offset = _range->last,
                .size_bytes = _range->bytes,
                .extent_count = _range->extent_count,
              });
        }
        _range.reset();
    }

    size_t _min_acceptable_extent_bytes;
    size_t _max_acceptable_range_bytes;
    size_t _max_ranges;
    std::optional<in_progress_range> _range;
    chunked_vector<levelable_range> _ranges;
};

} // namespace cloud_topics::l1
