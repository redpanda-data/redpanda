/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/state.h"

#include "model/namespace.h"

namespace cloud_topics::l1 {

std::optional<std::reference_wrapper<const partition_state>>
state::partition_state(const model::topic_id_partition& tidp) const {
    auto state_iter = topic_to_state.find(tidp.topic_id);
    if (state_iter == topic_to_state.end()) {
        return std::nullopt;
    }
    const auto& topic_state = state_iter->second;
    auto prt_iter = topic_state.pid_to_state.find(tidp.partition);
    if (prt_iter == topic_state.pid_to_state.end()) {
        return std::nullopt;
    }
    return prt_iter->second;
}

bool compaction_state::has_contiguous_range_with_tombstones(
  kafka::offset base_offset, kafka::offset last_offset) const {
    return get_contiguous_range_with_tombstones(base_offset, last_offset)
      .has_value();
}

bool compaction_state::erase_contiguous_range_with_tombstones(
  kafka::offset base_offset, kafka::offset last_offset) {
    auto tombstone_ranges = get_contiguous_range_with_tombstones(
      base_offset, last_offset);
    if (!tombstone_ranges.has_value()) {
        return false;
    }
    std::optional<compaction_state::cleaned_range_with_tombstones>
      replacement_begin;
    if (tombstone_ranges->begin->base_offset != base_offset) {
        replacement_begin = compaction_state::cleaned_range_with_tombstones{
          .base_offset = tombstone_ranges->begin->base_offset,
          .last_offset = kafka::prev_offset(base_offset),
          .cleaned_with_tombstones_at
          = tombstone_ranges->begin->cleaned_with_tombstones_at,
        };
    }
    std::optional<compaction_state::cleaned_range_with_tombstones>
      replacement_last;
    if (tombstone_ranges->last->last_offset != last_offset) {
        replacement_last = compaction_state::cleaned_range_with_tombstones{
          .base_offset = kafka::next_offset(last_offset),
          .last_offset = tombstone_ranges->last->last_offset,
          .cleaned_with_tombstones_at
          = tombstone_ranges->last->cleaned_with_tombstones_at,
        };
    }
    cleaned_ranges_with_tombstones.erase(
      tombstone_ranges->begin, std::next(tombstone_ranges->last));
    if (replacement_begin.has_value()) {
        cleaned_ranges_with_tombstones.insert(*replacement_begin);
    }
    if (replacement_last.has_value()) {
        cleaned_ranges_with_tombstones.insert(*replacement_last);
    }
    return true;
}

void compaction_state::truncate_with_new_start_offset(
  kafka::offset new_start_offset) {
    cleaned_ranges.truncate_with_new_start_offset(new_start_offset);

    // TODO: below is basically identical to offset_interval_set's
    // truncate_with_new_start_offset impl. Figure out a way to unify the code.

    // First, remove all intervals that are fully below the new start.
    while (!cleaned_ranges_with_tombstones.empty()) {
        auto begin_it = cleaned_ranges_with_tombstones.begin();
        if (begin_it->last_offset >= new_start_offset) {
            // This interval is partially or entirely above the new start.
            // Handle below.
            break;
        }
        // This interval is entirely below the new start.
        cleaned_ranges_with_tombstones.erase(begin_it);
    }
    if (cleaned_ranges_with_tombstones.empty()) {
        return;
    }
    auto begin_it = cleaned_ranges_with_tombstones.begin();
    if (begin_it->base_offset >= new_start_offset) {
        // This interval starts above or is aligned exactly with the new start.
        return;
    }
    // This interval is partially below the new start. Replace it with an
    // interval that is aligned with the new start.
    auto truncated_begin = *begin_it;
    truncated_begin.base_offset = new_start_offset;
    cleaned_ranges_with_tombstones.erase(begin_it);
    cleaned_ranges_with_tombstones.insert(truncated_begin);
}

bool compaction_state::may_add(
  const cleaned_range_with_tombstones& new_range) const {
    if (cleaned_ranges_with_tombstones.empty()) {
        return true;
    }
    // Ensure that new_range doesn't overlap with an existing range.
    auto first_ge_base = cleaned_ranges_with_tombstones.lower_bound(new_range);
    if (
      first_ge_base != cleaned_ranges_with_tombstones.end()
      && first_ge_base->base_offset <= new_range.last_offset) {
        // An existing range overlaps with the new range.
        return false;
    }
    if (first_ge_base != cleaned_ranges_with_tombstones.begin()) {
        auto last_lt_base = std::prev(first_ge_base);
        if (last_lt_base->last_offset >= new_range.base_offset) {
            // An existing range overlaps with the new range.
            return false;
        }
    }
    return true;
}

bool compaction_state::add(const cleaned_range_with_tombstones& new_range) {
    if (!may_add(new_range)) {
        return false;
    }
    cleaned_ranges_with_tombstones.insert(new_range);
    return true;
}

std::optional<compaction_state::tombstone_range_iters>
compaction_state::get_contiguous_range_with_tombstones(
  kafka::offset base, kafka::offset last) const {
    if (cleaned_ranges_with_tombstones.empty()) {
        return std::nullopt;
    }

    // Find the last interval that starts <= `base` (last_le_base). This will
    // be the interval that contains `base`, if it exists.
    auto first_gt_base = cleaned_ranges_with_tombstones.lower_bound(
      {.base_offset = kafka::next_offset(base)});
    if (first_gt_base == cleaned_ranges_with_tombstones.begin()) {
        return std::nullopt;
    }
    auto last_le_base = std::prev(first_gt_base);
    if (last_le_base->last_offset >= last) {
        return tombstone_range_iters{
          .begin = last_le_base,
          .last = last_le_base,
        };
    }
    auto it = last_le_base;

    // Keep track of where our contiguous ranges currently end.
    auto cur_tombstone_range_last = it->last_offset;
    // Keep track of where we expect the next range to start in order to be
    // contiguous.
    auto contiguous_next_offset = kafka::next_offset(last_le_base->last_offset);

    // Iterate forward, collecting the range only if it's contiguous.
    while (++it != cleaned_ranges_with_tombstones.end()) {
        if (it->base_offset != contiguous_next_offset) {
            break;
        }
        cur_tombstone_range_last = std::max(
          it->last_offset, cur_tombstone_range_last);
        contiguous_next_offset = kafka::next_offset(it->last_offset);
        if (cur_tombstone_range_last >= last) {
            return tombstone_range_iters{
              .begin = last_le_base,
              .last = it,
            };
        }
    }
    return std::nullopt;
}

compaction_state compaction_state::copy() const {
    compaction_state res;
    res.cleaned_ranges = cleaned_ranges;
    for (const auto& r : cleaned_ranges_with_tombstones) {
        res.cleaned_ranges_with_tombstones.emplace(r);
    }
    return res;
}

partition_state partition_state::copy() const {
    partition_state res{
      .extents = extents,
      .start_offset = start_offset,
      .next_offset = next_offset,
      .compaction_state = compaction_state
                            ? std::make_optional(compaction_state->copy())
                            : std::nullopt,
      .compaction_epoch = compaction_epoch,
      .term_starts = term_starts,
    };
    return res;
}

topic_state topic_state::copy() const {
    topic_state res;
    for (const auto& [p, s] : pid_to_state) {
        res.pid_to_state[p] = s.copy();
    }
    return res;
}

state state::copy() const {
    state res;
    for (const auto& [t, s] : topic_to_state) {
        res.topic_to_state[t] = s.copy();
    }
    for (const auto& [o, e] : objects) {
        res.objects[o] = e;
    }
    return res;
}

} // namespace cloud_topics::l1
