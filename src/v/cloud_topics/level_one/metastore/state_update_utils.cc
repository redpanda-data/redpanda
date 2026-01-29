/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/state_update_utils.h"

namespace cloud_topics::l1 {

std::expected<contiguous_intervals_by_tidp_t, ss::sstring>
contiguous_intervals_for_extents(
  const sorted_extents_by_tidp_t& sorted_extents_by_tp) {
    contiguous_intervals_by_tidp_t ret;
    for (const auto& [tidp, extents] : sorted_extents_by_tp) {
        auto& ret_intervals = ret[tidp];
        if (extents.empty()) {
            continue;
        }
        auto current_base = extents.begin()->base_offset;
        auto current_last = extents.begin()->last_offset;
        // Skip the first entry when iterating over `extents`.
        for (const auto& extent : extents | std::views::drop(1)) {
            auto expected_next = kafka::next_offset(current_last);
            if (extent.base_offset == expected_next) {
                // A new extent that is contiguous with the current interval.
                // Extend the current interval with the extent's last offset.
                current_last = extent.last_offset;
            } else if (extent.base_offset > expected_next) {
                // A new extent that is non-contiguous with the current
                // interval. Push back the current interval and start a new
                // interval with the extent's offset range.
                ret_intervals.push_back(
                  {.base_offset = current_base, .last_offset = current_last});
                current_base = extent.base_offset;
                current_last = extent.last_offset;
            } else {
                return std::unexpected(
                  fmt::format(
                    "Input object breaks partition {} offset ordering: "
                    "previous: {}, next: {}",
                    tidp,
                    current_last,
                    extent.base_offset));
            }
        }
        ret_intervals.push_back(
          {.base_offset = current_base, .last_offset = current_last});
    }

    return ret;
}

} // namespace cloud_topics::l1
