/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/tests/state_test_utils.h"

namespace datalake::coordinator {

chunked_vector<translated_offset_range> make_pending_files(
  const std::vector<std::pair<int64_t, int64_t>>& offset_bounds) {
    chunked_vector<translated_offset_range> files;
    files.reserve(offset_bounds.size());
    for (const auto& [begin, end] : offset_bounds) {
        files.emplace_back(translated_offset_range{
          .start_offset = kafka::offset{begin},
          .last_offset = kafka::offset{end},
          // Other args irrelevant.
        });
    }
    return files;
}

void check_partition(
  const topics_state& state,
  const model::topic_partition& tp,
  std::optional<int64_t> expected_committed,
  const std::vector<std::pair<int64_t, int64_t>>& offset_bounds) {
    auto prt_state_opt = state.partition_state(tp);
    ASSERT_TRUE(prt_state_opt.has_value());
    const auto& prt_state = prt_state_opt.value().get();
    if (expected_committed.has_value()) {
        ASSERT_TRUE(prt_state.last_committed.has_value());
        EXPECT_EQ(prt_state.last_committed.value()(), *expected_committed);
    } else {
        EXPECT_FALSE(prt_state.last_committed.has_value());
    }

    ASSERT_EQ(prt_state.pending_entries.size(), offset_bounds.size());
    for (size_t i = 0; i < offset_bounds.size(); ++i) {
        auto expected_begin = offset_bounds[i].first;
        auto expected_end = offset_bounds[i].second;
        EXPECT_EQ(prt_state.pending_entries.at(i).start_offset, expected_begin);
        EXPECT_EQ(prt_state.pending_entries.at(i).last_offset, expected_end);
    }
}

} // namespace datalake::coordinator
