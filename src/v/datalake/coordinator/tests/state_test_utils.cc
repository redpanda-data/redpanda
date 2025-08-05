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

#include "random/generators.h"

namespace datalake::coordinator {

chunked_vector<translated_offset_range> make_pending_files(
  const std::vector<std::pair<int64_t, int64_t>>& offset_bounds,
  bool with_file,
  bool dlq) {
    chunked_vector<translated_offset_range> files;
    files.reserve(offset_bounds.size());
    for (const auto& [begin, end] : offset_bounds) {
        chunked_vector<data_file> fs;
        if (with_file) {
            fs.emplace_back(data_file{
              .remote_path = fmt::format("{}-{}", begin, end),
              .file_size_bytes = 1024,
              .table_schema_id = 0,
              .partition_spec_id = 0,
              .partition_key = chunked_vector<std::optional<bytes>>::single(
                std::nullopt),
            });
        }
        auto total_bytes = (end - begin + 1)
                           * random_generators::get_int<uint64_t>(
                             100, 1000); // Simulate some bytes processed.
        if (dlq) {
            files.emplace_back(translated_offset_range{
              .start_offset = kafka::offset{begin},
              .last_offset = kafka::offset{end},
              .dlq_files = std::move(fs),
              .kafka_bytes_processed = total_bytes,
              // Other args irrelevant.
            });
        } else {
            files.emplace_back(translated_offset_range{
              .start_offset = kafka::offset{begin},
              .last_offset = kafka::offset{end},
              .files = std::move(fs),
              .kafka_bytes_processed = total_bytes,
              // Other args irrelevant.
            });
        }
    }
    return files;
}

void add_partition_state(
  std::vector<pairs_t> offset_bounds_by_pid,
  topic_state& state,
  model::offset added_at,
  bool with_files,
  bool dlq) {
    for (size_t i = 0; i < offset_bounds_by_pid.size(); i++) {
        auto pid = static_cast<model::partition_id>(i);
        auto& p_state = state.pid_to_pending_files[pid];
        for (auto& f :
             make_pending_files(offset_bounds_by_pid[i], with_files, dlq)) {
            p_state.pending_entries.emplace_back(pending_entry{
              .data = std::move(f), .added_pending_at = added_at});
        }
    }
}
topic_state make_topic_state(
  std::vector<pairs_t> offset_bounds_by_pid,
  model::offset added_at,
  bool with_files,
  bool dlq) {
    topic_state state;
    add_partition_state(
      std::move(offset_bounds_by_pid), state, added_at, with_files, dlq);
    return state;
}

void check_partition(
  const topics_state& state,
  const model::topic_partition& tp,
  std::optional<int64_t> expected_committed,
  const pairs_t& offset_bounds) {
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
        EXPECT_EQ(
          prt_state.pending_entries.at(i).data.start_offset, expected_begin);
        EXPECT_EQ(
          prt_state.pending_entries.at(i).data.last_offset, expected_end);
    }
}

} // namespace datalake::coordinator
