/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "container/fragmented_vector.h"
#include "datalake/coordinator/file_committer.h"
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "datalake/table_creator.h"
#include "model/fundamental.h"

#include <gtest/gtest.h>

#include <vector>

namespace datalake::coordinator {

class noop_table_creator : public table_creator {
    ss::future<checked<std::nullopt_t, errc>> ensure_table(
      const model::topic&,
      model::revision_id,
      record_schema_components) const final {
        co_return std::nullopt;
    }
};

// Simple committer that returns the set of updates that would mark all the
// pending files as committed. Doesn't affect any external state.
class simple_file_committer : public file_committer {
public:
    ss::future<checked<chunked_vector<mark_files_committed_update>, errc>>
    commit_topic_files_to_catalog(
      model::topic t, const topics_state& state) const override {
        chunked_vector<mark_files_committed_update> ret;
        auto t_iter = std::ranges::find(
          state.topic_to_state,
          t,
          &std::pair<model::topic, topic_state>::first);
        if (t_iter == state.topic_to_state.end()) {
            co_return ret;
        }
        // Mark the last file in each partition as committed.
        auto& t_state = t_iter->second;
        for (const auto& [pid, files] : t_state.pid_to_pending_files) {
            if (files.pending_entries.empty()) {
                continue;
            }
            model::topic_partition tp(t, pid);
            auto build_res = mark_files_committed_update::build(
              state,
              tp,
              t_state.revision,
              files.pending_entries.back().data.last_offset);
            EXPECT_FALSE(build_res.has_error());
            ret.emplace_back(std::move(build_res.value()));
        }
        co_return ret;
    }

    ss::future<checked<std::nullopt_t, errc>>
    drop_table(const model::topic&) const final {
        co_return std::nullopt;
    }

    ~simple_file_committer() override = default;
};

// Utility methods for generating and operating on coordinator state.

// Returns file entries corresponding to the given offset ranges.
//
// If with_file is true, the range will contain a data file, which may be
// useful when callers need more than just offset bounds (e.g. to test file
// deduplication).
chunked_vector<translated_offset_range> make_pending_files(
  const std::vector<std::pair<int64_t, int64_t>>& offset_bounds,
  bool with_file = false);

// Asserts that the given state has the expected partition state.
void check_partition(
  const topics_state& state,
  const model::topic_partition& tp,
  std::optional<int64_t> expected_committed,
  const std::vector<std::pair<int64_t, int64_t>>& offset_bounds);

} // namespace datalake::coordinator
