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
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "model/fundamental.h"

#include <gtest/gtest.h>

#include <vector>

namespace datalake::coordinator {

// Utility methods for generating and operating on coordinator state.

// Returns file entries corresponding to the given offset ranges.
chunked_vector<translated_offset_range> make_pending_files(
  const std::vector<std::pair<int64_t, int64_t>>& offset_bounds);

// Asserts that the given state has the expected partition state.
void check_partition(
  const topics_state& state,
  const model::topic_partition& tp,
  std::optional<int64_t> expected_committed,
  const std::vector<std::pair<int64_t, int64_t>>& offset_bounds);

} // namespace datalake::coordinator
