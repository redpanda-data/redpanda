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

#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"

#include <expected>

namespace cloud_topics::l1 {

using contiguous_intervals_by_tidp_t = chunked_hash_map<
  model::topic_id_partition,
  chunked_vector<offset_interval_set::interval>>;
using sorted_extents_by_tidp_t
  = chunked_hash_map<model::topic_id_partition, absl::btree_multiset<extent>>;

// Returns a vector of contiguous offset intervals found in
// `sorted_extents_by_tp` or an `stm_update_error` if the offsets of the
// provided extent are invalid. That is, the extents `[[0, 99], [100,199],
// [200,299], [300,399]]` are combined into the single continuous interval
// `[0,399]`, whereas the extents `[[0, 99], [100,199], [250,299], [300,399]]`
// would return two continuous intervals `[[0,199], [250,399]]`. An example of
// an invalid extent input would be `[[0,99], [200, 249], [239, 299]]`.
std::expected<contiguous_intervals_by_tidp_t, ss::sstring>
contiguous_intervals_for_extents(
  const sorted_extents_by_tidp_t& sorted_extents_by_tp);

} // namespace cloud_topics::l1
