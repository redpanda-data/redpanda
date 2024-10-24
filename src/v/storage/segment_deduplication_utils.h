// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "model/fundamental.h"
#include "storage/fwd.h"
#include "storage/index_state.h"
#include "storage/segment_set.h"

namespace storage {
using segment_list_t = fragmented_vector<segment_set::type>;
class stm_manager;

// Adds the keys from the given compacted index reader to the map. Returns
// true if the entire reader was successfully indexed, false if the index was
// full before reaching the end of the segment.
ss::future<bool> build_offset_map_for_segment(
  const compaction_config& cfg, const segment& seg, key_offset_map& m);

// Builds a map from key to latest offset from the last segment to the
// earliest segment in 'segs'.
//
// Returns the start offset of the earliest segment that was fully indexed.
// The resulting map may contain offsets below this point (i.e. the
// preceding segment may have been partially indexed), but to fully
// deduplicate the log, subsequent compactions should start below this
// offset.
//
// In the event a compacted index is missing or corrupted (e.g. if the segment
// was partially truncated), attempts to rebuild it and proceeds with building
// the map building.
//
// Throws an exception if there was a problem building the map or if the map
// couldn't build a single segment.
ss::future<model::offset> build_offset_map(
  const compaction_config& cfg,
  const segment_set& segs,
  ss::lw_shared_ptr<storage::stm_manager> stm_manager,
  storage::storage_resources&,
  storage::probe&,
  key_offset_map&);

// Rewrites 'seg' according to the parameters in 'cfg' to 'appender' and
// 'cmp_idx_writer', deduplicating with latest offsets per key from 'map'.
ss::future<index_state> deduplicate_segment(
  const compaction_config& cfg,
  const key_offset_map& map,
  ss::lw_shared_ptr<storage::segment> seg,
  segment_appender& appender,
  compacted_index_writer& cmp_idx_writer,
  storage::probe& probe,
  offset_delta_time should_offset_delta_times,
  ss::sharded<features::feature_table>&,
  bool inject_reader_failure = false);

} // namespace storage
