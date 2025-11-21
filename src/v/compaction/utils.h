// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction/key_offset_map.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_types.h"

#include <seastar/core/sharded.hh>

namespace compaction {

bool is_tx_batch_compaction_enabled(
  ss::sharded<features::feature_table>& feature_table);

// Returns `true` if the provided `record_batch_type` is a control batch that is
// immediately removable during compaction. This means we do not need to
// consider either deduplication (compactible) or `delete.retention.ms` (a form
// of filtering) to decide if these batches can be removed. They can be
// indiscriminately removed when they are seen.
bool is_removable_control_batch(
  const model::ntp& ntp,
  const model::record_batch_type batch_type,
  bool remove_user_tx_fence_enabled);

// Returns `true` or `false` indicating whether the batch type of the header
// passed contains records that may be removed by compaction- whether that is by
// de-duplication, or by other logic, i.e we need to consider transactional
// batch removal past `delete.retention.ms`. Batches which should NEVER be
// removed from the log via compaction filtering include compaction placeholder
// batches, as well as all batches that shift offset translation. These batches
// shouldn't even be considered in `should_keep()`, they should be kept
// unconditionally.
bool is_filterable(model::record_batch_type t);

// Returns `true` or `false` indicating whether the batch type of the header
// passed contains records that may be de-duplicated by compaction
// (de-duplication refers to the removal of records performed _only_ due to
// records with the same key existing for a later record in the log). This is a
// more stringent condition than `is_filterable()`- this condition determines
// whether records from a batch should be indexed in a .compaction_index file,
// and whether only the latest record for a given key should be kept in
// `should_keep()`.
bool is_compactible(const model::record_batch_header& h);

// Creates a placeholder batch type with the same header properties (type, base
// offset, timestamps, etc) as the provided `record_batch_header`.
model::record_batch make_placeholder_batch(model::record_batch_header& hdr);

// Returns `true` if the provided record has an offset greater than or equal to
// the latest offset indexed for that key within the provided map, or if the map
// does not have an entry for that key, and `false` otherwise.
ss::future<bool> is_latest_record_for_key(
  const key_offset_map& map,
  const model::record_batch& b,
  const model::record& r);

// A log is eligible for compaction if at least one of the following
// is true:
// 1. It's dirty enough: the dirty ratio is at least the minimum
//    cleanable dirty ratio.
// 2. It's gone long enough without compaction: the earliest first
//    batch timestamp of a dirty segment is longer ago than
//    the max compaction lag.
bool log_needs_compaction(
  double dirty_ratio,
  double min_cleanable_dirty_ratio,
  std::optional<model::timestamp> earliest_dirty_ts,
  std::chrono::milliseconds max_lag);

} // namespace compaction
