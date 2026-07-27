/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/compaction/compaction_filter.h"

#include "compaction/key.h"
#include "compaction/utils.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_utils.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <optional>
#include <vector>

namespace cloud_topics::l1 {

compaction_filter::compaction_filter(
  compaction::sliding_window_reducer::sink& sink,
  const compaction::key_offset_map& map,
  model::ntp ntp,
  const offset_interval_set& removable_tombstone_ranges)
  : filter(sink, std::move(ntp))
  , _map(map)
  , _removable_tombstone_ranges(removable_tombstone_ranges) {}

ss::future<bool> compaction_filter::should_keep(
  model::offset o,
  bool is_tombstone,
  const compaction::compaction_key& key) const {
    if (is_tombstone) {
        if (_removable_tombstone_ranges.contains(model::offset_cast(o))) {
            ++_stats.expired_tombstones_discarded;
            co_return false;
        }
    }

    co_return co_await compaction::is_latest_record_for_key(_map, o, key);
}

ss::future<> compaction_filter::maybe_index_offset_delta(
  const model::record_batch& b,
  model::record_key_metadata rec,
  chunked_vector<int32_t>& offset_deltas) const {
    const auto o = b.base_offset() + model::offset_delta(rec.offset_delta);
    if (
      co_await should_keep(
        o, rec.is_tombstone, compaction::compaction_key{std::move(rec.key)})) {
        offset_deltas.push_back(rec.offset_delta);
    }
}

ss::future<chunked_vector<int32_t>>
compaction_filter::compute_offset_deltas_to_keep(
  const model::record_batch& b) const {
    chunked_vector<int32_t> offset_deltas;
    offset_deltas.reserve(b.record_count());

    co_await b.for_each_record_key_async(
      [this, &b, &offset_deltas](model::record_key_metadata rec) {
          return maybe_index_offset_delta(b, std::move(rec), offset_deltas);
      });

    co_return offset_deltas;
}

ss::future<std::optional<compaction::filter::filtered_batch>>
compaction_filter::filter_batch_with_offset_deltas(
  model::record_batch b, chunked_vector<int32_t> offset_deltas) const {
    co_return co_await do_filter_batch(std::move(b), std::move(offset_deltas));
}

} // namespace cloud_topics::l1
