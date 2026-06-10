/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/compaction/compaction_sink.h"

#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/maintenance/compaction/compaction_source.h"
#include "cloud_topics/level_one/maintenance/l1_object_sink.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "cloud_topics/level_one/metastore/retry.h"
#include "compaction/reducer.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "utils/prefix_logger.h"

namespace cloud_topics::l1 {

namespace {

// Computes the ranges that may be marked as having all their tombstones
// removed, based on the `metastore`'s initial `removable_tombstone_ranges`
// response, and the extents that were processed by the `sink`. The returned
// `offset_interval_set` will be used for the compaction update to the
// `metastore`.
offset_interval_set get_removed_tombstone_ranges(
  const offset_interval_set& removable_tombstone_ranges,
  const offset_interval_set& processed_extents) {
    offset_interval_set removed_tombstone_ranges;
    auto stream = removable_tombstone_ranges.make_stream();
    while (stream.has_next()) {
        auto i = stream.next();
        if (processed_extents.covers(i.base_offset, i.last_offset)) {
            removed_tombstone_ranges.insert(i.base_offset, i.last_offset);
        }
    }
    return removed_tombstone_ranges;
}

// Computes the ranges that may be marked as clean, based on the dirty ranges
// that were processed by the `compaction_source`, and the extents that were
// processed by the `sink`. The returned `chunked_vector` of `cleaned_ranges`
// will be used for the compaction update to the `metastore`.
chunked_vector<metastore::compaction_update::cleaned_range>
get_new_cleaned_ranges(
  const chunked_vector<metastore::compaction_update::cleaned_range>&
    maybe_cleaned_ranges,
  const offset_interval_set& processed_extents,
  kafka::offset start_offset) {
    chunked_vector<metastore::compaction_update::cleaned_range>
      new_cleaned_ranges;
    new_cleaned_ranges.reserve(maybe_cleaned_ranges.size());
    for (const auto& cleaned_range : maybe_cleaned_ranges) {
        if (processed_extents.covers(start_offset, cleaned_range.last_offset)) {
            new_cleaned_ranges.push_back(cleaned_range);
        }
    }

    new_cleaned_ranges.shrink_to_fit();
    return new_cleaned_ranges;
}

} // namespace

compaction_sink::compaction_sink(
  model::topic_id_partition tp,
  model::ntp ntp,
  const chunked_vector<offset_interval_set::interval>& dirty_range_intervals,
  const offset_interval_set& removable_tombstone_ranges,
  metastore::compaction_epoch expected_compaction_epoch,
  kafka::offset start_offset,
  io* io,
  metastore* metastore,
  ss::abort_source& as,
  config::binding<size_t> max_object_size,
  size_t upload_part_size,
  prefix_logger& ctxlog,
  object_builder::options opts,
  cloud_topics::level_zero_notifier* notifier)
  : l1_object_sink(
      std::move(tp),
      io,
      metastore,
      as,
      std::move(max_object_size),
      upload_part_size,
      ctxlog,
      std::move(opts))
  , _dirty_range_intervals(dirty_range_intervals)
  , _removable_tombstone_ranges(removable_tombstone_ranges)
  , _expected_compaction_epoch(expected_compaction_epoch)
  , _start_offset(start_offset)
  , _ntp(std::move(ntp))
  , _notifier(notifier) {}

ss::future<bool>
compaction_sink::initialize(compaction::sliding_window_reducer::source& src) {
    auto& ct_src = static_cast<compaction_source&>(src);

    bool has_removable_tombstones = !_removable_tombstone_ranges.empty();
    bool has_dirty_ranges = !_dirty_range_intervals.empty();
    bool should_compact = has_removable_tombstones || has_dirty_ranges;

    if (!should_compact) {
        co_return false;
    }

    co_await init_metadata_builder();

    auto& new_cleaned_ranges = ct_src._new_cleaned_ranges;
    new_cleaned_ranges.shrink_to_fit();
    _new_cleaned_ranges = std::move(new_cleaned_ranges);

    vlog(
      _ctxlog.debug,
      "Built compaction map with {} keys (max allowed {})",
      ct_src._map->size(),
      ct_src._map->capacity());

    co_return true;
}

ss::future<ss::stop_iteration>
compaction_sink::operator()(model::record_batch b) {
    auto next_offset = model::offset_cast(b.base_offset());
    auto prev_offset = kafka::prev_offset(next_offset);

    if (
      _inflight_object
      && _inflight_object->builder->file_size() >= _max_object_size()) {
        co_await flush(prev_offset);
    }

    if (!_inflight_object) {
        co_await initialize_builder(next_offset);
    }

    co_await _inflight_object->builder->add_batch(std::move(b));

    co_return ss::stop_iteration::no;
}

ss::future<std::expected<void, metastore::errc>>
compaction_sink::do_compact_objects(metastore::compaction_map_t compact_map) {
    co_return co_await l1::retry_metastore_op_with_default_rtc(
      [this, &compact_map]() {
          return _metastore->compact_objects(*_metadata_builder, compact_map);
      },
      _as);
}

ss::future<> compaction_sink::compact_objects_without_update() {
    auto compaction_update = metastore::compaction_update{
      .new_cleaned_ranges = {},
      .removed_tombstones_ranges = {},
      .cleaned_at = model::timestamp::missing(),
      .expected_compaction_epoch = _expected_compaction_epoch};

    metastore::compaction_map_t compact_map;
    compact_map.emplace(_tp, std::move(compaction_update));
    auto replace_res = co_await do_compact_objects(std::move(compact_map));
    if (replace_res.has_value()) {
        vlog(
          _ctxlog.info, "Finalized job without a compaction metadata update");
    } else {
        vlog(_ctxlog.warn, "Could not finalize job: {}.", replace_res.error());
    }
}

ss::future<> compaction_sink::compact_objects_with_update(
  chunked_vector<metastore::compaction_update::cleaned_range>
    new_cleaned_ranges,
  offset_interval_set removed_tombstone_ranges) {
    auto compaction_update = metastore::compaction_update{
      .new_cleaned_ranges = std::move(new_cleaned_ranges),
      .removed_tombstones_ranges = std::move(removed_tombstone_ranges),
      .cleaned_at = model::timestamp::now(),
      .expected_compaction_epoch = _expected_compaction_epoch};

    auto compaction_update_str = fmt::format("{}", compaction_update);
    metastore::compaction_map_t compact_map;
    compact_map.emplace(_tp, std::move(compaction_update));
    auto commit_res = co_await do_compact_objects(std::move(compact_map));

    if (commit_res.has_value()) {
        vlog(
          _ctxlog.info,
          "Finalized job with compaction metadata update: {}",
          compaction_update_str);
    } else {
        vlog(
          _ctxlog.warn,
          "Could not finalize job with compaction metadata update {}: {}. "
          "Retrying object update without metadata.",
          compaction_update_str,
          commit_res.error());
        co_return co_await compact_objects_without_update();
    }
}

ss::future<> compaction_sink::finalize(bool success) {
    auto should_commit = co_await finalize_inflight(success);
    if (!should_commit) {
        co_return;
    }

    if (_any_object_failed) {
        co_await compact_objects_without_update();
    } else {
        auto removed_tombstone_ranges = get_removed_tombstone_ranges(
          _removable_tombstone_ranges, _processed_extents);
        auto new_cleaned_ranges = get_new_cleaned_ranges(
          _new_cleaned_ranges, _processed_extents, _start_offset);

        // Compute the new min_allowed_local_threshold floor for this
        // partition: the exclusive lower bound for local reads. The floor is
        // the max last_offset across new cleaned ranges, plus one.
        std::optional<kafka::offset> max_cleaned_last_offset;
        for (const auto& r : new_cleaned_ranges) {
            if (
              !max_cleaned_last_offset.has_value()
              || r.last_offset > *max_cleaned_last_offset) {
                max_cleaned_last_offset = r.last_offset;
            }
        }

        if (max_cleaned_last_offset.has_value() && _notifier != nullptr) {
            auto new_floor = kafka::next_offset(*max_cleaned_last_offset);

            // If tombstones were removed at or above the new floor we must
            // advance the floor before committing the removal: otherwise local
            // reads in [new_floor, removed-tombstone-range] could still serve
            // records that compaction has just deleted in L1. Below the floor
            // the removal is safe regardless, so a failed advance only blocks
            // the commit in the former case.
            bool tombstones_removed_above_floor = false;
            auto stream = removed_tombstone_ranges.make_stream();
            while (stream.has_next()) {
                if (stream.next().last_offset >= new_floor) {
                    tombstones_removed_above_floor = true;
                    break;
                }
            }

            vlog(
              _ctxlog.info,
              "[{}] compaction advancing min_allowed_local_threshold to {}",
              _tp,
              new_floor);
            auto res = co_await _notifier->set_min_allowed_local_threshold(
              _ntp, new_floor);
            if (!res.has_value()) {
                if (tombstones_removed_above_floor) {
                    vlog(
                      _ctxlog.warn,
                      "[{}] skipping compaction commit: failed to advance "
                      "min_allowed_local_threshold to {} ({}) while tombstones "
                      "were removed above it",
                      _tp,
                      new_floor,
                      res.error());
                    co_return;
                }
                vlog(
                  _ctxlog.warn,
                  "[{}] failed to advance min_allowed_local_threshold to {} "
                  "({}); committing compaction anyway",
                  _tp,
                  new_floor,
                  res.error());
            }
        }

        co_await compact_objects_with_update(
          std::move(new_cleaned_ranges), std::move(removed_tombstone_ranges));
    }
}

} // namespace cloud_topics::l1
