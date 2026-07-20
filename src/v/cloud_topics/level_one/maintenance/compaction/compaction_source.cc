/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/compaction/compaction_source.h"

#include "bytes/bytes.h"
#include "cloud_io/admission_control_types.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader.h"
#include "cloud_topics/level_one/maintenance/compaction/compaction_filter.h"
#include "cloud_topics/level_one/maintenance/logger.h"
#include "cloud_topics/level_one/metastore/extent_metadata_reader.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "cloud_topics/log_reader_config.h"
#include "compaction/key.h"
#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"
#include "model/batch_compression.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/record_utils.h"
#include "model/timeout_clock.h"
#include "utils/prefix_logger.h"

namespace cloud_topics::l1 {

namespace {

class map_building_reducer {
public:
    struct return_t {
        bool map_is_full;
        std::optional<model::offset> max_indexed_offset;
        bool range_has_tombstones;
    };

public:
    explicit map_building_reducer(
      compaction::key_offset_map& map, kafka::offset start_offset)
      : _map(map)
      , _start_offset(kafka::offset_cast(start_offset)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        if (b.compressed()) {
            b = co_await model::decompress_batch(b);
        }

        co_await b.for_each_record_key_async(
          [this, base_offset = b.base_offset()](
            model::record_key_metadata rec) -> ss::future<ss::stop_iteration> {
              if (rec.is_tombstone) {
                  _range_has_tombstones = true;
              }
              return maybe_index_record_in_map(
                rec.offset_delta, std::move(rec.key), base_offset);
          });

        if (_map_is_full) {
            co_return ss::stop_iteration::yes;
        }

        co_return ss::stop_iteration::no;
    }

    return_t end_of_stream() {
        return {_map_is_full, _max_indexed_offset, _range_has_tombstones};
    }

private:
    ss::future<ss::stop_iteration> maybe_index_record_in_map(
      int32_t offset_delta, bytes key_bytes, model::offset base_offset) {
        auto offset = base_offset + model::offset_delta(offset_delta);

        if (offset < _start_offset) {
            co_return ss::stop_iteration::no;
        }

        auto key = compaction::compaction_key{std::move(key_bytes)};
        bool inserted = co_await _map.put(key, offset);

        if (inserted) {
            _max_indexed_offset = model::offset(
              std::max(_max_indexed_offset.value_or(offset)(), offset()));
            co_return ss::stop_iteration::no;
        }

        _map_is_full = true;
        co_return ss::stop_iteration::yes;
    }

    compaction::key_offset_map& _map;
    model::offset _start_offset;

    bool _map_is_full{false};
    bool _range_has_tombstones{false};
    std::optional<model::offset> _max_indexed_offset{std::nullopt};
};

// Aligns the passed extent to the provided dirty range.
// For example, if the passed `dirty_range` is `[10,100]`, and some example
// `extents` are `[[0, 20], [21, 39], [40, 71], [72, 93], [94, 110]]`, the
// return value would be expected to be `[[10, 20], [21, 39], [40, 71], [72,
// 93], [94, 100]]` (respectively).
void align_extent_to_dirty_range(
  const offset_interval_set::interval& dirty_range,
  metastore::extent_metadata& extent) {
    extent.base_offset = kafka::offset{
      std::max(dirty_range.base_offset(), extent.base_offset())};
    extent.last_offset = kafka::offset{
      std::min(dirty_range.last_offset(), extent.last_offset())};
}

bool should_compact_extent(
  const metastore::extent_metadata& extent,
  std::chrono::milliseconds min_compaction_lag_ms) {
    const auto now = to_time_point(model::timestamp::now());
    const auto max_extent_ts = to_time_point(extent.max_timestamp);
    if (now - max_extent_ts < min_compaction_lag_ms) {
        return false;
    }
    return true;
}

} // namespace

compaction_source::compaction_source(
  model::ntp ntp,
  model::topic_id_partition tp,
  const chunked_vector<offset_interval_set::interval>& dirty_range_intervals,
  const offset_interval_set& removable_tombstone_ranges,
  kafka::offset start_offset,
  kafka::offset max_compactible_offset,
  compaction::key_offset_map* map,
  std::chrono::milliseconds min_compaction_lag_ms,
  metastore* metastore,
  io* io,
  ss::abort_source& as,
  compaction_job_state& state,
  compaction_worker_probe& probe,
  level_one_reader_probe* level_one_reader_probe,
  prefix_logger& ctxlog)
  : _ntp(std::move(ntp))
  , _tp(tp)
  , _dirty_range_intervals(dirty_range_intervals)
  , _removable_tombstone_ranges(removable_tombstone_ranges)
  , _start_offset(start_offset)
  , _max_compactible_offset(max_compactible_offset)
  , _dirty_range_it(_dirty_range_intervals.crbegin())
  , _extent_reader(
      metastore,
      tp,
      _start_offset,
      kafka::offset::max(),
      extent_metadata_reader::iteration_direction::forwards,
      as)
  , _extent_iterator(_extent_reader.generator())
  , _map(map)
  , _min_compaction_lag_ms(min_compaction_lag_ms)
  , _metastore(metastore)
  , _io(io)
  , _as(as)
  , _state(state)
  , _probe(probe)
  , _l1_reader_probe(level_one_reader_probe)
  , _ctxlog(ctxlog) {}

ss::future<> compaction_source::initialize() { co_return; }

ss::future<ss::stop_iteration> compaction_source::map_building_iteration() {
    if (preempted()) {
        co_return ss::stop_iteration::yes;
    }

    if (_dirty_range_it == _dirty_range_intervals.crend()) {
        co_return ss::stop_iteration::yes;
    }

    const auto& dirty_range = *_dirty_range_it;

    auto dirty_range_extent_reader = extent_metadata_reader(
      _metastore,
      _tp,
      dirty_range.base_offset,
      dirty_range.last_offset,
      extent_metadata_reader::iteration_direction::backwards,
      _as);
    auto gen = dirty_range_extent_reader.generator();
    bool map_is_full = false;
    // Worth noting that iteration over these extent aligned intervals is not
    // necessary for correctness- however, it provides a natural chunking of the
    // offset ranges for our reads, and also provides fine-grained intervals
    // over which we can indicate `has_tombstones` for the ranges.
    while (auto extent_res_opt = co_await gen()) {
        auto& extent_res = extent_res_opt->get();
        if (!extent_res.has_value()) {
            vlog(
              _ctxlog.warn,
              "Error fetching extent metadata during map building iteration: "
              "{}",
              extent_res.error());
            co_return ss::stop_iteration::yes;
        }

        auto extent = std::move(extent_res).value();

        align_extent_to_dirty_range(dirty_range, extent);
        const auto& start_offset = extent.base_offset;
        const auto& max_offset = extent.last_offset;

        cloud_topic_log_reader_config config(
          cloud_io::group_id::default_group,
          start_offset,
          max_offset,
          std::nullopt,
          _as);
        config.skip_cache = true;
        auto rdr = model::record_batch_reader(
          std::make_unique<level_one_log_reader_impl>(
            config, _ntp, _tp, _metastore, _io, _l1_reader_probe));

        auto res = co_await std::move(rdr).consume(
          map_building_reducer(*_map, start_offset), model::no_timeout);
        map_is_full = res.map_is_full;
        auto max_indexed_offset = res.max_indexed_offset;

        if (!map_is_full) {
            // The scan completed without the map filling up, implying it
            // read through the whole range. Regardless of whether we actually
            // indexed anything (we may not have, e.g. if the log was prefix
            // truncated, or if there's a gap caused by aborted transactions),
            // we should treat this range as clean upon completion.
            _new_cleaned_ranges.push_back(
              {.base_offset = start_offset,
               .last_offset = max_offset,
               .has_tombstones = res.range_has_tombstones});
        } else if (max_indexed_offset.has_value()) {
            // The map filled mid-range: only the prefix up to the last
            // indexed record is covered.
            auto last_offset = model::offset_cast(max_indexed_offset.value());
            dassert(
              start_offset <= last_offset,
              "Cleaned range must be properly bounded.");
            _new_cleaned_ranges.push_back(
              {.base_offset = start_offset,
               .last_offset = last_offset,
               .has_tombstones = res.range_has_tombstones});
        }
        // Otherwise the map filled before indexing anything in this range
        // (it was already at capacity on entry); nothing to clean.

        if (map_is_full) {
            co_return ss::stop_iteration::yes;
        }
    }

    ++_dirty_range_it;
    co_return ss::stop_iteration::no;
}

ss::future<ss::stop_iteration> compaction_source::deduplication_iteration(
  compaction::sliding_window_reducer::sink& sink) {
    if (preempted()) {
        co_return ss::stop_iteration::yes;
    }

    auto extent_res_opt = co_await _extent_iterator();
    if (!extent_res_opt.has_value()) {
        // We finished iterating.
        co_return ss::stop_iteration::yes;
    }

    auto& extent_res = extent_res_opt->get();

    if (!extent_res.has_value()) {
        // We received an error from the metastore.
        vlog(
          _ctxlog.warn,
          "Error fetching extent metadata during deduplication iteration: {}",
          extent_res.error());
        co_return ss::stop_iteration::yes;
    }

    auto extent = std::move(extent_res).value();

    if (extent.last_offset > _max_compactible_offset) {
        // We have iterated to an extent we cannot compact, stop compaction
        // here.
        co_return ss::stop_iteration::yes;
    }

    if (should_compact_extent(extent, _min_compaction_lag_ms)) {
        kafka::offset start_offset{extent.base_offset};
        kafka::offset last_offset{extent.last_offset};
        cloud_topic_log_reader_config config(
          cloud_io::group_id::default_group,
          start_offset,
          last_offset,
          std::nullopt,
          _as);
        config.skip_cache = true;
        auto rdr = model::record_batch_reader(
          std::make_unique<level_one_log_reader_impl>(
            config, _ntp, _tp, _metastore, _io, _l1_reader_probe));

        co_await sink.prepare_iteration(start_offset);
        auto stats = co_await std::move(rdr).consume(
          compaction_filter{sink, *_map, _ntp, _removable_tombstone_ranges},
          model::no_timeout);
        co_await sink.finish_iteration(start_offset, last_offset);
        if (stats.has_removed_data()) {
            vlog(
              _ctxlog.info,
              "L1 compaction removing data from offset range ({}~{}), stats: "
              "{}",
              start_offset,
              last_offset,
              stats);
        } else {
            vlog(
              _ctxlog.debug,
              "L1 compaction not removing data from offset range ({}~{}), "
              "stats: {}",
              start_offset,
              last_offset,
              stats);
        }

        _probe.add_stats(stats);
    }

    co_return ss::stop_iteration::no;
}

bool compaction_source::preempted() const {
    if (_as.abort_requested()) {
        return true;
    }

    if (
      _state == compaction_job_state::hard_stop
      || _state == compaction_job_state::soft_stop) {
        return true;
    }

    return false;
}

} // namespace cloud_topics::l1
