/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/source.h"

#include "cloud_topics/level_one/compaction/filter.h"
#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "cloud_topics/log_reader_config.h"
#include "compaction/key.h"
#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"
#include "model/batch_compression.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

namespace {

class map_building_reducer {
public:
    explicit map_building_reducer(compaction::key_offset_map& map)
      : _map(map) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        if (b.compressed()) {
            b = co_await model::decompress_batch(b);
        }

        co_await b.for_each_record_async(
          [this, base_offset = b.base_offset()](
            const model::record& r) -> ss::future<ss::stop_iteration> {
              return maybe_index_record_in_map(r, base_offset);
          });

        if (_map_is_full) {
            co_return ss::stop_iteration::yes;
        }

        co_return ss::stop_iteration::no;
    }

    bool end_of_stream() { return _map_is_full; }

private:
    ss::future<ss::stop_iteration> maybe_index_record_in_map(
      const model::record& r, model::offset base_offset) {
        auto offset = base_offset + model::offset_delta(r.offset_delta());

        auto key = compaction::compaction_key{iobuf_to_bytes(r.key())};
        bool inserted = co_await _map.put(key, offset);

        if (inserted) {
            co_return ss::stop_iteration::no;
        }

        _map_is_full = true;
        co_return ss::stop_iteration::yes;
    }

    compaction::key_offset_map& _map;
    bool _map_is_full{false};
};

} // namespace

compaction_source::compaction_source(
  model::ntp ntp,
  model::topic_id_partition tp,
  const chunked_vector<offset_interval_set::interval>& dirty_range_intervals,
  const offset_interval_set& removable_tombstone_ranges,
  metastore::extent_offsets_t extents,
  compaction::key_offset_map* map,
  metastore* metastore,
  io* io,
  ss::abort_source& as,
  compaction_job_state& state)
  : _ntp(std::move(ntp))
  , _tp(tp)
  , _dirty_range_intervals(dirty_range_intervals)
  , _removable_tombstone_ranges(removable_tombstone_ranges)
  , _extents(std::move(extents))
  , _map(map)
  , _metastore(metastore)
  , _io(io)
  , _as(as)
  , _state(state) {}

ss::future<> compaction_source::initialize() {
    _dirty_range_it = _dirty_range_intervals.cbegin();
    _extents_it = _extents.cbegin();
    _extents_end_it = _extents.cend();
    co_return;
}

ss::future<ss::stop_iteration> compaction_source::map_building_iteration() {
    if (preempted()) {
        co_return ss::stop_iteration::yes;
    }

    if (_dirty_range_it == _dirty_range_intervals.cend()) {
        co_return ss::stop_iteration::yes;
    }

    const auto& dirty_range = *_dirty_range_it;
    const auto& start_offset = dirty_range.base_offset;
    const auto& max_offset = dirty_range.last_offset;

    cloud_topic_log_reader_config config(start_offset, max_offset, _as);
    auto rdr = model::record_batch_reader(
      std::make_unique<level_one_log_reader_impl>(
        config, _ntp, _tp, _metastore, _io));

    auto map_is_full = co_await std::move(rdr).consume(
      map_building_reducer(*_map), model::no_timeout);

    if (map_is_full) {
        co_return ss::stop_iteration::yes;
    }

    ++_dirty_range_it;
    co_return ss::stop_iteration::no;
}

ss::future<ss::stop_iteration> compaction_source::deduplication_iteration(
  compaction::sliding_window_reducer::sink& sink) {
    auto& ct_sink = static_cast<compaction_sink&>(sink);
    if (preempted()) {
        co_return ss::stop_iteration::yes;
    }

    if (_extents_it == _extents_end_it) {
        co_return ss::stop_iteration::yes;
    }

    _extent_base_offset = _extents_it->base_offset;

    auto max_indexed_offset = model::offset_cast(_map->max_offset());
    // If we have iterated to a portion of the log which is higher than the
    // maximum indexed offset, we cannot perform any meaningful de-duplication.
    auto can_deduplicate = max_indexed_offset >= _extents_it->last_offset;
    // If we have iterated to a portion of the log which is higher than the last
    // removable tombstone, there is no longer any meaningful tombstone removal
    // work to do.
    auto can_remove_tombstones
      = !_removable_tombstone_ranges.empty()
        && _removable_tombstone_ranges.to_vec().back().last_offset
             >= _extents_it->last_offset;

    if (!can_deduplicate && !can_remove_tombstones) {
        co_return ss::stop_iteration::yes;
    }

    kafka::offset start_offset{_extents_it->base_offset};
    kafka::offset max_offset{_extents_it->last_offset};
    cloud_topic_log_reader_config config(start_offset, max_offset, _as);
    auto rdr = model::record_batch_reader(
      std::make_unique<level_one_log_reader_impl>(
        config, _ntp, _tp, _metastore, _io));

    auto stats = co_await rdr.consume(
      compaction_filter{
        ct_sink,
        *_map,
        _ntp,
        _removable_tombstone_ranges,
        start_offset,
        max_offset},
      model::no_timeout);
    if (stats.has_removed_data()) {
        vlog(
          compaction_log.info,
          "L1 compaction removing data from CTP {}, stats: {}",
          _ntp,
          stats);
    } else {
        vlog(
          compaction_log.info,
          "L1 compaction not removing data from CTP {}, stats: {}",
          _ntp,
          stats);
    }

    vlog(
      compaction_log.info,
      "Read from offset {} to offset {}",
      _extents_it->base_offset,
      _extents_it->last_offset);

    _extent_last_offset = _extents_it->last_offset;

    ++_extents_it;

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
