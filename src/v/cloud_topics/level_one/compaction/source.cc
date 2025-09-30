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
#include "cloud_topics/level_one/frontend_reader/reader.h"
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
    struct map_building_result {
        std::optional<kafka::offset> min_offset_indexed{std::nullopt};
        std::optional<kafka::offset> max_offset_indexed{std::nullopt};
        bool map_is_full{false};
    };

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

        if (_res.map_is_full) {
            co_return ss::stop_iteration::yes;
        }

        co_return ss::stop_iteration::no;
    }

    map_building_result end_of_stream() { return _res; }

private:
    ss::future<ss::stop_iteration> maybe_index_record_in_map(
      const model::record& r, model::offset base_offset) {
        auto offset = base_offset + model::offset_delta(r.offset_delta());

        auto key = compaction::compaction_key{iobuf_to_bytes(r.key())};
        bool inserted = co_await _map.put(key, offset);

        if (inserted) {
            // Assumes batches & records are read in a monotonically increasing
            // fashion, which is true for the current implementation using a
            // `level_one_log_reader_impl`. This may not be true in the future.
            auto ko = model::offset_cast(offset);
            if (!_res.min_offset_indexed.has_value()) {
                _res.min_offset_indexed = ko;
            }
            _res.max_offset_indexed = ko;
            co_return ss::stop_iteration::no;
        }

        _res.map_is_full = true;
        co_return ss::stop_iteration::yes;
    }

    compaction::key_offset_map& _map;
    map_building_result _res;
};

} // namespace

compaction_source::compaction_source(
  model::ntp ntp,
  model::topic_id_partition tp,
  metastore::compaction_offsets_response compaction_offsets,
  compaction::key_offset_map* map,
  metastore* metastore,
  io* io,
  ss::abort_source& as,
  compaction_job_state& state)
  : _ntp(std::move(ntp))
  , _tp(tp)
  , _dirty_ranges(std::move(compaction_offsets.dirty_ranges))
  , _removable_tombstone_ranges(
      std::move(compaction_offsets.removable_tombstone_ranges))
  , _map(map)
  , _metastore(metastore)
  , _io(io)
  , _as(as)
  , _state(state)
  , _dirty_range_intervals(_dirty_ranges.to_vec()) {}

ss::future<> compaction_source::initialize() {
    _map_building_it = _dirty_range_intervals.cbegin();
    co_return;
}

ss::future<ss::stop_iteration> compaction_source::map_building_iteration() {
    if (preempted()) {
        co_return ss::stop_iteration::yes;
    }

    if (_map_building_it == _dirty_range_intervals.cend()) {
        co_return ss::stop_iteration::yes;
    }

    const auto& dirty_range = *_map_building_it;
    const auto& start_offset = dirty_range.base_offset;
    const auto& max_offset = dirty_range.last_offset;

    cloud_topic_log_reader_config config(start_offset, max_offset, _as);
    auto rdr = model::record_batch_reader(
      std::make_unique<level_one_log_reader_impl>(
        config, _ntp, _tp, _metastore, _io));

    auto res = co_await std::move(rdr).consume(
      map_building_reducer(*_map), model::no_timeout);

    std::optional<kafka::offset> min_offset_indexed;
    std::optional<kafka::offset> max_offset_indexed;
    ss::stop_iteration ret;

    if (res.map_is_full) {
        // Rely on the result from the reducer to set min/max offset indexed.
        // Stop iterating since map is full.
        min_offset_indexed = res.min_offset_indexed;
        max_offset_indexed = res.max_offset_indexed;
        ret = ss::stop_iteration::yes;
    } else {
        // The entire range was indexed. We can continue iterating until the map
        // is full or we are preempted.
        min_offset_indexed = start_offset;
        max_offset_indexed = max_offset;
        ret = ss::stop_iteration::no;
    }

    if (min_offset_indexed.has_value() && max_offset_indexed.has_value()) {
        _indexed_intervals.insert(
          min_offset_indexed.value(), max_offset_indexed.value());
    }

    ++_map_building_it;
    co_return ret;
}

ss::future<ss::stop_iteration> compaction_source::deduplication_iteration(
  compaction::sliding_window_reducer::sink& sink) {
    if (preempted()) {
        co_return ss::stop_iteration::yes;
    }

    if (_indexed_intervals.empty()) {
        // We indexed nothing.
        co_return ss::stop_iteration::yes;
    }

    // TODO: Find a way to chunk this work into iterations.
    // An easy way to do this is to go from [start_offset,
    // next_dirty_interval.last_offset], and so on.
    auto indexed_intervals = _indexed_intervals.to_vec();
    kafka::offset start_offset{0};
    kafka::offset max_offset{indexed_intervals.back().last_offset};
    cloud_topic_log_reader_config config(start_offset, max_offset, _as);
    auto rdr = model::record_batch_reader(
      std::make_unique<level_one_log_reader_impl>(
        config, _ntp, _tp, _metastore, _io));

    auto stats = co_await rdr.consume(
      compaction_filter{sink, *_map, _ntp}, model::no_timeout);
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

    co_return ss::stop_iteration::yes;
}

bool compaction_source::preempted() const {
    if (_as.abort_requested()) {
        return true;
    }

    if (_state == compaction_job_state::stopped) {
        return true;
    }

    return false;
}

} // namespace cloud_topics::l1
