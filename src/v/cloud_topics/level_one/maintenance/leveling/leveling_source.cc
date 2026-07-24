/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/leveling/leveling_source.h"

#include "cloud_io/admission_control_types.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader.h"
#include "cloud_topics/level_one/metastore/extent_metadata_reader.h"
#include "cloud_topics/log_reader_config.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

namespace {

/// A passthrough consumer that forwards batches directly to the sink
/// without decompression or use of a compaction::filter for deduplication.
struct passthrough_consumer {
    compaction::sliding_window_reducer::sink& sink;

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        co_return co_await sink(std::move(b));
    }

    void end_of_stream() {}
};

} // namespace

leveling_source::leveling_source(
  model::ntp ntp,
  model::topic_id_partition tp,
  chunked_vector<levelable_range> leveling_ranges,
  metastore* metastore,
  io* io,
  ss::abort_source& as,
  compaction_job_state& state,
  prefix_logger& ctxlog)
  : _ntp(std::move(ntp))
  , _tp(tp)
  , _leveling_ranges(std::move(leveling_ranges))
  , _range_it(_leveling_ranges.begin())
  , _metastore(metastore)
  , _io(io)
  , _as(as)
  , _state(state)
  , _ctxlog(ctxlog) {}

ss::future<> leveling_source::initialize() { co_return; }

ss::future<ss::stop_iteration> leveling_source::map_building_iteration() {
    // No map building needed for leveling — we keep all records.
    co_return ss::stop_iteration::yes;
}

ss::future<ss::stop_iteration> leveling_source::deduplication_iteration(
  compaction::sliding_window_reducer::sink& sink) {
    if (preempted()) {
        co_return ss::stop_iteration::yes;
    }

    if (_range_it == _leveling_ranges.end()) {
        co_return ss::stop_iteration::yes;
    }

    auto reader = extent_metadata_reader(
      _metastore,
      _tp,
      kafka::offset{_range_it->base_offset},
      kafka::offset{_range_it->last_offset},
      extent_metadata_reader::iteration_direction::forwards,
      _as);
    auto gen = reader.generator();
    while (auto extent_res_opt = co_await gen()) {
        auto& extent_res = extent_res_opt->get();
        if (!extent_res.has_value()) {
            vlog(
              _ctxlog.warn,
              "Error fetching extent metadata during leveling iteration: {}",
              extent_res.error());
            co_return ss::stop_iteration::yes;
        }

        auto extent = std::move(extent_res).value();

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
            config, _ntp, _tp, _metastore, _io));

        co_await sink.prepare_iteration(start_offset);
        co_await std::move(rdr).consume(
          passthrough_consumer{sink}, model::no_timeout);
        co_await sink.finish_iteration(start_offset, last_offset);

        vlog(
          _ctxlog.debug,
          "L1 leveling rewriting offset range ({}~{})",
          start_offset,
          last_offset);
    }

    ++_range_it;
    co_return ss::stop_iteration::no;
}

bool leveling_source::preempted() const {
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
