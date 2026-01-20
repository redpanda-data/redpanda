/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/sink.h"

#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/source.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "compaction/reducer.h"
#include "model/batch_compression.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

#include <exception>

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
  const chunked_vector<offset_interval_set::interval>& dirty_range_intervals,
  const offset_interval_set& removable_tombstone_ranges,
  metastore::compaction_epoch expected_compaction_epoch,
  kafka::offset start_offset,
  io* io,
  compaction_committer* committer,
  object_builder::options opts)
  : _tp(tp)
  , _dirty_range_intervals(dirty_range_intervals)
  , _removable_tombstone_ranges(removable_tombstone_ranges)
  , _expected_compaction_epoch(expected_compaction_epoch)
  , _start_offset(start_offset)
  , _io(io)
  , _committer(committer)
  , _opts(opts) {}

ss::future<bool>
compaction_sink::initialize(compaction::sliding_window_reducer::source& src) {
    auto& ct_src = static_cast<compaction_source&>(src);

    bool has_removable_tombstones = !_removable_tombstone_ranges.empty();
    bool has_dirty_ranges = !_dirty_range_intervals.empty();
    bool should_compact = has_removable_tombstones || has_dirty_ranges;

    if (!should_compact) {
        co_return false;
    }

    _job = co_await _committer->begin_compaction_job(_tp);

    auto& new_cleaned_ranges = ct_src._new_cleaned_ranges;
    new_cleaned_ranges.shrink_to_fit();
    _new_cleaned_ranges = std::move(new_cleaned_ranges);

    vlog(
      compaction_log.debug,
      "Built compaction map for tidp {}, job id {} with {} keys (max allowed "
      "{})",
      _tp,
      _job->id(),
      ct_src._map->size(),
      ct_src._map->capacity());

    co_return true;
}

ss::future<>
compaction_sink::initialize_builder(kafka::offset object_base_offset) {
    auto staging_file_fut = co_await ss::coroutine::as_future(
      _io->create_tmp_file());

    if (staging_file_fut.failed()) {
        auto e = staging_file_fut.get_exception();
        vlogl(
          compaction_log,
          ssx::is_shutdown_exception(e) ? ss::log_level::warn
                                        : ss::log_level::error,
          "Exception creating staging file: {}",
          e);
        std::rethrow_exception(e);
    }
    auto staging_file_result = staging_file_fut.get();

    auto active_staging_file = std::move(staging_file_result).value();
    auto output_stream = co_await active_staging_file->output_stream();

    auto builder = object_builder::create(std::move(output_stream), _opts);

    co_await builder->start_partition(_tp);

    _inflight_object = std::make_unique<compacted_object>(
      std::move(active_staging_file), std::move(builder), object_base_offset);
}

ss::future<> compaction_sink::flush(kafka::offset object_last_offset) {
    if (!_inflight_object) {
        co_return;
    }

    auto inflight_object = std::exchange(_inflight_object, nullptr);
    auto active_staging_file = std::exchange(
      inflight_object->active_staging_file, nullptr);
    auto builder = std::exchange(inflight_object->builder, nullptr);
    auto object_base_offset = inflight_object->object_base_offset;

    auto object_info_fut = co_await ss::coroutine::as_future(builder->finish());
    co_await builder->close();
    if (object_info_fut.failed()) {
        auto e = object_info_fut.get_exception();
        vlogl(
          compaction_log,
          ssx::is_shutdown_exception(e) ? ss::log_level::warn
                                        : ss::log_level::error,
          "Exception creating object_info: {}. Exiting compaction early.",
          e);
        co_await active_staging_file->remove();
        std::rethrow_exception(e);
    }

    auto object_info = object_info_fut.get();
    auto ntp_md = [this](
                    const object_builder::object_info& info,
                    kafka::offset object_base_offset,
                    kafka::offset object_last_offset) {
        auto [first, last] = info.index.partitions.equal_range(_tp);
        vassert(
          std::distance(first, last) == 1,
          "Expected one partition range in builder.");
        return metastore::object_metadata::ntp_metadata{
          .tidp = _tp,
          .base_offset = object_base_offset,
          .last_offset = object_last_offset,
          .max_timestamp = first->second.max_timestamp,
          .pos = first->second.file_position,
          .size = first->second.length};
    }(object_info, object_base_offset, object_last_offset);

    auto file_and_info = file_and_md_info{
      .staging_file = std::move(active_staging_file),
      .info = std::move(object_info),
      .ntp_md = std::move(ntp_md)};

    _job->add_l1_object(std::move(file_and_info));
}

ss::future<ss::stop_iteration>
compaction_sink::operator()(model::record_batch b, model::compression c) {
    auto next_offset = model::offset_cast(b.base_offset());
    auto prev_offset = kafka::prev_offset(next_offset);

    if (
      _inflight_object
      && _inflight_object->builder->file_size() >= max_object_size) {
        co_await flush(prev_offset);
    }

    if (!_inflight_object) {
        co_await initialize_builder(next_offset);
    }

    if (c != model::compression::none) {
        b = co_await model::compress_batch(c, std::move(b));
    }

    co_await _inflight_object->builder->add_batch(std::move(b));

    co_return ss::stop_iteration::no;
}

ss::future<>
compaction_sink::prepare_iteration(kafka::offset next_extent_base) {
    bool is_first_extent = _processed_extents.empty();
    if (!is_first_extent) {
        auto prev_extent_last_offset
          = _processed_extents.make_reverse_stream().next().last_offset;
        if (next_extent_base == kafka::next_offset(prev_extent_last_offset)) {
            co_return;
        }
        // Passed extents are non-contiguous. Force a roll of the
        // currently built L1 object with previous extent's last offset, and
        // start a new L1 object with the new extent's base offset.
        co_await flush(prev_extent_last_offset);
        // Intentional fallthrough.
    }
    co_await initialize_builder(next_extent_base);
}

ss::future<> compaction_sink::finish_iteration(
  kafka::offset prev_extent_base, kafka::offset prev_extent_last) {
    _processed_extents.insert(prev_extent_base, prev_extent_last);
    co_return;
}

ss::future<> compaction_sink::finalize() {
    if (!_job) {
        co_return;
    }

    std::exception_ptr eptr;
    try {
        if (_inflight_object) {
            if (!_processed_extents.empty()) {
                auto last_offset
                  = _processed_extents.make_reverse_stream().next().last_offset;
                co_await flush(last_offset);
            } else {
                // We started an object but didn't process any extents, which
                // means no meaningful work has been performed. Discard the
                // inflight object.
                auto inflight_object = std::exchange(_inflight_object, nullptr);
                auto active_staging_file = std::exchange(
                  inflight_object->active_staging_file, nullptr);
                auto builder = std::exchange(inflight_object->builder, nullptr);
                co_await active_staging_file->remove();
                co_await builder->close();
            }
        }
    } catch (...) {
        eptr = std::current_exception();
    }

    auto removed_tombstone_ranges = get_removed_tombstone_ranges(
      _removable_tombstone_ranges, _processed_extents);
    auto new_cleaned_ranges = get_new_cleaned_ranges(
      _new_cleaned_ranges, _processed_extents, _start_offset);

    auto id = _job->id();
    _job = nullptr;
    co_await _committer->finalize_compaction_job(
      id,
      std::move(new_cleaned_ranges),
      std::move(removed_tombstone_ranges),
      _expected_compaction_epoch);

    if (eptr) {
        std::rethrow_exception(eptr);
    }
}

} // namespace cloud_topics::l1
