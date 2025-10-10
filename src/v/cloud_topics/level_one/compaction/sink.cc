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

compaction_sink::compaction_sink(
  model::topic_id_partition tp,
  const chunked_vector<offset_interval_set::interval>& dirty_range_intervals,
  const offset_interval_set& removable_tombstone_ranges,
  compaction::key_offset_map* map,
  io* io,
  compaction_committer* committer,
  object_builder::options opts)
  : _tp(tp)
  , _dirty_range_intervals(dirty_range_intervals)
  , _removable_tombstone_ranges(removable_tombstone_ranges)
  , _map(map)
  , _io(io)
  , _committer(committer)
  , _opts(opts) {}

ss::future<>
compaction_sink::initialize(compaction::sliding_window_reducer::source& src) {
    auto& ct_src = static_cast<compaction_source&>(src);
    _extent_base_offset = &ct_src._extent_base_offset;
    _extent_last_offset = &ct_src._extent_last_offset;
    _update_base_offset = *_extent_base_offset;

    if (
      !ct_src._extents.empty()
      && (!_removable_tombstone_ranges.empty() || !_dirty_range_intervals.empty())) {
        co_await initialize_builder();
    }
}

bool compaction_sink::needs_roll() const {
    // TODO: This needs to consider L1 object size and what-not eventually.
    if (needs_pushing()) {
        return true;
    }

    if (!_active_staging_file) {
        return true;
    }

    if (_builder->file_size() >= max_object_size) {
        return true;
    }

    return false;
}

bool compaction_sink::needs_pushing() const {
    // We should be pushing when we have rewritten up to the last offset of a
    // dirty interval. e.g. dirty intervals are [50,75], [100,120], index
    // [50,75] AND [100,120]. Once we have rewriting offsets [0,75], we can mark
    // [50,75] as clean and push the update so far to the `committer`
    // because there is forward progress on removing dirty ranges. For the next
    // interval we rewrite [76,120], and then can mark [100,120] as clean.
    // Unfortunately this sort of scheduling is obviously bad if the dirty
    // ranges of the log looks more like a single large entry, e.g. [100,
    // 1000000], which is quite possible. For that reason, there needs to be a
    // certain commit rate or accumulated data size to consider for pushing
    // updates.
    // auto did_compact_up_to_dirty_range = _dirty_range_it
    //                                       != _dirty_range_intervals.cend()
    //                                     && _max_batch_offset
    //                                          >= _dirty_range_it->last_offset;
    // if (did_compact_up_to_dirty_range) {
    //    return true;
    //}
    return false;
}

ss::future<> compaction_sink::initialize_builder() {
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

    _active_staging_file = std::move(staging_file_result).value();
    auto output_stream = co_await _active_staging_file->output_stream();

    _builder = object_builder::create(std::move(output_stream), _opts);

    co_await _builder->start_partition(_tp);
}

ss::future<> compaction_sink::roll(bool initialize_new_builder) {
    // 1. Add current `_active_staging_file` & `_builder` info to
    // `_closed_staging_files_and_md_infos`.
    if (_active_staging_file) {
        auto active_staging_file = std::exchange(_active_staging_file, nullptr);
        auto builder = std::exchange(_builder, nullptr);

        auto object_info_fut = co_await ss::coroutine::as_future(
          builder->finish());
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

        _update_last_offset = *_extent_last_offset;

        auto object_info = object_info_fut.get();
        auto ntp_md = [this](const object_builder::object_info& info) {
            auto [first, last] = info.index.partitions.equal_range(_tp);
            vassert(
              std::distance(first, last) == 1,
              "Expected one partition range in builder.");
            return metastore::object_metadata::ntp_metadata{
              .tidp = _tp,
              .base_offset = _update_base_offset,
              .last_offset = _update_last_offset,
              .max_timestamp = first->second.max_timestamp,
              .pos = first->second.file_position,
              .size = first->second.length};
        }(object_info);

        _closed_staging_files_and_md_infos.emplace_back(
          std::move(active_staging_file),
          std::move(object_info),
          std::move(ntp_md));

        _update_base_offset = *_extent_base_offset;
    }

    // 2. Start new `_active_staging_file` and `_builder`.
    if (initialize_new_builder) {
        co_await initialize_builder();
    }
}

ss::future<> compaction_sink::maybe_roll() {
    if (needs_roll()) {
        co_await roll(true);
    }
}

metastore::compaction_update compaction_sink::make_compaction_update() {
    const auto max_indexed_offset = model::offset_cast(_map->max_offset());
    const auto range_has_tombstones = std::exchange(
      _range_has_tombstones, false);

    chunked_vector<metastore::compaction_update::cleaned_range>
      new_cleaned_ranges;

    std::optional<kafka::offset> clean_base_offset;
    std::optional<kafka::offset> clean_last_offset;
    // Find the range that is now made clean by this update.
    // e.g. for the dirty ranges and max_indexed_offset:
    //    max_indexed_offset (43) ---v
    //    [  [0, 15],  [20, 30],  [40, 50], [60, 72]  ]
    // we expect to find base_offset=0, last_offset=43.
    for (const auto& dirty_range : _dirty_range_intervals) {
        if (max_indexed_offset >= dirty_range.base_offset) {
            if (!clean_base_offset.has_value()) {
                clean_base_offset = dirty_range.base_offset;
            }
            clean_last_offset = std::min(
              dirty_range.last_offset, max_indexed_offset);
        } else {
            break;
        }
    }

    if (clean_base_offset.has_value() && clean_last_offset.has_value()) {
        new_cleaned_ranges.push_back(
          metastore::compaction_update::cleaned_range{
            .base_offset = clean_base_offset.value(),
            .last_offset = clean_last_offset.value(),
            .has_tombstones = range_has_tombstones});
    }

    offset_interval_set removed_tombstones_ranges;
    auto tombstone_strm = _removable_tombstone_ranges.make_stream();
    while (tombstone_strm.has_next()) {
        auto i = tombstone_strm.next();
        if (_update_last_offset >= i.base_offset) {
            removed_tombstones_ranges.insert(
              i.base_offset, std::min(i.last_offset, _update_last_offset));
        } else {
            break;
        }
    }

    return metastore::compaction_update{
      .new_cleaned_ranges = std::move(new_cleaned_ranges),
      .removed_tombstones_ranges = std::move(removed_tombstones_ranges),
      .cleaned_at = model::timestamp::now()};
}

void compaction_sink::push_update() {
    if (_closed_staging_files_and_md_infos.empty()) {
        return;
    }

    auto closed_staging_files_and_md_infos = std::exchange(
      _closed_staging_files_and_md_infos, {});

    auto compact_update = make_compaction_update();

    auto out = object_output_t{
      .tidp = _tp,
      .staging_files_and_md_infos = std::move(
        closed_staging_files_and_md_infos),
      .compact_update = std::move(compact_update)};

    _committer->push_update(std::move(out));
}

void compaction_sink::maybe_push_update() {
    if (needs_pushing()) {
        push_update();
    }
}

ss::future<ss::stop_iteration>
compaction_sink::operator()(model::record_batch b, model::compression c) {
    co_await maybe_roll();
    maybe_push_update();

    if (c != model::compression::none) {
        b = co_await model::compress_batch(c, std::move(b));
    }

    co_await _builder->add_batch(std::move(b));

    co_return ss::stop_iteration::no;
}

ss::future<> compaction_sink::finalize() {
    co_await roll(false);
    push_update();
}

} // namespace cloud_topics::l1
