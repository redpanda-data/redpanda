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

#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/source.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "cloud_topics/level_one/metastore/retry.h"
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
  metastore* metastore,
  ss::abort_source& as,
  config::binding<size_t> max_object_size,
  size_t upload_part_size,
  object_builder::options opts)
  : _max_object_size(std::move(max_object_size))
  , _upload_part_size(upload_part_size)
  , _tp(tp)
  , _dirty_range_intervals(dirty_range_intervals)
  , _removable_tombstone_ranges(removable_tombstone_ranges)
  , _expected_compaction_epoch(expected_compaction_epoch)
  , _start_offset(start_offset)
  , _io(io)
  , _metastore(metastore)
  , _as(as)
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

    auto metadata_builder_res
      = co_await l1::retry_metastore_op_with_default_rtc(
        [this]() { return _metastore->object_builder(); }, _as);
    if (!metadata_builder_res.has_value()) {
        vlog(
          compaction_log.warn,
          "Could not create object metadata builder for compaction of tidp {}: "
          "{}. Aborting.",
          _tp,
          metadata_builder_res.error());
        throw std::runtime_error("Couldn't begin compaction");
    }
    _metadata_builder = std::move(metadata_builder_res).value();

    auto& new_cleaned_ranges = ct_src._new_cleaned_ranges;
    new_cleaned_ranges.shrink_to_fit();
    _new_cleaned_ranges = std::move(new_cleaned_ranges);

    vlog(
      compaction_log.debug,
      "Built compaction map for tidp {} with {} keys (max allowed {})",
      _tp,
      ct_src._map->size(),
      ct_src._map->capacity());

    co_return true;
}

ss::future<>
compaction_sink::initialize_builder(kafka::offset object_base_offset) {
    auto oid_res = co_await _metadata_builder->create_object_for(_tp);
    if (!oid_res.has_value()) {
        vlog(
          compaction_log.warn,
          "Failed to create object for tidp {}: {}",
          _tp,
          oid_res.error());
        throw std::runtime_error("Failed to create object for compaction");
    }
    auto oid = std::move(oid_res).value();

    auto upload_res = co_await _io->create_multipart_upload(
      oid, _upload_part_size, &_as);

    if (!upload_res.has_value()) {
        std::ignore = _metadata_builder->remove_pending_object(oid);
        vlog(
          compaction_log.warn,
          "Failed to create multipart upload for object {}, tidp {}: {}",
          oid,
          _tp,
          static_cast<int>(upload_res.error()));
        throw std::runtime_error(
          "Failed to create multipart upload for compaction");
    }

    auto upload = std::move(upload_res).value();
    auto output_stream = upload->as_stream();

    auto builder = object_builder::create(std::move(output_stream), _opts);
    co_await builder->start_partition(_tp);

    _inflight_object = std::make_unique<compacted_object>(
      std::move(upload), std::move(builder), oid, object_base_offset);
}

ss::future<> compaction_sink::discard_object(
  cloud_storage_clients::multipart_upload_ref upload,
  std::unique_ptr<object_builder> builder,
  object_id oid) {
    (co_await ss::coroutine::as_future(upload->abort())).ignore_ready_future();
    (co_await ss::coroutine::as_future(builder->close())).ignore_ready_future();
    std::ignore = _metadata_builder->remove_pending_object(oid);
    _any_object_failed = true;
}

ss::future<> compaction_sink::flush(kafka::offset object_last_offset) {
    if (!_inflight_object) {
        co_return;
    }

    auto inflight_object = std::exchange(_inflight_object, nullptr);
    auto upload = std::move(inflight_object->upload);
    auto builder = std::exchange(inflight_object->builder, nullptr);
    auto oid = inflight_object->oid;
    auto object_base_offset = inflight_object->object_base_offset;

    // Write the footer and get object metadata.
    auto object_info_fut = co_await ss::coroutine::as_future(builder->finish());
    if (object_info_fut.failed()) {
        auto e = object_info_fut.get_exception();
        vlogl(
          compaction_log,
          ssx::is_shutdown_exception(e) ? ss::log_level::debug
                                        : ss::log_level::warn,
          "Exception creating object_info: {}.",
          e);
        co_return co_await discard_object(
          std::move(upload), std::move(builder), oid);
    }

    // close() completes the multipart upload via the stream's data sink.
    auto close_fut = co_await ss::coroutine::as_future(builder->close());
    if (close_fut.failed()) {
        auto e = close_fut.get_exception();
        vlogl(
          compaction_log,
          ssx::is_shutdown_exception(e) ? ss::log_level::debug
                                        : ss::log_level::warn,
          "Exception closing object builder: {}.",
          e);
        if (!upload->is_finalized()) {
            co_await upload->abort();
        }
        std::ignore = _metadata_builder->remove_pending_object(oid);
        _any_object_failed = true;
        co_return;
    }

    // Upload succeeded — register the object with the metadata builder.
    auto object_info = object_info_fut.get();

    vlog(
      compaction_log.trace,
      "Completed multipart upload for object {} ({}~{}) for tidp {}",
      oid,
      object_base_offset,
      object_last_offset,
      _tp);

    auto [first, last] = object_info.index.partitions.equal_range(_tp);
    vassert(
      std::distance(first, last) == 1,
      "Expected one partition range in builder.");
    dassert(
      object_base_offset <= object_last_offset,
      "Compaction sink produced inverted extent for tidp {}: base_offset {} "
      "> last_offset {}",
      _tp,
      object_base_offset,
      object_last_offset);
    auto ntp_md = metastore::object_metadata::ntp_metadata{
      .tidp = _tp,
      .base_offset = object_base_offset,
      .last_offset = object_last_offset,
      .max_timestamp = first->second.max_timestamp,
      .pos = first->second.file_position,
      .size = first->second.length};

    auto add_res = _metadata_builder->add(oid, std::move(ntp_md));
    if (!add_res.has_value()) {
        vlog(
          compaction_log.warn,
          "Failed to add object {} to metadata builder: {}",
          oid,
          add_res.error());
        std::ignore = _metadata_builder->remove_pending_object(oid);
        _any_object_failed = true;
        co_return;
    }

    auto finish_res = _metadata_builder->finish(
      oid, object_info.footer_offset, object_info.size_bytes);
    if (!finish_res.has_value()) {
        vlog(
          compaction_log.warn,
          "Failed to finish object {} in metadata builder: {}",
          oid,
          finish_res.error());
        std::ignore = _metadata_builder->remove_pending_object(oid);
        _any_object_failed = true;
    }
}

ss::future<ss::stop_iteration>
compaction_sink::operator()(model::record_batch b, model::compression c) {
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
          compaction_log.info,
          "Finalized compaction of tidp {} without a compaction metadata "
          "update",
          _tp);
    } else {
        vlog(
          compaction_log.warn,
          "Could not commit object update during compaction of tidp {}: {}.",
          _tp,
          replace_res.error());
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
          compaction_log.info,
          "Finalized compaction of tidp {} with compaction metadata update {}",
          _tp,
          compaction_update_str);
    } else {
        vlog(
          compaction_log.warn,
          "Could not commit metadata update {} for compaction of tidp {}: {}. "
          "Retrying object update without metadata.",
          compaction_update_str,
          _tp,
          commit_res.error());
        co_return co_await compact_objects_without_update();
    }
}

ss::future<> compaction_sink::finalize(bool success) {
    if (!_metadata_builder) {
        co_return;
    }

    if (_inflight_object) {
        if (success && !_processed_extents.empty()) {
            auto last_offset
              = _processed_extents.make_reverse_stream().next().last_offset;
            co_await flush(last_offset);
        } else {
            // On the exceptional path, the inflight object may either:
            // 1. Have a base offset > the last processed extent's last offset.
            //    E.g., iterating over extents [[0,9], [10,19]], a new object is
            //    rolled with `object_base_offset=15`, an exception is thrown
            //    and caught, and we call `sink->finalize()` with
            //    `last_offset=9`. Flushing would give us an object with offsets
            //    [15,9], violating the offset space.
            // 2. Have partially-processed extent data that extends beyond what
            //    `_processed_extents` tracks. E.g., iterating over extents
            //    [[0,9], [10,19]] with `object_base_offset=0`, we iterate up to
            //    offset 15 in the second extent, an exception is thrown and and
            //    caught, and we call `sink->finalize()` with `last_offset=9`.
            //    Flushing would give us an object with offsets [0,9], even
            //    though the inflight object now contains data for the offset
            //    space [0,15].
            // In either case, discard the inflight object in the exceptional
            // path to avoid uploading an object whose contents don't match the
            // recorded metadata.
            auto inflight_object = std::exchange(_inflight_object, nullptr);
            co_await discard_object(
              std::move(inflight_object->upload),
              std::exchange(inflight_object->builder, nullptr),
              inflight_object->oid);
        }
    }

    if (_metadata_builder->is_empty()) {
        vlog(
          compaction_log.debug,
          "No built or uploaded objects for tidp {}.",
          _tp);
        co_return;
    }

    if (_any_object_failed) {
        co_await compact_objects_without_update();
    } else {
        auto removed_tombstone_ranges = get_removed_tombstone_ranges(
          _removable_tombstone_ranges, _processed_extents);
        auto new_cleaned_ranges = get_new_cleaned_ranges(
          _new_cleaned_ranges, _processed_extents, _start_offset);
        co_await compact_objects_with_update(
          std::move(new_cleaned_ranges), std::move(removed_tombstone_ranges));
    }
}

} // namespace cloud_topics::l1
