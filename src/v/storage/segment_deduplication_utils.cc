// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_deduplication_utils.h"

#include "compaction/key_offset_map.h"
#include "compaction/utils.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/compaction_key.h"
#include "storage/compaction_reducers.h"
#include "storage/exceptions.h"
#include "storage/index_state.h"
#include "storage/probe.h"
#include "storage/scoped_file_tracker.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "storage/segment_utils.h"
#include "storage/types.h"

#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/as_future.hh>

#include <exception>
#include <optional>

namespace storage {

namespace {
ss::future<ss::stop_iteration> put_entry(
  compaction::key_offset_map& map,
  const compacted_index::entry& idx_entry,
  bool& fully_indexed) {
    auto offset = idx_entry.offset + model::offset_delta(idx_entry.delta);
    bool success = co_await map.put(idx_entry.key, offset);
    if (success) {
        co_return ss::stop_iteration::no;
    }
    fully_indexed = false;
    co_return ss::stop_iteration::yes;
}

ss::future<bool> is_latest_record_for_enhanced_key(
  const compaction::key_offset_map& map,
  const model::record_batch& b,
  const model::record& r) {
    const auto o = b.base_offset() + model::offset_delta(r.offset_delta());
    auto key_view = compaction::compaction_key{iobuf_to_bytes(r.key())};
    auto key = enhance_key(
      b.header().type, b.header().attrs.is_control(), key_view);

    auto latest_offset_indexed = co_await map.get(key);
    // If the map hasn't indexed the given key, we should keep the
    // key.
    if (!latest_offset_indexed.has_value()) {
        co_return true;
    }
    // We should only keep the record if its offset is equal or higher than
    // that indexed.
    co_return o >= latest_offset_indexed.value();
}

} // anonymous namespace

ss::future<bool> build_offset_map_for_segment(
  const compaction::compaction_config& cfg,
  const segment& seg,
  compaction::key_offset_map& m) {
    auto compaction_idx_path = seg.path().to_compacted_index();
    auto compaction_idx_file = co_await internal::make_reader_handle(
      compaction_idx_path, cfg.sanitizer_config);
    std::exception_ptr eptr;
    auto rdr = make_file_backed_compacted_reader(
      compaction_idx_path, compaction_idx_file, 64_KiB, cfg.asrc);
    try {
        co_await rdr.verify_integrity();
    } catch (...) {
        // TODO: rebuild?
        eptr = std::current_exception();
    }
    if (eptr) {
        co_await rdr.close();
        vlog(
          gclog.error,
          "Error building offset map for segment {}: {}",
          seg.path(),
          eptr);
        std::rethrow_exception(eptr);
    }
    bool fully_indexed = true;
    co_await rdr.for_each_async(
      [&m, &fully_indexed](const compacted_index::entry& idx_entry) {
          return put_entry(m, idx_entry, fully_indexed);
      },
      model::no_timeout);
    co_return fully_indexed;
}

ss::future<model::offset> build_offset_map(
  const compaction::compaction_config& cfg,
  const segment_set& segs,
  ss::lw_shared_ptr<storage::stm_hookset> stm_hookset,
  storage_resources& resources,
  storage::probe& probe,
  compaction::key_offset_map& m,
  ss::sharded<features::feature_table>& feature_table) {
    if (segs.empty()) {
        throw std::runtime_error("No segments to build offset map");
    }

    std::optional<model::offset> min_segment_fully_indexed;
    // Build the key offset map by iterating on older and older data.
    auto iter = std::prev(segs.end());
    while (true) {
        if (cfg.asrc) {
            cfg.asrc->check();
        }
        auto seg = *iter;
        if (seg->has_clean_compact_timestamp()) {
            // This segment has already been fully deduplicated, so building the
            // offset map for it would be pointless.
            vlog(
              gclog.trace,
              "segment is already cleanly compacted, no need to add it to the "
              "offset_map: {}",
              seg->filename());

            min_segment_fully_indexed = seg->offsets().get_base_offset();

            if (iter == segs.begin()) {
                break;
            } else {
                --iter;
                continue;
            }
        }
        vlog(gclog.trace, "Adding segment to offset map: {}", seg->filename());

        try {
            auto read_lock = co_await seg->read_lock();
            co_await internal::maybe_rebuild_compaction_index(
              seg,
              stm_hookset,
              cfg,
              read_lock,
              resources,
              probe,
              compaction::is_tx_batch_compaction_enabled(feature_table));
        } catch (const segment_closed_exception& e) {
            // Stop early if the segment e.g. has been prefix truncated.
            // We'll make do with the offset map we have so far.
            break;
        }

        auto seg_fully_indexed = co_await build_offset_map_for_segment(
          cfg, *seg, m);
        if (!seg_fully_indexed) {
            // The offset map is full. Note that we may have only partially
            // indexed a segment, but it's safe to use this index. If no new
            // segments come in, the next time we compact, we need to start
            // from this segment for completeness.
            vlog(gclog.debug, "Segment not fully indexed: {}", seg->filename());
            break;
        }
        min_segment_fully_indexed = seg->offsets().get_base_offset();
        if (iter == segs.begin()) {
            break;
        }
        iter--;
    }
    if (!min_segment_fully_indexed.has_value()) {
        // If we broke out without setting an offset, we failed to index even a
        // single segment, likely because it had too many keys.
        throw zero_segments_indexed_exception(
          fmt::format("Couldn't index {}", iter->get()->path()));
    }
    co_return min_segment_fully_indexed.value();
}

ss::future<index_state> deduplicate_segment(
  const compaction::compaction_config& cfg,
  const compaction::key_offset_map& map,
  ss::lw_shared_ptr<storage::segment> seg,
  segment_appender& appender,
  compacted_index_writer& cmp_idx_writer,
  ss::lw_shared_ptr<storage::stm_hookset> stm_hookset,
  probe& probe,
  offset_delta_time should_offset_delta_times,
  ss::sharded<features::feature_table>& feature_table,
  bool inject_reader_failure) {
    auto read_holder = co_await seg->read_lock();
    if (seg->is_closed()) {
        throw segment_closed_exception();
    }
    auto rdr = internal::create_segment_full_reader(
      seg, cfg, probe, std::move(read_holder));

    auto segment_last_offset = seg->offsets().get_committed_offset();
    auto compaction_placeholder_enabled = feature_table.local().is_active(
      features::feature::compaction_placeholder_batch);
    auto tx_batch_compaction_enabled
      = compaction::is_tx_batch_compaction_enabled(feature_table);
    const bool past_tombstone_delete_horizon
      = internal::is_past_tombstone_delete_horizon(seg, cfg);
    bool may_have_tombstone_records = false;
    const bool past_tx_delete_horizon
      = internal::is_past_transaction_batch_delete_horizon(
        seg, cfg, tx_batch_compaction_enabled);
    bool may_have_transaction_control_batches = false;
    bool may_have_transaction_data_or_fence_batches = false;

    auto is_latest_record = [&map](
                              const model::record_batch& b,
                              const model::record& r) -> ss::future<bool> {
        return is_latest_record_for_enhanced_key(map, b, r);
    };

    const auto& ntp = seg->path().get_ntp();
    auto record_filter = [f = std::move(is_latest_record),
                          &feature_table,
                          &ntp,
                          segment_last_offset,
                          past_tombstone_delete_horizon,
                          &may_have_tombstone_records,
                          &probe,
                          past_tx_delete_horizon,
                          &may_have_transaction_control_batches,
                          &may_have_transaction_data_or_fence_batches,
                          tx_batch_compaction_enabled](
                           const model::record_batch& b,
                           const model::record& r,
                           bool is_last_record_in_batch) {
        return internal::should_keep(
          b,
          r,
          ntp,
          is_last_record_in_batch,
          f,
          probe,
          feature_table,
          segment_last_offset,
          past_tombstone_delete_horizon,
          may_have_tombstone_records,
          past_tx_delete_horizon,
          may_have_transaction_control_batches,
          may_have_transaction_data_or_fence_batches,
          tx_batch_compaction_enabled);
    };

    auto copy_reducer = internal::copy_data_segment_reducer(
      ntp,
      std::move(record_filter),
      &appender,
      seg->path().is_internal_topic(),
      should_offset_delta_times,
      seg->index().base_offset(),
      segment_last_offset,
      compaction_placeholder_enabled,
      tx_batch_compaction_enabled,
      stm_hookset,
      &cmp_idx_writer,
      inject_reader_failure,
      cfg.asrc);

    auto res = co_await std::move(rdr).consume(
      std::move(copy_reducer), model::no_timeout);
    const auto& stats = res.reducer_stats;
    if (stats.has_removed_data()) {
        vlog(
          gclog.info,
          "Windowed compaction filtering removing data from {}: {}",
          seg->filename(),
          stats);
    } else {
        vlog(
          gclog.debug,
          "Windowed compaction filtering not removing any records from {}: {}",
          seg->filename(),
          stats);
    }

    // restore broker timestamp, self compact timestamp and clean compact
    // timestamp
    auto& new_idx = res.new_idx;
    new_idx.broker_timestamp = seg->index().broker_timestamp();
    new_idx.self_compact_timestamp = seg->index().self_compact_timestamp();
    new_idx.clean_compact_timestamp = seg->index().clean_compact_timestamp();

    // Set may_have_tombstone_records
    new_idx.may_have_tombstone_records = may_have_tombstone_records;

    // Set may_have_transaction_control_batches
    new_idx.may_have_transaction_control_batches
      = may_have_transaction_control_batches;

    // Set may_have_transaction_data_or_fence_batches
    new_idx.may_have_transaction_data_or_fence_batches
      = may_have_transaction_data_or_fence_batches;

    if (
      seg->index().may_have_tombstone_records()
      && !may_have_tombstone_records) {
        probe.add_segment_marked_tombstone_free();
    }

    co_return std::move(new_idx);
}

ss::future<bool> index_chunk_of_segment_for_map(
  const compaction::compaction_config& compact_cfg,
  ss::lw_shared_ptr<segment> seg,
  compaction::key_offset_map& map,
  probe& pb,
  model::offset& last_indexed_offset) {
    if (seg->is_closed()) {
        throw segment_closed_exception();
    }
    co_await map.reset();
    auto read_holder = co_await seg->read_lock();
    auto start_offset_inclusive = model::next_offset(last_indexed_offset);
    auto rdr = internal::create_segment_full_reader(
      seg, compact_cfg, pb, std::move(read_holder), start_offset_inclusive);
    internal::map_building_reducer reducer(
      seg->path().get_ntp(), &map, start_offset_inclusive);

    bool fully_indexed_segment = co_await std::move(rdr).consume(
      reducer, model::no_timeout);

    last_indexed_offset = map.max_offset();
    if (fully_indexed_segment) {
        vlog(
          gclog.trace,
          "Finished building offset map for segment {}",
          seg->reader().filename());
    } else {
        vlog(
          gclog.trace,
          "Built offset map up to offset {}/{} for segment {}",
          last_indexed_offset,
          seg->offsets().get_dirty_offset(),
          seg->reader().filename());
    }

    co_return fully_indexed_segment;
}

ss::future<bool> segment_needs_rewrite_with_offset_map(
  const compaction::compaction_config& cfg,
  ss::lw_shared_ptr<segment> seg,
  const compaction::key_offset_map& map) {
    auto compaction_idx_path = seg->path().to_compacted_index();
    // If the file doesn't exist for whatever reason, return true.
    // This could, for example, race with a truncation which removes the
    // .compaction_index.
    if (!co_await ss::file_exists(compaction_idx_path.string())) {
        co_return true;
    }
    auto compaction_idx_file = co_await internal::make_reader_handle(
      compaction_idx_path, cfg.sanitizer_config);
    auto rdr = make_file_backed_compacted_reader(
      compaction_idx_path, compaction_idx_file, 64_KiB, cfg.asrc);

    auto fut = co_await ss::coroutine::as_future(rdr.verify_integrity());
    if (fut.failed()) {
        // If we were unable to read the .compaction_index file, proceed with
        // compaction.
        fut.ignore_ready_future();
        co_await rdr.close();
        co_return true;
    }

    bool segment_needs_rewrite = false;
    // The segment in question needs rewriting _only_ iff it contains at least
    // one key found in the key_offset_map which is not the latest offset for
    // that key.
    co_await rdr.for_each_async(
      [&map, &segment_needs_rewrite](
        const compacted_index::entry& e) -> ss::future<ss::stop_iteration> {
          model::offset o = e.offset + model::offset_delta(e.delta);
          return map.get(e.key).then([o, &segment_needs_rewrite](
                                       std::optional<model::offset> map_entry) {
              if (map_entry.has_value() && map_entry.value() > o) {
                  segment_needs_rewrite = true;
                  return ss::stop_iteration::yes;
              }
              return ss::stop_iteration::no;
          });
      },
      model::no_timeout);
    co_return segment_needs_rewrite;
}

} // namespace storage
