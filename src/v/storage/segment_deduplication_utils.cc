// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_deduplication_utils.h"

#include "storage/compacted_index_writer.h"
#include "storage/compaction_reducers.h"
#include "storage/index_state.h"
#include "storage/key_offset_map.h"
#include "storage/probe.h"
#include "storage/scoped_file_tracker.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "storage/segment_utils.h"
#include "storage/types.h"

#include <exception>

namespace storage {

namespace {
ss::future<ss::stop_iteration> put_entry(
  key_offset_map& map,
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

ss::future<bool> should_keep(
  const key_offset_map& map,
  const model::record_batch& b,
  const model::record& r) {
    const auto o = b.base_offset() + model::offset_delta(r.offset_delta());
    auto key_view = compaction_key{iobuf_to_bytes(r.key())};
    auto key = enhance_key(
      b.header().type, b.header().attrs.is_control(), key_view);
    auto latest_offset_indexed = co_await map.get(key);
    // If the map hasn't indexed the given key, we should keep the
    // key.
    if (!latest_offset_indexed.has_value()) {
        vlog(gclog.info, "AWONG {}: {}", o, true);
        co_return true;
    }
    // We should only keep the record if its offset is equal or higher than
    // that indexed.
    auto ret = o >= latest_offset_indexed.value();
    vlog(gclog.info, "AWONG {}: {}", o, ret);
    co_return ret;
}
} // anonymous namespace

ss::future<bool> build_offset_map_for_segment(
  const compaction_config& cfg, const segment& seg, key_offset_map& m) {
    auto compaction_idx_path = seg.path().to_compacted_index();
    auto compaction_idx_file = co_await internal::make_reader_handle(
      compaction_idx_path, cfg.sanitizer_config);
    std::exception_ptr eptr;
    auto rdr = make_file_backed_compacted_reader(
      compaction_idx_path, compaction_idx_file, cfg.iopc, 64_KiB);
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
  const compaction_config& cfg,
  const segment_set& segs,
  ss::lw_shared_ptr<storage::stm_manager> stm_manager,
  storage_resources& resources,
  storage::probe& probe,
  key_offset_map& m) {
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
        const auto& seg = iter->get();
        vlog(gclog.trace, "Adding segment to offset map: {}", seg->filename());
        auto read_lock = co_await seg->read_lock();
        segment_full_path idx_path = seg->path().to_compacted_index();
        std::optional<scoped_file_tracker> to_clean;
        bool segment_closed = false;
        while (true) {
            if (seg->is_closed()) {
                // Stop early if the segment e.g. has been prefix truncated.
                // We'll make do with the offset map we have so far.
                vlog(
                  gclog.debug,
                  "Stopping add to offset map, segment closed: {}",
                  seg->filename());
                segment_closed = true;
                break;
            }
            auto state = co_await internal::detect_compaction_index_state(
              idx_path, cfg);
            if (!internal::compacted_index_needs_rebuild(state)) {
                break;
            }
            // Rebuilding the compaction index will take the read lock again,
            // so release here.
            read_lock.return_all();

            // Until we check the segment isn't closed under lock, we may need
            // to delete the new index: its segment may already be removed!
            if (!to_clean.has_value()) {
                to_clean.emplace(
                  scoped_file_tracker{cfg.files_to_cleanup, {idx_path}});
            }

            co_await internal::rebuild_compaction_index(
              *iter, stm_manager, cfg, probe, resources);

            // Take the lock again before checking the compaction index state
            // to avoid races with truncations while building the offset map.
            read_lock = co_await seg->read_lock();
        }
        if (segment_closed) {
            break;
        }
        if (to_clean.has_value()) {
            to_clean->clear();
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
        min_segment_fully_indexed = seg->offsets().base_offset;
        if (iter == segs.begin()) {
            break;
        }
        iter--;
    }
    if (!min_segment_fully_indexed.has_value()) {
        // If we broke out without setting an offset, we failed to index even a
        // single segment, likely because it had too many keys.
        throw std::runtime_error(
          fmt::format("Couldn't index {}", iter->get()->path()));
    }
    co_return min_segment_fully_indexed.value();
}

ss::future<index_state> deduplicate_segment(
  const compaction_config& cfg,
  const key_offset_map& map,
  ss::lw_shared_ptr<storage::segment> seg,
  segment_appender& appender,
  compacted_index_writer& cmp_idx_writer,
  probe& probe,
  offset_delta_time should_offset_delta_times) {
    auto read_holder = co_await seg->read_lock();
    if (seg->is_closed()) {
        throw segment_closed_exception();
    }
    auto rdr = internal::create_segment_full_reader(
      seg, cfg, probe, std::move(read_holder));
    auto orig_max_offset = seg->offsets().committed_offset;
    auto copy_reducer = internal::copy_data_segment_reducer(
      [&map](const model::record_batch& b, const model::record& r) {
          return should_keep(map, b, r);
      },
      &appender,
      seg->path().is_internal_topic(),
      should_offset_delta_times,
      orig_max_offset,
      &cmp_idx_writer);

    auto new_idx = co_await rdr.consume(
      std::move(copy_reducer), model::no_timeout);
    vassert(
      new_idx.max_offset == orig_max_offset,
      "new offsets: {}, original: {}",
      new_idx,
      seg->offsets());
    new_idx.broker_timestamp = seg->index().broker_timestamp();
    co_return new_idx;
}

} // namespace storage
