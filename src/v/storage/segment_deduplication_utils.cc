// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_deduplication_utils.h"

#include "model/timestamp.h"
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
        co_return true;
    }
    // We should only keep the record if its offset is equal or higher than
    // that indexed.
    co_return o >= latest_offset_indexed.value();
}
} // anonymous namespace

ss::future<bool> build_offset_map_for_segment(
  const compaction_config& cfg, const segment& seg, key_offset_map& m) {
    auto compaction_idx_path = seg.path().to_compacted_index();
    auto compaction_idx_file = co_await internal::make_reader_handle(
      compaction_idx_path, cfg.sanitizer_config);
    std::exception_ptr eptr;
    auto rdr = make_file_backed_compacted_reader(
      compaction_idx_path, compaction_idx_file, cfg.iopc, 64_KiB, cfg.asrc);
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
        auto seg = *iter;
        if (seg->index().has_clean_compact_timestamp()) {
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
              seg, stm_manager, cfg, read_lock, resources, probe);
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
  offset_delta_time should_offset_delta_times,
  ss::sharded<features::feature_table>& feature_table,
  bool inject_reader_failure) {
    auto read_holder = co_await seg->read_lock();
    if (seg->is_closed()) {
        throw segment_closed_exception();
    }
    auto rdr = internal::create_segment_full_reader(
      seg, cfg, probe, std::move(read_holder));
    auto compaction_placeholder_enabled = feature_table.local().is_active(
      features::feature::compaction_placeholder_batch);

    const bool past_tombstone_delete_horizon
      = internal::is_past_tombstone_delete_horizon(seg, cfg);
    bool may_have_tombstone_records = false;
    auto copy_reducer = internal::copy_data_segment_reducer(
      [&map,
       &may_have_tombstone_records,
       segment_last_offset = seg->offsets().get_committed_offset(),
       past_tombstone_delete_horizon,
       compaction_placeholder_enabled](
        const model::record_batch& b,
        const model::record& r,
        bool is_last_record_in_batch) {
          auto is_last_batch = b.last_offset() == segment_last_offset;
          // once compaction placeholder feature is enabled, we are not
          // worried about empty batches as the reducer then installs a
          // placeholder batch if all the records are compacted away.
          if (
            !compaction_placeholder_enabled
            && (is_last_batch && is_last_record_in_batch)) {
              vlog(
                gclog.trace,
                "retaining last record: {} of segment from batch: {}",
                r,
                b.header());
              return ss::make_ready_future<bool>(true);
          }

          // Deal with tombstone record removal
          if (r.is_tombstone() && past_tombstone_delete_horizon) {
              return ss::make_ready_future<bool>(false);
          }

          return should_keep(map, b, r).then(
            [&may_have_tombstone_records,
             is_tombstone = r.is_tombstone()](bool keep) {
                if (is_tombstone && keep) {
                    may_have_tombstone_records = true;
                }
                return keep;
            });
      },
      &appender,
      seg->path().is_internal_topic(),
      should_offset_delta_times,
      seg->offsets().get_committed_offset(),
      &cmp_idx_writer,
      inject_reader_failure,
      cfg.asrc);

    auto new_idx = co_await std::move(rdr).consume(
      std::move(copy_reducer), model::no_timeout);

    // restore broker timestamp and clean compact timestamp
    new_idx.broker_timestamp = seg->index().broker_timestamp();
    new_idx.clean_compact_timestamp = seg->index().clean_compact_timestamp();

    // Set may_have_tombstone_records
    new_idx.may_have_tombstone_records = may_have_tombstone_records;

    co_return new_idx;
}

} // namespace storage
