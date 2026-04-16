/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "compaction/key_offset_map.h"
#include "compaction/utils.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_reader.h"
#include "storage/compacted_index_writer.h"
#include "storage/compacted_offset_list.h"
#include "storage/logger.h"
#include "storage/probe.h"
#include "storage/readers_cache.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"
#include "utils/named_type.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include <roaring/roaring.hh>

namespace storage::internal {

// Rebuilds the compaction index for a segment, if it is needed.
// Requires a rwlock::holder to be passed in, which is likely to be the
// segment's read_lock(). The lock owned by the holder will be held after this
// function call completes, allowing the caller to proceed to self compaction or
// other destructive operations.
//
// Returns the recovery_state, indicating status of the compaction index and
// whether self-compaction should be executed or not.
ss::future<compacted_index::recovery_state> maybe_rebuild_compaction_index(
  ss::lw_shared_ptr<segment> s,
  ss::lw_shared_ptr<storage::stm_hookset> stm_hookset,
  const compaction::compaction_config& cfg,
  ss::rwlock::holder& read_holder,
  storage_resources& resources,
  storage::probe& pb,
  bool tx_batch_compaction_enabled);

/// \brief, this method will acquire it's own locks on the segment
///
ss::future<compaction_result> self_compact_segment(
  ss::lw_shared_ptr<storage::segment>,
  ss::lw_shared_ptr<storage::stm_hookset>,
  const compaction::compaction_config&,
  storage::probe&,
  storage::readers_cache&,
  storage::storage_resources&,
  ss::sharded<features::feature_table>& feature_table,
  bool force_compaction = false);

/// \brief, rebuilds a given segment's compacted index. This method acquires
/// locks on the segment.
ss::future<> rebuild_compaction_index(
  ss::lw_shared_ptr<segment> s,
  ss::lw_shared_ptr<storage::stm_hookset> stm_hookset,
  compaction::compaction_config cfg,
  storage::probe& pb,
  storage_resources& resources,
  bool tx_batch_compaction_enabled);

/*
 * Concatentate segments into a minimal new segment.
 *
 * This is effectively equivalent to acquiring the proper locks while
 * concatenating segment data into the given path and then building an open
 * segment around the new data file. The returned segment will only have a
 * reader and the proper offset tracking metadata.
 *
 * Note that the segment has an index, but it is empty. The caller is expected
 * to either immediately rebuild or replace the index. Current behavior is that
 * readers built from a segment with an empty segment will read from the
 * beginning which is exactly what we want for the rebuild process.
 *
 * The resulting segment will have the same term and base offset of the first
 * segment, and upper range offsets (e.g. stable_offset) taken from the last
 * segment in the input range.
 */
ss::future<std::tuple<
  ss::lw_shared_ptr<segment>,
  chunked_vector<segment::generation_id>>>
make_concatenated_segment(
  segment_full_path,
  chunked_vector<ss::lw_shared_ptr<segment>>&,
  compaction::compaction_config,
  storage_resources& resources,
  ss::sharded<features::feature_table>& feature_table);

// Must be called with _segments_rewrite_lock from the parent log held.
// Concatenates all segments in `segments` and replaces the in-memory and
// on-disk state of `target` with the built replacement. Self compaction is
// performed on the replacement target before the swap occurs to ensure its
// state is fully re-built (.base_index file, .compaction_index file, in-memory
// states). Write locks for all passed segments will be held during
// concatenation.
ss::future<compaction_result> concatenate_and_rebuild_target_segment(
  ss::lw_shared_ptr<segment> target,
  chunked_vector<ss::lw_shared_ptr<segment>>& segments,
  ss::lw_shared_ptr<storage::stm_hookset> stm_hookset,
  compaction::compaction_config cfg,
  storage::probe& pb,
  storage::readers_cache& readers_cache,
  storage_resources& resources,
  ss::sharded<features::feature_table>& feature_table,
  ssx::mutex& segment_rewrite_lock);

ss::future<> write_concatenated_compacted_index(
  std::filesystem::path,
  chunked_vector<ss::lw_shared_ptr<segment>>&,
  compaction::compaction_config,
  storage_resources& resources);

ss::future<chunked_vector<ss::rwlock::holder>> transfer_segment(
  ss::lw_shared_ptr<segment> to,
  ss::lw_shared_ptr<segment> from,
  compaction::compaction_config cfg,
  probe& probe,
  chunked_vector<ss::rwlock::holder>,
  std::optional<size_t> new_cmp_idx_size);

/*
 * Acquire write locks on multiple segments. The process will proceed until
 * success, or timeout. Failure to acquire the locks may result from contention
 * or deadlock. There is no intelligent handling for deadlock avoidance or
 * fairness. If a lock cannot be acquired all held locks are released and the
 * process is retried. Favor more retries over longer timeouts.
 */
ss::future<chunked_vector<ss::rwlock::holder>> write_lock_segments(
  chunked_vector<ss::lw_shared_ptr<segment>>& segments,
  ss::semaphore::clock::duration timeout,
  int retries);

/// make file handle with default opts
ss::future<ss::file> make_writer_handle(
  const std::filesystem::path&,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config,
  bool truncate = false);
/// make file handle with default opts
ss::future<ss::file> make_reader_handle(
  const std::filesystem::path&,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config);
ss::future<ss::file> make_handle(
  std::filesystem::path path,
  ss::open_flags flags,
  ss::file_open_options opt,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config);

ss::future<segment_appender_ptr> make_segment_appender(
  const segment_full_path& path,
  std::optional<uint64_t> segment_size,
  storage_resources& resources,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config,
  segment_appender::stats_ptr shared_stats);

uint64_t segment_size_from_config(const storage::ntp_config&);

/*
1. if footer.flags == truncate write new .compacted_index file
2. produce list of dedup'd (base_offset,delta) - in memory
3. consume that list, and produce small batches of 500_KiB
4. write new batches to disk
*/

/// \brief this is a 0-based index (i.e.: i++) of the entries we need to
/// save starting at 0 on a *new* `.compacted_index` file this represents
/// the fully dedupped entries, clean of truncations, etc
ss::future<roaring::Roaring>
  natural_index_of_entries_to_keep(compacted_index_reader);

// Returns the size of the built compacted index in bytes.
ss::future<size_t> copy_filtered_entries(
  storage::compacted_index_reader input,
  roaring::Roaring to_copy_index_filter,
  storage::compacted_index_writer output);

/// \brief writes a new `*.compacted_index` file and *closes* the
/// input compacted_index_reader file
///
/// Returns the size of the built compacted index in bytes.
ss::future<size_t> write_clean_compacted_index(
  storage::compacted_index_reader,
  compaction::compaction_config,
  storage_resources& resources);

ss::future<compacted_offset_list>
  generate_compacted_list(model::offset, storage::compacted_index_reader);

ss::future<bool> detect_if_segment_already_compacted(
  std::filesystem::path, compaction::compaction_config);

bool compacted_index_needs_rebuild(compacted_index::recovery_state state);

ss::future<compacted_index::recovery_state> detect_compaction_index_state(
  segment_full_path p, compaction::compaction_config cfg);

/// \brief creates a model::record_batch_reader from segment meta
///
model::record_batch_reader create_segment_full_reader(
  ss::lw_shared_ptr<storage::segment>,
  compaction::compaction_config,
  storage::probe&,
  ss::rwlock::holder,
  std::optional<model::offset> start_offset = std::nullopt);

ss::future<storage::index_state> do_copy_segment_data(
  ss::lw_shared_ptr<storage::segment>,
  compaction::compaction_config,
  ss::lw_shared_ptr<storage::stm_hookset>,
  storage::probe&,
  ss::rwlock::holder,
  storage_resources&);

ss::future<> do_swap_data_file_handles(
  std::filesystem::path compacted,
  ss::lw_shared_ptr<storage::segment>,
  compaction::compaction_config,
  probe&,
  std::optional<size_t>);

// Generates a random jitter percentage [as a fraction] with in the passed
// percents range.
float random_jitter(jitter_percents);

// key types used to store data in key-value store
enum class kvstore_key_type : int8_t {
    start_offset = 0,
    clean_segment = 1,
};

bytes start_offset_key(model::ntp ntp);
bytes clean_segment_key(model::ntp ntp);

struct clean_segment_value
  : serde::envelope<
      clean_segment_value,
      serde::version<0>,
      serde::compat_version<0>> {
    ss::sstring segment_name;
    auto serde_fields() { return std::tie(segment_name); }
};

offset_delta_time should_apply_delta_time_offset(
  ss::sharded<features::feature_table>& feature_table);

// Checks if a segment is past the tombstone deletion horizon.
//
// Returns true iff the segment `s` has been marked as cleanly
// compacted, the `compaction_config` has a value assigned for
// `tombstone_retention_ms`, and the current timestamp is greater than
// `clean_compact_timestamp + tombstone_retention_ms`. In all other cases,
// the returned value is false, indicating that tombstone records in the segment
// are not yet eligible for removal.
bool is_past_tombstone_delete_horizon(
  ss::lw_shared_ptr<segment> seg, const compaction::compaction_config& cfg);

// Checks if a segment may have any tombstones currently eligible for deletion.
//
// Returns true if the segment is marked as potentially having tombstone
// records, and if the result of evaluating
// `is_past_tombstone_delete_horizon(seg, cfg)` is also true. This can return
// false-positives, since segments that have not yet gone through the compaction
// process are assumed to potentially contain tombstones until proven otherwise.
bool may_have_removable_tombstones(
  ss::lw_shared_ptr<segment> seg, const compaction::compaction_config& cfg);

bool is_past_transaction_batch_delete_horizon(
  ss::lw_shared_ptr<segment> seg,
  const compaction::compaction_config& cfg,
  bool tx_batch_compaction_enabled);

bool has_removable_transaction_batches(
  ss::lw_shared_ptr<segment> seg,
  const compaction::compaction_config& cfg,
  bool tx_batch_compaction_enabled);

// Mark a segment as completed window compaction, and whether it is "clean" (in
// which case the `clean_compact_timestamp` is set in the segment's index).
// Also potentially issues a call to seg->index()->flush(), if the
// `clean_compact_timestamp` was set in the index.
//
// Returns a boolean indicating if the segment was marked as cleanly compacted
// for the first time and assigned a cleanly compacted timestamp.
ss::future<bool> mark_segment_as_finished_window_compaction(
  ss::lw_shared_ptr<segment> seg, bool set_clean_compact_timestamp, probe& pb);

ss::future<bool> mark_segment_as_finished_self_compaction(
  ss::lw_shared_ptr<segment> seg, probe& pb);

template<typename Func>
auto with_segment_reader_handle(segment_reader_handle handle, Func func) {
    static_assert(
      std::is_nothrow_move_constructible_v<Func>,
      "Func's move constructor must not throw");

    return ss::do_with(
      std::move(handle),
      [func = std::move(func)](segment_reader_handle& handle) {
          return ss::futurize_invoke(func, handle).finally([&handle] {
              return handle.close().then(
                [] { return ss::make_ready_future<>(); });
          });
      });
}

// Returns `true` or `false` depending on whether the provided record/control
// batch is instantly removable by compaction.
// This will return `true` for:
//   1. Removable control batches (such as those found in the
//      __consumer_offsets topic).
//   2. Expired tombstone records past the removal horizon set by
//      `delete.retention.ms`
//   3. Expired control batches past the removal horizon set by
//      `delete.retention.ms`
// In all other cases, return `false`.
inline bool can_discard(
  const model::record_batch& b,
  const model::record& r,
  const model::ntp& ntp,
  bool past_tombstone_delete_horizon,
  bool past_tx_delete_horizon,
  bool tx_batch_compaction_enabled) {
    if (
      compaction::is_removable_control_batch(
        ntp, b.header().type, tx_batch_compaction_enabled)) {
        return true;
    }

    // Deal with tombstone record removal
    if (r.is_tombstone() && past_tombstone_delete_horizon) {
        return true;
    }

    // Deal with transactional control batch removal.
    // `tx_batch_compaction_enabled` is already considered in the variable
    // `past_tx_delete_horizon`, so it does not need to be queried again here.
    if (b.header().attrs.is_control() && past_tx_delete_horizon) {
        return true;
    }

    return false;
}

template<typename Func>
ss::future<bool> should_keep(
  const model::record_batch& b,
  const model::record& r,
  const model::ntp& ntp,
  bool is_last_record_in_batch,
  Func&& is_latest_key,
  probe& pb,
  ss::sharded<features::feature_table>& feature_table,
  model::offset segment_last_offset,
  bool past_tombstone_delete_horizon,
  bool& may_have_tombstone_records,
  bool past_tx_delete_horizon,
  bool& has_tx_control_batches,
  bool& has_tx_data_or_fence_batches,
  bool tx_batch_compaction_enabled) {
    const auto compaction_placeholder_enabled = feature_table.local().is_active(
      features::feature::compaction_placeholder_batch);
    const auto is_compactible = compaction::is_compactible(b.header());
    const auto is_last_batch = b.last_offset() == segment_last_offset;
    const auto is_tx_control_batch = b.header().attrs.is_control();
    const auto is_tx_data_batch = (b.header().type == model::record_batch_type::raft_data
         && b.header().attrs.is_transactional());
    const auto is_tx_fence_batch = b.header().type
                                   == model::record_batch_type::tx_fence;
    const auto is_tombstone = r.is_tombstone();
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
        if (is_tombstone) {
            may_have_tombstone_records = true;
        }

        if (is_tx_control_batch) {
            has_tx_control_batches = true;
        }

        if (is_tx_data_batch || is_tx_fence_batch) {
            has_tx_data_or_fence_batches = true;
        }

        co_return true;
    }

    // Before considering `is_latest_key()`, remove
    // records/batches which are always considered removable as well as expired
    // tombstone records/control batches.
    if (
      can_discard(
        b,
        r,
        ntp,
        past_tombstone_delete_horizon,
        past_tx_delete_horizon,
        tx_batch_compaction_enabled)) {
        if (is_tombstone) {
            pb.add_removed_tombstone();
        }
        if (is_tx_control_batch) {
            pb.add_removed_control_batch();
        }
        co_return false;
    }

    if (!is_compactible) {
        if (is_tx_control_batch) {
            has_tx_control_batches = true;
        }
        if (is_tx_fence_batch) {
            has_tx_data_or_fence_batches = true;
        }
        co_return true;
    }

    auto keep = co_await is_latest_key(b, r);

    if (is_tombstone && keep) {
        may_have_tombstone_records = true;
    }

    if (is_tx_data_batch && keep) {
        has_tx_data_or_fence_batches = true;
    }

    co_return keep;
}

} // namespace storage::internal
