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
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_reader.h"
#include "storage/compacted_index_writer.h"
#include "storage/compacted_offset_list.h"
#include "storage/probe.h"
#include "storage/readers_cache.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"
#include "utils/named_type.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>

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
  ss::lw_shared_ptr<storage::stm_manager> stm_manager,
  const compaction_config& cfg,
  ss::rwlock::holder& read_holder,
  storage_resources& resources,
  storage::probe& pb);

/// \brief, this method will acquire it's own locks on the segment
///
ss::future<compaction_result> self_compact_segment(
  ss::lw_shared_ptr<storage::segment>,
  ss::lw_shared_ptr<storage::stm_manager>,
  const storage::compaction_config&,
  storage::probe&,
  storage::readers_cache&,
  storage::storage_resources&,
  ss::sharded<features::feature_table>& feature_table);

/// \brief, rebuilds a given segment's compacted index. This method acquires
/// locks on the segment.
ss::future<> rebuild_compaction_index(
  ss::lw_shared_ptr<segment> s,
  ss::lw_shared_ptr<storage::stm_manager> stm_manager,
  compaction_config cfg,
  storage::probe& pb,
  storage_resources& resources);

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
ss::future<
  std::tuple<ss::lw_shared_ptr<segment>, std::vector<segment::generation_id>>>
make_concatenated_segment(
  segment_full_path,
  std::vector<ss::lw_shared_ptr<segment>>,
  compaction_config,
  storage_resources& resources,
  ss::sharded<features::feature_table>& feature_table);

ss::future<> write_concatenated_compacted_index(
  std::filesystem::path,
  std::vector<ss::lw_shared_ptr<segment>>,
  compaction_config,
  storage_resources& resources);

ss::future<std::vector<ss::rwlock::holder>> transfer_segment(
  ss::lw_shared_ptr<segment> to,
  ss::lw_shared_ptr<segment> from,
  compaction_config cfg,
  probe& probe,
  std::vector<ss::rwlock::holder>);

/*
 * Acquire write locks on multiple segments. The process will proceed until
 * success, or timeout. Failure to acquire the locks may result from contention
 * or deadlock. There is no intelligent handling for deadlock avoidance or
 * fairness. If a lock cannot be acquired all held locks are released and the
 * process is retried. Favor more retries over longer timeouts.
 */
ss::future<std::vector<ss::rwlock::holder>> write_lock_segments(
  std::vector<ss::lw_shared_ptr<segment>>& segments,
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

ss::future<compacted_index_writer> make_compacted_index_writer(
  const std::filesystem::path& path,
  ss::io_priority_class iopc,
  storage_resources& resources,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config);

ss::future<segment_appender_ptr> make_segment_appender(
  const segment_full_path& path,
  size_t number_of_chunks,
  std::optional<uint64_t> segment_size,
  ss::io_priority_class iopc,
  storage_resources& resources,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config);

size_t number_of_chunks_from_config(const storage::ntp_config&);
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

ss::future<> copy_filtered_entries(
  storage::compacted_index_reader input,
  roaring::Roaring to_copy_index_filter,
  storage::compacted_index_writer output);

/// \brief writes a new `*.compacted_index` file and *closes* the
/// input compacted_index_reader file
ss::future<> write_clean_compacted_index(
  storage::compacted_index_reader,
  storage::compaction_config,
  storage_resources& resources);

ss::future<compacted_offset_list>
  generate_compacted_list(model::offset, storage::compacted_index_reader);

ss::future<bool>
  detect_if_segment_already_compacted(std::filesystem::path, compaction_config);

bool compacted_index_needs_rebuild(compacted_index::recovery_state state);

ss::future<compacted_index::recovery_state>
detect_compaction_index_state(segment_full_path p, compaction_config cfg);

/// \brief creates a model::record_batch_reader from segment meta
///
model::record_batch_reader create_segment_full_reader(
  ss::lw_shared_ptr<storage::segment>,
  storage::compaction_config,
  storage::probe&,
  ss::rwlock::holder);

ss::future<storage::index_state> do_copy_segment_data(
  ss::lw_shared_ptr<storage::segment>,
  storage::compaction_config,
  storage::probe&,
  ss::rwlock::holder,
  storage_resources&);

ss::future<> do_swap_data_file_handles(
  std::filesystem::path compacted,
  ss::lw_shared_ptr<storage::segment>,
  storage::compaction_config,
  probe&);

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

inline bool
is_compactible_control_batch(const model::record_batch_type batch_type) {
    // Control batches in consumer offsets are special compared to
    // the ones in data partitions can be safely compacted away.
    return batch_type == model::record_batch_type::group_fence_tx
           || batch_type == model::record_batch_type::group_prepare_tx
           || batch_type == model::record_batch_type::group_abort_tx
           || batch_type == model::record_batch_type::group_commit_tx;
}

inline bool is_compactible(const model::record_batch_header& h) {
    if (
      (h.attrs.is_control() && !is_compactible_control_batch(h.type))
      || h.type == model::record_batch_type::compaction_placeholder) {
        // Keep control batches to ensure we maintain transaction boundaries.
        // They should be rare.
        return false;
    }
    static const auto filtered_types = model::offset_translator_batch_types();
    auto n = std::count(filtered_types.begin(), filtered_types.end(), h.type);
    return n == 0;
}

inline bool is_compactible(const model::record_batch& b) {
    return is_compactible(b.header());
}

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
  ss::lw_shared_ptr<segment> seg, const compaction_config& cfg);

// Checks if a segment may have any tombstones currently eligible for deletion.
//
// Returns true if the segment is marked as potentially having tombstone
// records, and if the result of evaluating
// `is_past_tombstone_delete_horizon(seg, cfg)` is also true. This can return
// false-positives, since segments that have not yet gone through the compaction
// process are assumed to potentially contain tombstones until proven otherwise.
bool may_have_removable_tombstones(
  ss::lw_shared_ptr<segment> seg, const compaction_config& cfg);

// Mark a segment as completed window compaction, and whether it is "clean" (in
// which case the `clean_compact_timestamp` is set in the segment's index).
void mark_segment_as_finished_window_compaction(
  ss::lw_shared_ptr<segment> seg, bool set_clean_compact_timestamp);

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

} // namespace storage::internal
