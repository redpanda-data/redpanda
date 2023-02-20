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

/// \brief, this method will acquire it's own locks on the segment
///
ss::future<compaction_result> self_compact_segment(
  ss::lw_shared_ptr<storage::segment>,
  ss::lw_shared_ptr<storage::stm_manager>,
  storage::compaction_config,
  storage::probe&,
  storage::readers_cache&,
  storage::storage_resources&,
  offset_delta_time apply_offset);

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
  storage::debug_sanitize_files,
  bool truncate = false);
/// make file handle with default opts
ss::future<ss::file>
make_reader_handle(const std::filesystem::path&, storage::debug_sanitize_files);
ss::future<ss::file> make_handle(
  const std::filesystem::path path,
  ss::open_flags flags,
  ss::file_open_options opt,
  debug_sanitize_files debug);

ss::future<compacted_index_writer> make_compacted_index_writer(
  const std::filesystem::path& path,
  storage::debug_sanitize_files debug,
  ss::io_priority_class iopc,
  storage_resources& resources);

ss::future<segment_appender_ptr> make_segment_appender(
  const segment_full_path& path,
  storage::debug_sanitize_files debug,
  size_t number_of_chunks,
  std::optional<uint64_t> segment_size,
  ss::io_priority_class iopc,
  storage_resources& resources);

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
};

inline bool is_compactible(const model::record_batch& b) {
    return !(
      b.header().type == model::record_batch_type::raft_configuration
      || b.header().type == model::record_batch_type::archival_metadata
      || b.header().type == model::record_batch_type::version_fence);
}

offset_delta_time should_apply_delta_time_offset(
  ss::sharded<features::feature_table>& feature_table);

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
