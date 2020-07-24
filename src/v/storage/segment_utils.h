#pragma once
#include "model/record_batch_reader.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_reader.h"
#include "storage/compacted_index_writer.h"
#include "storage/compacted_offset_list.h"
#include "storage/probe.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>

#include <roaring/roaring.hh>

namespace storage::internal {

/// \brief, this method will acquire it's own locks on the segment
///
ss::future<> self_compact_segment(
  ss::lw_shared_ptr<storage::segment>,
  storage::compaction_config,
  storage::probe&);

/// make file handle with default opts
ss::future<ss::file>
make_writer_handle(const std::filesystem::path&, storage::debug_sanitize_files);
/// make file handle with default opts
ss::future<ss::file>
make_reader_handle(const std::filesystem::path&, storage::debug_sanitize_files);

ss::future<compacted_index_writer> make_compacted_index_writer(
  const std::filesystem::path& path,
  storage::debug_sanitize_files debug,
  ss::io_priority_class iopc);

ss::future<segment_appender_ptr> make_segment_appender(
  const std::filesystem::path& path,
  storage::debug_sanitize_files debug,
  size_t number_of_chunks,
  ss::io_priority_class iopc);

size_t number_of_chunks_from_config(const storage::ntp_config&);

/*
1. if footer.flags == truncate write new .compacted_index file
2. produce list of dedup'd (base_offset,delta) - in memory
3. consume that list, and produce small batches of 500_KiB
4. write new batches to disk
*/

/// \brief this is a 0-based index (i.e.: i++) of the entries we need to
/// save starting at 0 on a *new* `.compacted_index` file this represents
/// the fully dedupped entries, clean of truncations, etc
ss::future<Roaring> natural_index_of_entries_to_keep(compacted_index_reader);

ss::future<> copy_filtered_entries(
  storage::compacted_index_reader input,
  Roaring to_copy_index_filter,
  storage::compacted_index_writer output);

/// \brief writes a new `*.compacted_index` file and *closes* the
/// input compacted_index_reader file
ss::future<> write_clean_compacted_index(
  storage::compacted_index_reader, storage::compaction_config);

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
  model::record_batch_reader);

ss::future<> do_swap_data_file_handles(
  std::filesystem::path compacted,
  ss::lw_shared_ptr<storage::segment>,
  storage::compaction_config);

ss::future<model::record_batch> decompress_batch(model::record_batch&&);

} // namespace storage::internal
