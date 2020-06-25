#pragma once
#include "storage/compacted_index.h"
#include "storage/compacted_index_reader.h"
#include "storage/compacted_index_writer.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>

#include <roaring/roaring.hh>

namespace storage::internal {
using namespace storage; // NOLINT

/// \brief, this method will acquire it's own locks on the segment
///
ss::future<>
  self_compact_segment(ss::lw_shared_ptr<segment>, compaction_config);

/// make file handle with default opts
ss::future<ss::file>
make_writer_handle(const std::filesystem::path&, storage::debug_sanitize_files);
/// make file handle with default opts
ss::future<ss::file>
make_reader_handle(const std::filesystem::path&, storage::debug_sanitize_files);

ss::future<compacted_index_writer> make_compacted_index_writer(
  const std::filesystem::path& path,
  debug_sanitize_files debug,
  ss::io_priority_class iopc);

ss::future<segment_appender_ptr> make_segment_appender(
  const std::filesystem::path& path,
  debug_sanitize_files debug,
  size_t number_of_chunks,
  ss::io_priority_class iopc);

size_t number_of_chunks_from_config(const ntp_config&);

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
  compacted_index_reader input,
  Roaring to_copy_index_filter,
  compacted_index_writer output);

/// \brief writes a new `*.compacted_index` file and *closes* the
/// input compacted_index_reader file
ss::future<>
  write_clean_compacted_index(compacted_index_reader, compaction_config);

struct offset_compaction_list {
    using relative = std::pair<uint32_t, int32_t>;

    explicit offset_compaction_list(model::offset base)
      : base_offset(base) {}
    model::offset base_offset;
    ss::circular_buffer<relative> relative_delta;
};

ss::future<offset_compaction_list>
  generate_compacted_list(compacted_index_reader);

// TODO: needs to look over on the locking mechanics
ss::future<> write_compacted_segment(
  ss::lw_shared_ptr<segment>, segment_appender_ptr, offset_compaction_list);

} // namespace storage::internal
