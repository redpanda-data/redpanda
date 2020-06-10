#pragma once
#include "storage/segment.h"

#include <seastar/core/shared_ptr.hh>
namespace storage::internal {
inline ss::future<>
self_compact_segment(ss::lw_shared_ptr<segment>, compaction_config) {
    return ss::now();
}
using namespace storage; // NOLINT

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

} // namespace storage::internal
