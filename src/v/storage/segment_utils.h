#pragma once
#include "storage/segment.h"

#include <seastar/core/shared_ptr.hh>
namespace storage::internal {
inline ss::future<>
self_compact_segment(ss::lw_shared_ptr<segment>, compaction_config) {
    return ss::now();
}

/// make file handle with default opts
ss::future<ss::file>
make_writer_handle(const std::filesystem::path&, storage::debug_sanitize_files);
/// make file handle with default opts
ss::future<ss::file>
make_reader_handle(const std::filesystem::path&, storage::debug_sanitize_files);

} // namespace storage::internal
