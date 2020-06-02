#pragma once
#include "storage/segment.h"

#include <seastar/core/shared_ptr.hh>
namespace storage::internal {
inline ss::future<>
self_compact_segment(ss::lw_shared_ptr<segment>, compaction_config) {
    return ss::now();
}

} // namespace storage::internal
