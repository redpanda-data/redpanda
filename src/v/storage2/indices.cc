
#include "storage2/indices.h"

#include "storage2/fileset.h"

namespace storage {

template<>
segment_indices::indices(offset_index oix, timestamp_index tix)
  : instances_(oix, tix) {}

/**
 * for a given segment, this opens all indices that are indexing it.
 */
template<>
seastar::future<segment_indices> segment_indices::open(open_fileset& ofs) {
    auto& oix = *ofs.begin();
    auto& tix = *std::next(ofs.begin());
    return when_all_succeed(offset_index::open(oix), timestamp_index::open(tix))
      .then([](offset_index oix, timestamp_index tix) {
          return make_ready_future<segment_indices>(
            segment_indices(std::move(oix), std::move(tix)));
      });
}

} // namespace storage