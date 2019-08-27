#include "storage/log.h"

#include "storage/version.h"

namespace storage {

log::log(
  model::namespaced_topic_partition ntp,
  log_manager& manager,
  log_set segs) noexcept
  : _ntp(std::move(ntp))
  , _manager(manager)
  , _segs(std::move(segs)) {
    if (_segs.size()) {
        _tracker.update_committed_offset(_segs.last()->max_offset());
        _tracker.update_dirty_offset(_segs.last()->max_offset());
    }
}

future<> log::close() {
    return parallel_for_each(_segs, [](log_segment_ptr& seg) {
        return seg->close();
    });
}

} // namespace storage
