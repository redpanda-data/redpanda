#include "cloud_storage/prefetch_tracker.h"

#include "cloud_storage/logger.h"
#include "config/configuration.h"

namespace cloud_storage {

prefetch_tracker::prefetch_tracker()
  : _enabled(config::shard_local_cfg().cloud_storage_prefetch_enable.value())
  , _prefetch_threshold(
      config::shard_local_cfg().cloud_storage_prefetch_threshold.value())
  , _prefetch_size(
      config::shard_local_cfg().cloud_storage_prefetch_size_trigger.value()) {}

prefetch_tracker::prefetch_tracker(
  bool enabled, size_t threshold, size_t limit) noexcept
  : _enabled(enabled)
  , _prefetch_threshold(threshold)
  , _prefetch_size(limit) {}

void prefetch_tracker::on_new_segment() {
    _segment_ready = false;
    _segment_bytes = 0;
    _segment_size = 0;
}

void prefetch_tracker::set_segment_size(size_t sz) { _segment_size = sz; }

void prefetch_tracker::on_bytes_consumed(size_t sz) {
    _total_bytes += sz;
    _segment_bytes += sz;
}

bool prefetch_tracker::operator()() {
    if (!_enabled) {
        return false;
    }
    if (_total_bytes < _prefetch_threshold) {
        vlog(
          cst_log.trace,
          "prefetch_tracker: prefetch disabled, {} is less than threshold {}",
          _total_bytes,
          _prefetch_threshold);
        return false;
    }
    auto bytes_remains = _segment_size - _segment_bytes;
    if (
      !_segment_ready && _segment_size >= _prefetch_size
      && bytes_remains <= _prefetch_size) {
        vlog(
          cst_log.debug,
          "prefetch_tracker: prefetch enabled, segment bytes {} is less than "
          "prefetch limit {}",
          _segment_bytes,
          _prefetch_size);
        _segment_ready = true;
        return true;
    }
    vlog(
      cst_log.trace,
      "prefetch_tracker: prefetch disabled, segment bytes {} is greater than "
      "prefetch limit {} or segment ready is {}",
      _segment_bytes,
      _prefetch_size,
      _segment_ready);
    return false;
}

const prefetch_tracker prefetch_tracker::disabled = prefetch_tracker(
  false, 0, 0);

} // namespace cloud_storage
