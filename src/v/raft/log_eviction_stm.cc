#include "raft/log_eviction_stm.h"

#include "raft/consensus.h"
#include "raft/types.h"

#include <seastar/core/future-util.hh>

namespace raft {

log_eviction_stm::log_eviction_stm(
  consensus* raft, ss::logger& logger, ss::abort_source& as)
  : _raft(raft)
  , _logger(logger)
  , _as(as) {}

ss::future<> log_eviction_stm::start() {
    monitor_log_eviction();
    return ss::now();
}

ss::future<> log_eviction_stm::stop() { return _gate.close(); }

void log_eviction_stm::monitor_log_eviction() {
    (void)ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _gate.is_closed(); },
          [this] {
              return _raft->monitor_log_eviction(_as)
                .then([this](model::offset last_evicted) {
                    return handle_deletion_notification(last_evicted);
                })
                .handle_exception([this](std::exception_ptr e) {
                    vlog(_logger.trace, "Error handling log eviction - {}", e);
                });
          });
    });
}

ss::future<>
log_eviction_stm::handle_deletion_notification(model::offset last_evicted) {
    vlog(
      _logger.trace,
      "Handling log deletion notification for offset: {}",
      last_evicted);
    // do nothing, we already taken the snapshot
    if (last_evicted <= _previous_eviction_offset) {
        return ss::now();
    }
    // persist empty snapshot, we can have no timeout in here as we are passing
    // in an abort source
    _previous_eviction_offset = last_evicted;

    return _raft->events()
      .wait(last_evicted, model::no_timeout, _as)
      .then([this, last_evicted]() mutable {
          return _raft->write_snapshot(write_snapshot_cfg(
            last_evicted,
            iobuf(),
            write_snapshot_cfg::should_prefix_truncate::no));
      });
}
} // namespace raft
