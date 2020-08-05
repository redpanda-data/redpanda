#include "raft/log_eviction_stm.h"

#include "raft/consensus.h"
#include "raft/types.h"

#include <seastar/core/future-util.hh>

namespace raft {

log_eviction_stm::log_eviction_stm(consensus* raft, ss::logger& logger)
  : _raft(raft)
  , _logger(logger) {}

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
              return _raft->monitor_log_eviction()
                .then([this](storage::eviction_range_lock lock) {
                    return handle_deletion_notification(std::move(lock));
                })
                .handle_exception([this](std::exception_ptr e) {
                    vlog(_logger.trace, "Error handling log eviction - {}", e);
                });
          });
    });
}

ss::future<> log_eviction_stm::handle_deletion_notification(
  storage::eviction_range_lock lock) {
    vlog(
      _logger.trace,
      "Handling log deletion notification for offset: {}",
      lock.last_evicted);
    // persist empty snapshot
    return _raft
      ->write_snapshot(write_snapshot_cfg(
        lock.last_evicted,
        iobuf(),
        write_snapshot_cfg::should_prefix_truncate::no))
      .finally([lock = std::move(lock)] {});
}
} // namespace raft