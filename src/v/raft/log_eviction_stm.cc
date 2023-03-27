// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/log_eviction_stm.h"

#include "raft/consensus.h"
#include "raft/types.h"

#include <seastar/core/future-util.hh>

namespace raft {

log_eviction_stm::log_eviction_stm(
  consensus* raft,
  ss::logger& logger,
  ss::lw_shared_ptr<storage::stm_manager> stm_manager,
  ss::abort_source& as)
  : _raft(raft)
  , _logger(logger)
  , _stm_manager(std::move(stm_manager))
  , _as(as) {}

ss::future<> log_eviction_stm::start() {
    monitor_log_eviction();
    return ss::now();
}

ss::future<> log_eviction_stm::stop() { return _gate.close(); }

void log_eviction_stm::monitor_log_eviction() {
    ssx::spawn_with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _gate.is_closed(); },
          [this] {
              return _raft->monitor_log_eviction(_as)
                .then([this](model::offset last_evicted) {
                    return handle_deletion_notification(last_evicted);
                })
                .handle_exception_type(
                  [](const ss::abort_requested_exception&) {
                      // ignore abort requested exception, shutting down
                  })
                .handle_exception_type([](const ss::gate_closed_exception&) {
                    // ignore gate closed exception, shutting down
                })
                .handle_exception([this](std::exception_ptr e) {
                    vlog(
                      _logger.trace,
                      "Error handling log eviction - {}, ntp: {}",
                      e,
                      _raft->ntp());
                });
          });
    });
}

ss::future<>
log_eviction_stm::handle_deletion_notification(model::offset last_evicted) {
    vlog(
      _logger.trace,
      "Handling log deletion notification for offset: {}, ntp: {}",
      last_evicted,
      _raft->ntp());

    // Store where the local storage layer has requested eviction up to,
    // irrespective of whether we can actually evict up to this point yet.
    _requested_eviction_offset = last_evicted;

    model::offset max_collectible_offset
      = _stm_manager->max_collectible_offset();
    if (last_evicted > max_collectible_offset) {
        vlog(
          _logger.trace,
          "Can only evict up to offset: {}, ntp: {}",
          max_collectible_offset,
          _raft->ntp());
        last_evicted = max_collectible_offset;
    }

    // do nothing, we already taken the snapshot
    if (last_evicted <= _previous_eviction_offset) {
        return ss::now();
    }

    // persist empty snapshot, we can have no timeout in here as we are passing
    // in an abort source
    _previous_eviction_offset = last_evicted;

    return _raft->visible_offset_monitor()
      .wait(last_evicted, model::no_timeout, _as)
      .then([this, last_evicted]() mutable {
          auto f = ss::now();

          if (_stm_manager) {
              f = _raft->refresh_commit_index().then([this, last_evicted] {
                  return _stm_manager->ensure_snapshot_exists(last_evicted);
              });
          }

          return f.then([this, last_evicted]() {
              return _raft->write_snapshot(
                write_snapshot_cfg(last_evicted, iobuf()));
          });
      });
}
} // namespace raft
