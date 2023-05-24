// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_registry_stm.h"

#include "cluster/logger.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

namespace cluster {

tx_registry_stm::tx_registry_stm(ss::logger& logger, raft::consensus* c)
  : tx_registry_stm(logger, c, config::shard_local_cfg()) {}

tx_registry_stm::tx_registry_stm(
  ss::logger& logger, raft::consensus* c, config::configuration& cfg)
  : persisted_stm("tx_registry.snapshot", logger, c)
  , _sync_timeout(cfg.tx_registry_sync_timeout_ms.bind())
  , _log_capacity(cfg.tx_registry_log_capacity.bind()) {}

ss::future<checked<model::term_id, errc>> tx_registry_stm::sync() {
    return ss::with_gate(_gate, [this] { return do_sync(_sync_timeout()); });
}

ss::future<checked<model::term_id, errc>>
tx_registry_stm::do_sync(model::timeout_clock::duration timeout) {
    if (!_c->is_leader()) {
        co_return errc::not_leader;
    }

    auto ready = co_await persisted_stm::sync(timeout);
    if (!ready) {
        co_return errc::generic_tx_error;
    }
    co_return _insync_term;
}

ss::future<> tx_registry_stm::apply(model::record_batch b) {
    _insync_offset = b.last_offset();

    _processed++;
    if (_processed > _log_capacity()) {
        ssx::spawn_with_gate(_gate, [this] { return truncate_log_prefix(); });
    }

    return ss::now();
}

ss::future<> tx_registry_stm::truncate_log_prefix() {
    if (_is_truncating) {
        return ss::now();
    }
    if (_processed <= _log_capacity()) {
        return ss::now();
    }
    _is_truncating = true;
    return _c->write_snapshot(raft::write_snapshot_cfg(_next_snapshot, iobuf()))
      .then([this] {
          _next_snapshot = _insync_offset;
          _processed = 0;
      })
      .finally([this] { _is_truncating = false; });
}

ss::future<> tx_registry_stm::apply_snapshot(stm_snapshot_header, iobuf&&) {
    return ss::make_exception_future<>(
      std::logic_error("tx_registry_stm doesn't support snapshots"));
}

ss::future<stm_snapshot> tx_registry_stm::take_snapshot() {
    return ss::make_exception_future<stm_snapshot>(
      std::logic_error("tx_registry_stm doesn't support snapshots"));
}

ss::future<> tx_registry_stm::handle_raft_snapshot() {
    return write_lock().then(
      [this]([[maybe_unused]] ss::basic_rwlock<>::holder unit) {
          _next_snapshot = _c->start_offset();
          _processed = 0;
          set_next(_next_snapshot);
          _insync_offset = model::prev_offset(_next_snapshot);
          return ss::now();
      });
}

} // namespace cluster
