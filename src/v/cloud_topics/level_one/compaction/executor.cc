/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/executor.h"

namespace cloud_topics::l1 {

compaction_executor::compaction_executor(
  ss::sharded<file_io>* io, ss::sharded<compaction_committer>* committer)
  : _io(io)
  , _committer(committer) {}

ss::future<> compaction_executor::start() {
    co_await _workers.start(
      ss::sharded_parameter([this] { return &_io->local(); }),
      ss::sharded_parameter([this] { return &_committer->local(); }));

    for (worker_shard i = 0; i < ss::smp::count; ++i) {
        _avail_workers.emplace_back(i);
    }

    // Awake any waiters that may already exist.
    _cvar.broadcast();
}

ss::future<> compaction_executor::stop() { co_await _workers.stop(); }

ss::future<> compaction_executor::compact_log(log_info_and_meta log) {
    auto worker_fut = co_await ss::coroutine::as_future(get_available_worker());
    if (worker_fut.failed()) {
        auto eptr = worker_fut.get_exception();
        auto log_lvl = ssx::is_shutdown_exception(eptr) ? ss::log_level::warn
                                                        : ss::log_level::debug;
        vlogl(
          compaction_log,
          log_lvl,
          "Caught exception {} while waiting for compaction worker.",
          eptr);
        co_return;
    }

    auto worker = worker_fut.get();
    do_compact_log(worker, std::move(log));
}

ss::future<> compaction_executor::request_stop_compaction(model::ntp ntp) {
    auto it = _inflight.find(ntp);
    if (it == _inflight.end()) {
        co_return;
    }

    auto shard = it->second;
    co_await _workers.invoke_on(
      shard, [ntp = std::move(ntp)](compaction_worker& worker) {
          return worker.request_stop_compact(std::move(ntp));
      });
}

ss::future<> compaction_executor::request_stop_executor() {
    _as.request_abort();
    _cvar.broken();
    co_await _workers.invoke_on_all(
      [](compaction_worker& worker) { worker.set_stopped(); });
}

void compaction_executor::do_compact_log(
  compaction_executor::worker_shard shard, log_info_and_meta log) {
    if (!log.meta->link.is_linked()) {
        return;
    }

    ssx::spawn_with_gate(log.meta->gate, [shard, log = std::move(log), this] {
        auto ntp = log.meta->ntp;
        _inflight.emplace(ntp, shard);
        return _workers
          .invoke_on(
            shard,
            [this, log = std::move(log)](compaction_worker& worker) {
                return worker.compact(std::move(log), _as);
            })
          .finally([shard, ntp = std::move(ntp), this] {
              _inflight.erase(ntp);
              _avail_workers.emplace_back(shard);
              _cvar.signal();
          });
    });
}

ss::future<compaction_executor::worker_shard>
compaction_executor::get_available_worker() {
    while (!_as.abort_requested()) {
        if (!_avail_workers.empty()) {
            auto worker = _avail_workers.front();
            _avail_workers.pop_front();
            co_return worker;
        }

        co_await ssx::with_timeout_abortable(
          _cvar.wait(), model::no_timeout, _as);
    }

    __builtin_unreachable();
}

} // namespace cloud_topics::l1
