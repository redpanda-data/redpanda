/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/worker_manager.h"

#include "cloud_topics/level_one/common/file_io.h"
#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/worker.h"
#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

worker_manager::worker_manager(
  log_compaction_queue& work_queue,
  ss::sharded<file_io>* io,
  ss::sharded<replicated_metastore>* metastore,
  ss::sharded<compaction_committer>* committer,
  compaction_scheduler_probe& probe)
  : _work_queue(work_queue)
  , _io(io)
  , _metastore(metastore)
  , _committer(committer)
  , _probe(probe) {}

ss::future<> worker_manager::start() {
    co_await _workers.start(
      this,
      ss::sharded_parameter([this] { return &_io->local(); }),
      ss::sharded_parameter([this] { return &_metastore->local(); }),
      ss::sharded_parameter([this] { return &_committer->local(); }));
    co_await _workers.invoke_on_all(&compaction_worker::start);
}

ss::future<> worker_manager::stop() { co_await _workers.stop(); }

std::optional<foreign_log_compaction_meta_ptr>
worker_manager::try_acquire_work(ss::shard_id shard) {
    vassert(
      ss::this_shard_id() == worker_manager_shard,
      "Expected calls to worker_manager::try_acquire_work() to always "
      "execute on shard {}",
      worker_manager_shard);

    if (_work_queue.empty()) {
        return std::nullopt;
    }

    auto log = _work_queue.top();
    _work_queue.pop();

    if (!log) {
        return std::nullopt;
    }

    if (!log->link.is_linked()) {
        return std::nullopt;
    }

    log->inflight = shard;
    return ss::make_foreign(log);
}

void worker_manager::complete_work(log_compaction_meta* log) {
    vassert(
      ss::this_shard_id() == worker_manager_shard,
      "Expected calls to worker_manager::complete_work() to always execute on "
      "shard {}",
      worker_manager_shard);
    log->inflight.reset();
    _probe.log_compacted();
}

ss::future<>
worker_manager::request_stop_compaction(log_compaction_meta_ptr log) {
    if (!log) {
        co_return;
    }

    auto shard_opt = log->inflight;
    if (!shard_opt.has_value()) {
        co_return;
    }

    auto shard = shard_opt.value();

    co_await _workers.invoke_on(shard, [](compaction_worker& worker) {
        return worker.terminate_current_job();
    });
}

ss::future<> worker_manager::alert_workers() {
    co_await _workers.invoke_on_all(
      [](compaction_worker& worker) { worker.alert_worker(); });
}

ss::future<> worker_manager::pause_worker(ss::shard_id worker) {
    co_await _workers.invoke_on(
      worker, [](compaction_worker& worker) { return worker.pause_worker(); });
}

ss::future<> worker_manager::resume_worker(ss::shard_id worker) {
    co_await _workers.invoke_on(
      worker, [](compaction_worker& worker) { return worker.resume_worker(); });
}

} // namespace cloud_topics::l1
