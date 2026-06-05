/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/worker_manager.h"

#include "cloud_topics/level_one/common/file_io.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/worker.h"
#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "ssx/future-util.h"

namespace cloud_topics::l1 {

worker_manager::worker_manager(
  log_compaction_queue& compaction_queue,
  ss::sharded<file_io>* io,
  ss::sharded<replicated_metastore>* metastore,
  ss::sharded<cluster::metadata_cache>* metadata_cache,
  compaction_scheduler_probe& probe,
  ss::sharded<level_one_reader_probe>* l1_reader_probe,
  ss::sharded<l1_reader_cache>* reader_cache)
  : _compaction_queue(compaction_queue)
  , _io(io)
  , _metastore(metastore)
  , _metadata_cache(metadata_cache)
  , _probe(probe)
  , _l1_reader_probe(l1_reader_probe)
  , _reader_cache(reader_cache) {}

ss::future<> worker_manager::start() {
    co_await _workers.start(
      this,
      ss::sharded_parameter([this] { return &_io->local(); }),
      ss::sharded_parameter([this] { return &_metastore->local(); }),
      ss::sharded_parameter([this] { return &_metadata_cache->local(); }),
      scheduling_groups::instance().cloud_topics_compaction_sg(),
      ss::sharded_parameter([this] { return &_l1_reader_probe->local(); }),
      _reader_cache);
    co_await _workers.invoke_on_all(&compaction_worker::start);
}

ss::future<> worker_manager::stop() {
    co_await _gate.close();
    co_await _workers.stop();
}

std::optional<foreign_log_compaction_meta_ptr>
worker_manager::try_acquire_compaction_work(ss::shard_id shard) {
    vassert(
      ss::this_shard_id() == worker_manager_shard,
      "Expected calls to worker_manager::try_acquire_compaction_work() to "
      "always "
      "execute on shard {}",
      worker_manager_shard);

    if (_compaction_queue.empty()) {
        return std::nullopt;
    }

    auto log = _compaction_queue.top();
    _compaction_queue.pop();

    if (!log) {
        return std::nullopt;
    }

    if (!log->link.is_linked()) {
        return std::nullopt;
    }

    dassert(
      log->compaction.s == log_compaction_state::status::queued,
      "Expected log state to be queued when acquiring work");
    log->compaction.s = log_compaction_state::status::inflight;
    log->compaction.inflight_shard = shard;
    return ss::make_foreign(log);
}

void worker_manager::complete_compaction_work(log_compaction_meta* log) {
    vassert(
      ss::this_shard_id() == worker_manager_shard,
      "Expected calls to worker_manager::complete_compaction_work() to always "
      "execute on "
      "shard {}",
      worker_manager_shard);

    dassert(
      log->compaction.s == log_compaction_state::status::inflight,
      "Expected log state to be inflight when completing work");
    log->compaction.s = log_compaction_state::status::idle;
    log->compaction.inflight_shard.reset();
    log->compaction.info_and_ts.reset();

    _probe.log_compacted();
}

void worker_manager::request_stop_compaction(log_compaction_meta_ptr log) {
    if (!log) {
        return;
    }

    auto shard_opt = log->compaction.inflight_shard;
    if (!shard_opt.has_value()) {
        return;
    }

    auto shard = shard_opt.value();

    ssx::spawn_with_gate(_gate, [this, shard]() {
        return _workers.invoke_on(shard, [](compaction_worker& worker) {
            return worker.terminate_compaction_job();
        });
    });
}

ss::future<> worker_manager::alert_compaction_workers() {
    auto guard = _gate.hold();
    co_await _workers.invoke_on_all(
      [](compaction_worker& worker) { worker.alert_compaction_fiber(); });
}

ss::future<> worker_manager::pause_worker(ss::shard_id worker) {
    auto guard = _gate.hold();
    co_await _workers.invoke_on(
      worker, [](compaction_worker& worker) { return worker.pause_worker(); });
}

ss::future<> worker_manager::resume_worker(ss::shard_id worker) {
    auto guard = _gate.hold();
    co_await _workers.invoke_on(
      worker, [](compaction_worker& worker) { return worker.resume_worker(); });
}

} // namespace cloud_topics::l1
