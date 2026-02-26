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
  log_leveling_queue& leveling_queue,
  ss::sharded<file_io>* io,
  ss::sharded<replicated_metastore>* metastore,
  ss::sharded<cluster::metadata_cache>* metadata_cache,
  maintenance_scheduler_probe& probe,
  ss::sharded<level_one_reader_probe>* l1_reader_probe)
  : _compaction_queue(compaction_queue)
  , _leveling_queue(leveling_queue)
  , _io(io)
  , _metastore(metastore)
  , _metadata_cache(metadata_cache)
  , _probe(probe)
  , _l1_reader_probe(l1_reader_probe) {}

ss::future<> worker_manager::start() {
    co_await _workers.start(
      this,
      ss::sharded_parameter([this] { return &_io->local(); }),
      ss::sharded_parameter([this] { return &_metastore->local(); }),
      ss::sharded_parameter([this] { return &_metadata_cache->local(); }),
      scheduling_groups::instance().cloud_topics_compaction_sg(),
      ss::sharded_parameter([this] { return &_l1_reader_probe->local(); }));
    co_await _workers.invoke_on_all(&maintenance_worker::start);
}

ss::future<> worker_manager::stop() {
    co_await _gate.close();
    co_await _workers.stop();
}

std::optional<foreign_log_maintenance_meta_ptr>
worker_manager::try_acquire_from(
  log_maintenance_queue& queue, ss::shard_id shard) {
    vassert(
      ss::this_shard_id() == worker_manager_shard,
      "Expected calls to worker_manager::try_acquire_*_work() to always "
      "execute on shard {}",
      worker_manager_shard);

    if (queue.empty()) {
        return std::nullopt;
    }

    auto log = queue.top();
    queue.pop();

    if (!log) {
        return std::nullopt;
    }

    if (!log->link.is_linked()) {
        return std::nullopt;
    }

    dassert(
      log->state == log_maintenance_meta::log_state::queued,
      "Expected log state to be queued when acquiring work");
    log->state = log_maintenance_meta::log_state::inflight;
    log->inflight_shard = shard;
    return ss::make_foreign(log);
}

std::optional<foreign_log_maintenance_meta_ptr>
worker_manager::try_acquire_compaction_work(ss::shard_id shard) {
    return try_acquire_from(_compaction_queue, shard);
}

std::optional<foreign_log_maintenance_meta_ptr>
worker_manager::try_acquire_leveling_work(ss::shard_id shard) {
    return try_acquire_from(_leveling_queue, shard);
}

void worker_manager::complete_work(log_maintenance_meta* log) {
    vassert(
      ss::this_shard_id() == worker_manager_shard,
      "Expected calls to worker_manager::complete_work() to always execute on "
      "shard {}",
      worker_manager_shard);

    dassert(
      log->state == log_maintenance_meta::log_state::inflight,
      "Expected log state to be inflight when completing work");
    bool was_compaction = log->compaction_info_and_ts.has_value();
    log->state = log_maintenance_meta::log_state::idle;
    log->inflight_shard.reset();
    log->compaction_info_and_ts.reset();
    log->leveling_info_and_ts.reset();

    if (was_compaction) {
        _probe.log_compacted();
    } else {
        _probe.log_leveled();
    }
}

void worker_manager::request_stop_maintenance(log_maintenance_meta_ptr log) {
    if (!log) {
        return;
    }

    auto shard_opt = log->inflight_shard;
    if (!shard_opt.has_value()) {
        return;
    }

    auto shard = shard_opt.value();

    auto ntp = log->ntp;
    ssx::spawn_with_gate(_gate, [this, shard, ntp = std::move(ntp)]() mutable {
        return _workers.invoke_on(
          shard, [ntp = std::move(ntp)](maintenance_worker& worker) {
              worker.terminate_current_job(ntp);
          });
    });
}

ss::future<> worker_manager::alert_compaction() {
    auto guard = _gate.hold();
    co_await _workers.invoke_on_all(
      [](maintenance_worker& worker) { worker.alert_compaction(); });
}

ss::future<> worker_manager::alert_leveling() {
    auto guard = _gate.hold();
    co_await _workers.invoke_on_all(
      [](maintenance_worker& worker) { worker.alert_leveling(); });
}

ss::future<> worker_manager::pause_worker(ss::shard_id worker) {
    auto guard = _gate.hold();
    co_await _workers.invoke_on(
      worker, [](maintenance_worker& worker) { return worker.pause_worker(); });
}

ss::future<> worker_manager::resume_worker(ss::shard_id worker) {
    auto guard = _gate.hold();
    co_await _workers.invoke_on(worker, [](maintenance_worker& worker) {
        return worker.resume_worker();
    });
}

} // namespace cloud_topics::l1
