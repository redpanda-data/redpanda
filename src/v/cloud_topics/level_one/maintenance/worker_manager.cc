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
#include "container/chunked_vector.h"
#include "model/timestamp.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "ssx/future-util.h"

namespace cloud_topics::l1 {

worker_manager::worker_manager(
  compaction_queue& compaction_queue,
  leveling_queue& leveling_queue,
  ss::sharded<file_io>* io,
  ss::sharded<replicated_metastore>* metastore,
  ss::sharded<cluster::metadata_cache>* metadata_cache,
  compaction_scheduler_probe& probe,
  ss::sharded<level_one_reader_probe>* l1_reader_probe,
  ss::sharded<cloud_topics::level_zero_notifier>* notifier)
  : _compaction_queue(compaction_queue)
  , _leveling_queue(leveling_queue)
  , _io(io)
  , _metastore(metastore)
  , _metadata_cache(metadata_cache)
  , _probe(probe)
  , _l1_reader_probe(l1_reader_probe)
  , _notifier(notifier) {}

ss::future<> worker_manager::start() {
    co_await _workers.start(
      this,
      ss::sharded_parameter([this] { return &_io->local(); }),
      ss::sharded_parameter([this] { return &_metastore->local(); }),
      ss::sharded_parameter([this] { return &_metadata_cache->local(); }),
      scheduling_groups::instance().cloud_topics_compaction_sg(),
      ss::sharded_parameter([this] { return &_l1_reader_probe->local(); }),
      ss::sharded_parameter([this] {
          return _notifier == nullptr ? nullptr : &_notifier->local();
      }));
    co_await _workers.invoke_on_all(&compaction_worker::start);
}

ss::future<> worker_manager::stop() {
    co_await _gate.close();
    co_await _workers.stop();
}

std::optional<foreign_compaction_job_ptr>
worker_manager::try_acquire_compaction_work(ss::shard_id shard) {
    vassert(
      ss::this_shard_id() == worker_manager_shard,
      "Expected calls to worker_manager::try_acquire_compaction_work() to "
      "always execute on shard {}",
      worker_manager_shard);

    if (_compaction_queue.empty()) {
        return std::nullopt;
    }

    auto job = _compaction_queue.top();
    _compaction_queue.pop();

    if (!job) {
        return std::nullopt;
    }

    // An unmanaged CTP is evicted from the queue by
    // `compaction_scheduler::unmanage_partition`, so a queued job's meta is
    // always still linked into the scheduler's managed-log list.
    dassert(
      job->meta->link.is_linked(),
      "Acquired compaction work for an unmanaged CTP {}",
      job->meta->ntp);

    // Marking the CTP inflight (and skipping inflight CTPs during sampling)
    // is how a CTP is kept out of the queue while being compacted.
    job->meta->compaction.inflight_shard = shard;

    // Ideally, leveling should not run concurrently with compaction of the same
    // CTP. Stop any inflight leveling jobs and clear the queue of any others
    // queued for the same CTP.
    _leveling_queue.clear(job->meta->tidp);
    _probe.set_leveling_queue_length(_leveling_queue.size());
    request_stop_leveling(job->meta);

    return ss::make_foreign(job);
}

void worker_manager::complete_compaction_work(compaction_job* job) {
    vassert(
      ss::this_shard_id() == worker_manager_shard,
      "Expected calls to worker_manager::complete_compaction_work() to always "
      "execute on shard {}",
      worker_manager_shard);

    dassert(
      job->meta->compaction.inflight_shard.has_value(),
      "Expected CTP {} to be inflight when completing work",
      job->meta->ntp);
    job->meta->compaction.inflight_shard.reset();

    _probe.log_compacted();
}

std::optional<foreign_leveling_job_ptr>
worker_manager::try_acquire_leveling_work(ss::shard_id shard) {
    vassert(
      ss::this_shard_id() == worker_manager_shard,
      "Expected calls to worker_manager::try_acquire_leveling_work() to always "
      "execute on shard {}",
      worker_manager_shard);

    while (!_leveling_queue.empty()) {
        auto job = _leveling_queue.top();
        _leveling_queue.pop();
        _probe.set_leveling_queue_length(_leveling_queue.size());

        if (!job || !job->meta || !job->meta->link.is_linked()) {
            // The CTP was unmanaged after this job was queued; drop it.
            continue;
        }

        if (job->meta->compaction.inflight_shard.has_value()) {
            // The CTP has an inflight compaction on-going; drop it.
            continue;
        }

        ++(job->meta->leveling.inflight_shards[shard]);
        // Mark the range inflight (no commit timestamp yet) so the collector
        // won't re-queue an overlapping range until this job completes.
        [[maybe_unused]] const bool inserted
          = job->meta->leveling.inflight_ranges.insert(
            job->range.base_offset, job->range.last_offset, std::nullopt);
        dassert(
          inserted,
          "Failed to mark leveling range {}~{} for CTP {} inflight; it is "
          "empty or overlaps a range already inflight, which the collector "
          "should have skipped when queueing.",
          job->range.base_offset,
          job->range.last_offset,
          job->meta->tidp);
        return ss::make_foreign(job);
    }

    return std::nullopt;
}

void worker_manager::complete_leveling_work(
  leveling_job* job, ss::shard_id shard) {
    vassert(
      ss::this_shard_id() == worker_manager_shard,
      "Expected calls to worker_manager::complete_leveling_work() to always "
      "execute on shard {}",
      worker_manager_shard);

    if (!job || !job->meta) {
        return;
    }

    auto& leveling = job->meta->leveling;
    auto& inflight_shard_cnt = leveling.inflight_shards[shard];
    dassert(
      inflight_shard_cnt > 0,
      "inflight shard count should be greater than 0 when completing leveling "
      "work.");
    --inflight_shard_cnt;

    // The range was recorded as active (`nullopt`) on dequeue. Stamp it with
    // the commit time in place so the collector applies a post-commit cooldown
    // before re-scheduling it, evicting it once a later collection postdates
    // the commit.
    [[maybe_unused]] const bool assigned = leveling.inflight_ranges.assign(
      job->range.base_offset, job->range.last_offset, model::timestamp::now());
    dassert(
      assigned,
      "Failed to stamp completed leveling range {}~{} for CTP {} with a commit "
      "time; no inflight range with exactly those bounds was found.",
      job->range.base_offset,
      job->range.last_offset,
      job->meta->tidp);

    _probe.leveling_range_completed();
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

void worker_manager::request_stop_leveling(log_compaction_meta_ptr log) {
    if (!log) {
        return;
    }

    chunked_vector<ss::shard_id> shards;
    for (const auto& [shard, count] : log->leveling.inflight_shards) {
        if (count > 0) {
            shards.push_back(shard);
        }
    }

    if (shards.empty()) {
        return;
    }

    ssx::spawn_with_gate(
      _gate, [this, tidp = log->tidp, shards = std::move(shards)]() mutable {
          return _workers.invoke_on(
            std::move(shards), [tidp](compaction_worker& worker) {
                return worker.terminate_leveling_jobs_for_tidp(tidp);
            });
      });
}

ss::future<> worker_manager::alert_compaction_workers() {
    auto guard = _gate.hold();
    co_await _workers.invoke_on_all(
      [](compaction_worker& worker) { worker.alert_compaction_fiber(); });
}

ss::future<> worker_manager::alert_leveling_workers() {
    auto guard = _gate.hold();
    co_await _workers.invoke_on_all(
      [](compaction_worker& worker) { worker.alert_leveling_fiber(); });
}

ss::future<>
worker_manager::pause_worker(ss::shard_id worker, maintenance_job_type kind) {
    auto guard = _gate.hold();
    co_await _workers.invoke_on(worker, [kind](compaction_worker& worker) {
        return worker.pause_worker(kind);
    });
}

ss::future<>
worker_manager::resume_worker(ss::shard_id worker, maintenance_job_type kind) {
    auto guard = _gate.hold();
    co_await _workers.invoke_on(worker, [kind](compaction_worker& worker) {
        return worker.resume_worker(kind);
    });
}

ss::future<> worker_manager::pause_all_workers(maintenance_job_type kind) {
    auto guard = _gate.hold();
    co_await _workers.invoke_on_all(
      [kind](compaction_worker& worker) { return worker.pause_worker(kind); });
}

ss::future<> worker_manager::resume_all_workers(maintenance_job_type kind) {
    auto guard = _gate.hold();
    co_await _workers.invoke_on_all(
      [kind](compaction_worker& worker) { return worker.resume_worker(kind); });
}

} // namespace cloud_topics::l1
