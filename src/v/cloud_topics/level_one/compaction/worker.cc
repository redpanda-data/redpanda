/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/worker.h"

#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/sink.h"
#include "cloud_topics/level_one/compaction/source.h"
#include "cloud_topics/level_one/compaction/worker_manager.h"
#include "compaction/reducer.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l1 {

compaction_worker::compaction_worker(
  worker_manager* worker_manager,
  io* io,
  metastore* metastore,
  compaction_committer* committer)
  : _worker_update_queue([](const std::exception_ptr& ex) {
      vlog(
        compaction_log.error,
        "Unexpected compaction worker update queue error: {}",
        ex);
  })
  , _worker_manager(worker_manager)
  , _io(io)
  , _metastore(metastore)
  , _committer(committer) {}

ss::future<> compaction_worker::start() {
    _probe.setup_metrics();
    start_work_loop();
    co_return;
}

ss::future<> compaction_worker::stop() {
    terminate_current_job();
    _worker_state = worker_state::stopped;
    co_await _worker_update_queue.shutdown();

    _as.request_abort();
    _worker_cv.broken();

    auto close_fut = _gate.close();

    co_await clear_work_fut();

    co_await std::move(close_fut);
}

void compaction_worker::start_work_loop() {
    vassert(
      !_work_fut.has_value(),
      "Cannot set value of _work_fut when it already has a value.");
    _work_fut = ssx::spawn_with_gate_then(
      _gate, [this]() { return work_loop(); });
}

ss::future<> compaction_worker::work_loop() {
    constexpr std::chrono::seconds poll_frequency(60);

    while (is_active()) {
        try {
            co_await _worker_cv.wait(poll_frequency);
        } catch (const ss::semaphore_timed_out&) {
            // Fall through
        }

        while (is_active()) {
            auto maybe_work = co_await try_acquire_work_from_manager();

            if (!maybe_work.has_value()) {
                break;
            }

            auto work = std::move(maybe_work).value();

            auto tidp = work->tidp;

            auto compact_fut = co_await ss::coroutine::as_future(
              compact_log(work.get()));
            co_await complete_work_on_manager(std::move(work));

            if (compact_fut.failed()) {
                auto eptr = compact_fut.get_exception();
                auto log_lvl = ssx::is_shutdown_exception(eptr)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
                vlogl(
                  compaction_log,
                  log_lvl,
                  "Caught exception {} while compacting CTP {}.",
                  eptr,
                  tidp);
            }
        }
    }
}

ss::future<> compaction_worker::clear_work_fut() {
    if (_work_fut.has_value()) {
        co_await std::move(_work_fut).value();
        _work_fut.reset();
    }
}

ss::future<> compaction_worker::compact_log(log_compaction_meta* log) {
    if (!is_active()) {
        co_return;
    }

    // If there was a concurrent race with a request to cancel/stop an inflight
    // compaction, early return after resetting state to `idle`.
    if (
      _job_state == compaction_job_state::soft_stop
      || _job_state == compaction_job_state::hard_stop) {
        _job_state = compaction_job_state::idle;
        co_return;
    }

    if (!log) {
        co_return;
    }

    if (!log->link.is_linked()) {
        co_return;
    }

    auto tidp = log->tidp;
    auto ntp = log->ntp;

    if (!log->info_and_ts.has_value()) {
        vlog(
          compaction_log.error,
          "Log {} in compaction process did not have metastore information "
          "set. Concurrency issue?",
          tidp);
        co_return;
    }

    vlog(compaction_log.info, "Compacting CTP {}", tidp);

    _job_state = compaction_job_state::running;
    _inflight_ntp = ntp;

    auto compaction_offsets = metastore::compaction_offsets_response{
      .dirty_ranges = log->info_and_ts->info.offsets_response.dirty_ranges,
      .removable_tombstone_ranges
      = log->info_and_ts->info.offsets_response.removable_tombstone_ranges,
      .extents = log->info_and_ts->info.offsets_response.extents.copy()};

    // Lazy initialization of offset map.
    if (!_map) {
        co_await initialize_map();
    } else {
        co_await _map->reset();
    }

    auto dirty_range_intervals = compaction_offsets.dirty_ranges.to_vec();

    auto src = std::make_unique<compaction_source>(
      std::move(ntp),
      tidp,
      dirty_range_intervals,
      compaction_offsets.removable_tombstone_ranges,
      std::move(compaction_offsets.extents),
      _map.get(),
      _metastore,
      _io,
      _as,
      _job_state,
      _probe);
    auto sink = std::make_unique<compaction_sink>(
      tidp,
      dirty_range_intervals,
      compaction_offsets.removable_tombstone_ranges,
      _map.get(),
      _io,
      _committer);
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    // Start measuring time-to-compact here.
    auto m = _probe.auto_compaction_measurement();

    auto compact_fut = co_await ss::coroutine::as_future(
      std::move(reducer).run());

    if (compact_fut.failed()) {
        auto eptr = compact_fut.get_exception();
        auto log_lvl = ssx::is_shutdown_exception(eptr) ? ss::log_level::debug
                                                        : ss::log_level::warn;
        vlogl(
          compaction_log,
          log_lvl,
          "Caught exception {} while compacting CTP {}.",
          eptr,
          tidp);

        // Don't let failed compaction runs contribute to the histogram.
        m->cancel();
    } else {
        vlog(compaction_log.info, "Finished compacting CTP {}", tidp);
    }

    _job_state = compaction_job_state::idle;
    _inflight_ntp.reset();
}

ss::future<std::optional<foreign_log_compaction_meta_ptr>>
compaction_worker::try_acquire_work_from_manager() {
    co_return co_await ss::smp::submit_to(
      worker_manager::worker_manager_shard,
      [this, shard = ss::this_shard_id()]() {
          return _worker_manager->try_acquire_work(shard);
      });
}

ss::future<> compaction_worker::complete_work_on_manager(
  foreign_log_compaction_meta_ptr log) {
    co_return co_await ss::smp::submit_to(
      worker_manager::worker_manager_shard, [this, log = std::move(log)] {
          _worker_manager->complete_work(log.get());
          // Destruct foreign_ptr on owning shard by moving it into closure.
          std::ignore = std::move(log);
      });
}

bool compaction_worker::is_active() const {
    return !_gate.is_closed() && !_as.abort_requested()
           && _worker_state == worker_state::active;
}

void compaction_worker::interrupt_current_job() {
    if (_inflight_ntp.has_value()) {
        vlog(
          compaction_log.debug,
          "Interrupting compaction job for CTP {}",
          _inflight_ntp);
    }
    _job_state = compaction_job_state::soft_stop;
}

void compaction_worker::terminate_current_job() {
    if (_inflight_ntp.has_value()) {
        vlog(
          compaction_log.debug,
          "Terminating compaction job for CTP {}",
          _inflight_ntp);
    }
    _job_state = compaction_job_state::hard_stop;
}

ss::future<> compaction_worker::pause_worker() {
    ss::promise<> p;
    _worker_update_queue.submit(
      [&, this] { return do_pause_worker().finally([&] { p.set_value(); }); });
    co_await p.get_future();
}

ss::future<> compaction_worker::do_pause_worker() {
    // If worker is `stopped`, we shouldn't be able to resume it. If it is
    // already `paused`, this is a no-op.
    if (_worker_state != worker_state::active) {
        co_return;
    }

    vlog(
      compaction_log.info,
      "Pausing compaction worker on shard {}",
      ss::this_shard_id());

    interrupt_current_job();

    _worker_state = worker_state::paused;
    // Signal `_worker_cv` in case work_loop is currently waiting.
    alert_worker();
    co_await clear_work_fut();

    vlog(
      compaction_log.info,
      "Paused compaction worker on shard {}",
      ss::this_shard_id());
}

ss::future<> compaction_worker::resume_worker() {
    ss::promise<> p;
    _worker_update_queue.submit(
      [&, this] { return do_resume_worker().finally([&] { p.set_value(); }); });
    co_await p.get_future();
}

ss::future<> compaction_worker::do_resume_worker() {
    // If worker is `stopped`, we shouldn't be able to resume it. If it is
    // already `active`, this is a no-op.
    if (_worker_state != worker_state::paused) {
        co_return;
    }

    // Set state back to active and start a new background loop.
    _worker_state = worker_state::active;
    start_work_loop();
    vlog(
      compaction_log.info,
      "Resumed compaction worker on shard {}",
      ss::this_shard_id());
}

void compaction_worker::alert_worker() { _worker_cv.signal(); }

ss::future<> compaction_worker::initialize_map() {
    if (_map) {
        co_return;
    }

    // TODO: use memory group reservation.
    // auto compaction_mem_bytes = memory_groups().compaction_reserved_memory();
    auto compaction_mem_bytes
      = config::shard_local_cfg().storage_compaction_key_map_memory();
    auto compaction_map = std::make_unique<compaction::hash_key_offset_map>();
    co_await compaction_map->initialize(compaction_mem_bytes);
    _map = std::move(compaction_map);
}

} // namespace cloud_topics::l1
