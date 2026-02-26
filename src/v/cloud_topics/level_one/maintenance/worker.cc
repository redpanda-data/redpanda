/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/worker.h"

#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"
#include "cloud_topics/level_one/maintenance/compaction/compaction_sink.h"
#include "cloud_topics/level_one/maintenance/compaction/compaction_source.h"
#include "cloud_topics/level_one/maintenance/leveling/leveling_sink.h"
#include "cloud_topics/level_one/maintenance/leveling/leveling_source.h"
#include "cloud_topics/level_one/maintenance/logger.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/worker_manager.h"
#include "cluster/metadata_cache.h"
#include "compaction/reducer.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "resource_mgmt/memory_groups.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l1 {

maintenance_worker::maintenance_worker(
  worker_manager* worker_manager,
  io* io,
  metastore* metastore,
  cluster::metadata_cache* metadata_cache,
  ss::scheduling_group compaction_sg,
  level_one_reader_probe* l1_reader_probe)
  : _worker_update_queue([](const std::exception_ptr& ex) {
      vlog(
        maintenance_log.error,
        "Unexpected compaction worker update queue error: {}",
        ex);
  })
  , _compaction_poll_interval(
      config::shard_local_cfg().cloud_topics_compaction_interval_ms.bind())
  , _leveling_poll_interval(
      config::shard_local_cfg().cloud_topics_leveling_interval_ms.bind())
  , _max_concurrent_leveling_ops(
      config::shard_local_cfg().cloud_topics_concurrent_maintenance_ops.bind())
  , _leveling_sem(_max_concurrent_leveling_ops(), "l1/leveling")
  , _worker_manager(worker_manager)
  , _io(io)
  , _metastore(metastore)
  , _metadata_cache(metadata_cache)
  , _compaction_sg(compaction_sg)
  , _l1_reader_probe(l1_reader_probe) {
    _compaction_poll_interval.watch([this]() { _compaction_cv.signal(); });
    _leveling_poll_interval.watch([this]() { _leveling_cv.signal(); });
    _max_concurrent_leveling_ops.watch(
      [this]() { _leveling_sem.set_capacity(_max_concurrent_leveling_ops()); });
}

ss::future<> maintenance_worker::start() {
    _probe.setup_metrics();
    start_compaction_loop();
    start_leveling_loop();
    co_return;
}

ss::future<> maintenance_worker::stop() {
    terminate_all_jobs();
    _worker_state = worker_state::stopped;
    _as.request_abort();
    _compaction_cv.broken();
    _leveling_cv.broken();
    _leveling_sem.broken();

    co_await _worker_update_queue.shutdown();

    auto close_fut = _gate.close();

    co_await clear_compaction_fut();
    co_await clear_leveling_fut();

    if (_map) {
        co_await _map->initialize(0);
        _map.reset();
    }

    co_await std::move(close_fut);
}

void maintenance_worker::start_compaction_loop() {
    vassert(
      !_compaction_work_fut.has_value(),
      "Cannot set value of _compaction_work_fut when it already has a value.");
    _compaction_work_fut = ssx::spawn_with_gate_then(_gate, [this]() {
        return ss::with_scheduling_group(
          _compaction_sg, [this]() { return compaction_work_loop(); });
    });
}

void maintenance_worker::start_leveling_loop() {
    vassert(
      !_leveling_work_fut.has_value(),
      "Cannot set value of _leveling_work_fut when it already has a value.");
    _leveling_work_fut = ssx::spawn_with_gate_then(_gate, [this]() {
        return ss::with_scheduling_group(
          _compaction_sg, [this]() { return leveling_work_loop(); });
    });
}

ss::future<> maintenance_worker::compaction_work_loop() {
    while (is_active()) {
        auto poll_interval = _compaction_poll_interval();
        try {
            co_await _compaction_cv.wait(_compaction_poll_interval());
        } catch (const ss::condition_variable_timed_out&) {
            // Fall through
        }

        if (poll_interval != _compaction_poll_interval()) {
            continue;
        }

        while (is_active()) {
            auto maybe_work
              = co_await try_acquire_compaction_work_from_manager();

            if (!maybe_work.has_value()) {
                break;
            }

            auto work = std::move(maybe_work).value();
            auto tidp = work->tidp;

            auto job_fut = co_await ss::coroutine::as_future(
              compact_log(work.get()));
            co_await complete_work_on_manager(std::move(work));

            if (job_fut.failed()) {
                auto eptr = job_fut.get_exception();
                auto log_lvl = ssx::is_shutdown_exception(eptr)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
                vlogl(
                  maintenance_log,
                  log_lvl,
                  "Caught exception {} while compacting CTP {}.",
                  eptr,
                  tidp);
            }
        }
    }
}

ss::future<> maintenance_worker::leveling_work_loop() {
    while (is_active()) {
        auto poll_interval = _leveling_poll_interval();
        try {
            co_await _leveling_cv.wait(_leveling_poll_interval());
        } catch (const ss::condition_variable_timed_out&) {
            // Fall through
        }

        if (poll_interval != _leveling_poll_interval()) {
            continue;
        }

        // Spawn concurrent leveling fibers up to the semaphore limit.
        while (is_active()) {
            auto units = co_await _leveling_sem.get_units(1);

            auto maybe_work = co_await try_acquire_leveling_work_from_manager();

            if (!maybe_work.has_value()) {
                break;
            }

            auto work = std::move(maybe_work).value();

            auto ctx_it = _leveling_jobs.emplace(
              _leveling_jobs.end(), maintenance_job_state::idle, work->ntp);

            // Fire-and-forget within the gate; the semaphore controls
            // concurrency and the gate ensures cleanup on stop.
            ssx::spawn_with_gate(
              _gate,
              [this, ctx_it, work = std::move(work), units = std::move(units)](
                this auto) -> ss::future<> {
                  auto tidp = work->tidp;

                  auto job_fut = co_await ss::coroutine::as_future(
                    level_log(work.get(), *ctx_it));
                  co_await complete_work_on_manager(std::move(work));

                  if (job_fut.failed()) {
                      auto eptr = job_fut.get_exception();
                      auto log_lvl = ssx::is_shutdown_exception(eptr)
                                       ? ss::log_level::debug
                                       : ss::log_level::warn;
                      vlogl(
                        maintenance_log,
                        log_lvl,
                        "Caught exception {} while leveling CTP {}.",
                        eptr,
                        tidp);
                  }

                  _leveling_jobs.erase(ctx_it);
                  _leveling_cv.signal();
              });
        }
    }
}

ss::future<> maintenance_worker::clear_compaction_fut() {
    if (_compaction_work_fut.has_value()) {
        co_await std::move(_compaction_work_fut).value();
        _compaction_work_fut.reset();
    }
}

ss::future<> maintenance_worker::clear_leveling_fut() {
    if (_leveling_work_fut.has_value()) {
        co_await std::move(_leveling_work_fut).value();
        _leveling_work_fut.reset();
    }

    // Wait for all inflight leveling fibers to complete. The fibers
    // erase from _leveling_jobs and signal _leveling_cv on completion.
    // During stop(), the CV is broken and the gate close drains fibers,
    // so we only actively wait when the CV is still usable (pause path).
    try {
        while (!_leveling_jobs.empty()) {
            co_await _leveling_cv.wait();
        }
    } catch (const ss::broken_condition_variable&) {
        // During stop(), fibers will be drained by gate close.
    }
}

ss::future<> maintenance_worker::compact_log(log_maintenance_meta* log) {
    if (!is_active()) {
        co_return;
    }

    // If there was a concurrent race with a request to cancel/stop an inflight
    // compaction, early return after resetting state to `idle`.
    if (
      _compaction_job_state == maintenance_job_state::soft_stop
      || _compaction_job_state == maintenance_job_state::hard_stop) {
        _compaction_job_state = maintenance_job_state::idle;
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

    if (!log->compaction_info_and_ts.has_value()) {
        vlog(
          maintenance_log.error,
          "Log {} in compaction process did not have metastore information "
          "set. Concurrency issue?",
          tidp);
        co_return;
    }

    vlog(maintenance_log.info, "Compacting CTP {}", tidp);

    _compaction_job_state = maintenance_job_state::running;
    _compaction_inflight_ntp = ntp;

    auto compaction_offsets = metastore::compaction_offsets_response{
      .dirty_ranges
      = log->compaction_info_and_ts->info.offsets_response.dirty_ranges,
      .removable_tombstone_ranges
      = log->compaction_info_and_ts->info.offsets_response
          .removable_tombstone_ranges};
    auto expected_compaction_epoch
      = log->compaction_info_and_ts->info.compaction_epoch;
    auto start_offset = log->compaction_info_and_ts->info.start_offset;
    auto max_compactible_offset
      = log->compaction_info_and_ts->max_compactible_offset;

    // Lazy initialization of offset map.
    if (!_map) {
        co_await initialize_map();
    } else {
        co_await _map->reset();
    }

    auto dirty_range_intervals = compaction_offsets.dirty_ranges.to_vec();

    auto min_lag_ms = [this, &ntp]() -> std::chrono::milliseconds {
        std::optional<std::chrono::milliseconds> topic_min_lag_override;
        if (likely(_metadata_cache)) {
            auto topic_md_ref = _metadata_cache->get_topic_metadata_ref(
              model::topic_namespace_view(ntp));
            if (topic_md_ref.has_value()) {
                topic_min_lag_override = topic_md_ref.value()
                                           .get()
                                           .get_configuration()
                                           .properties.min_compaction_lag_ms;
            }
        }
        return topic_min_lag_override.value_or(
          config::shard_local_cfg().min_compaction_lag_ms());
    }();

    auto src = std::make_unique<compaction_source>(
      std::move(ntp),
      tidp,
      dirty_range_intervals,
      compaction_offsets.removable_tombstone_ranges,
      start_offset,
      max_compactible_offset,
      _map.get(),
      min_lag_ms,
      _metastore,
      _io,
      _as,
      _compaction_job_state,
      _probe,
      _l1_reader_probe);
    auto sink = std::make_unique<compaction_sink>(
      tidp,
      dirty_range_intervals,
      compaction_offsets.removable_tombstone_ranges,
      expected_compaction_epoch,
      start_offset,
      _io,
      _metastore,
      _as,
      config::shard_local_cfg().cloud_topics_compaction_max_object_size.bind(),
      l1::object_builder::options{
        .indexing_interval
        = config::shard_local_cfg().cloud_topics_l1_indexing_interval(),
      });
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
          maintenance_log,
          log_lvl,
          "Caught exception {} while compacting CTP {}.",
          eptr,
          tidp);

        // Don't let failed compaction runs contribute to the histogram.
        m->cancel();
    } else {
        vlog(maintenance_log.info, "Finished compacting CTP {}", tidp);
    }

    _compaction_job_state = maintenance_job_state::idle;
    _compaction_inflight_ntp.reset();
}

ss::future<> maintenance_worker::level_log(
  log_maintenance_meta* log, leveling_job_ctx& ctx) {
    if (!is_active()) {
        co_return;
    }

    if (
      ctx.state == maintenance_job_state::soft_stop
      || ctx.state == maintenance_job_state::hard_stop) {
        ctx.state = maintenance_job_state::idle;
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

    if (!log->leveling_info_and_ts.has_value()) {
        vlog(
          maintenance_log.error,
          "Log {} in leveling process did not have metastore leveling "
          "information set. Concurrency issue?",
          tidp);
        co_return;
    }

    vlog(maintenance_log.info, "Leveling CTP {}", tidp);

    ctx.state = maintenance_job_state::running;
    ctx.ntp = ntp;

    auto leveling_ranges
      = log->leveling_info_and_ts->info.leveling_ranges.to_vec();

    auto src = std::make_unique<leveling_source>(
      std::move(ntp),
      tidp,
      std::move(leveling_ranges),
      _metastore,
      _io,
      _as,
      ctx.state,
      _probe);
    auto sink = std::make_unique<leveling_sink>(
      tidp,
      _io,
      _metastore,
      _as,
      config::shard_local_cfg()
        .cloud_topics_reconciliation_max_object_size.bind());
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    auto m = _probe.auto_compaction_measurement();

    auto level_fut = co_await ss::coroutine::as_future(
      std::move(reducer).run());

    if (level_fut.failed()) {
        auto eptr = level_fut.get_exception();
        auto log_lvl = ssx::is_shutdown_exception(eptr) ? ss::log_level::debug
                                                        : ss::log_level::warn;
        vlogl(
          maintenance_log,
          log_lvl,
          "Caught exception {} while leveling CTP {}.",
          eptr,
          tidp);

        m->cancel();
    } else {
        vlog(maintenance_log.info, "Finished leveling CTP {}", tidp);
    }

    ctx.state = maintenance_job_state::idle;
}

ss::future<std::optional<foreign_log_maintenance_meta_ptr>>
maintenance_worker::try_acquire_compaction_work_from_manager() {
    co_return co_await ss::smp::submit_to(
      worker_manager::worker_manager_shard,
      [this, shard = ss::this_shard_id()]() {
          return _worker_manager->try_acquire_compaction_work(shard);
      });
}

ss::future<std::optional<foreign_log_maintenance_meta_ptr>>
maintenance_worker::try_acquire_leveling_work_from_manager() {
    co_return co_await ss::smp::submit_to(
      worker_manager::worker_manager_shard,
      [this, shard = ss::this_shard_id()]() {
          return _worker_manager->try_acquire_leveling_work(shard);
      });
}

ss::future<> maintenance_worker::complete_work_on_manager(
  foreign_log_maintenance_meta_ptr log) {
    co_return co_await ss::smp::submit_to(
      worker_manager::worker_manager_shard, [this, log = std::move(log)] {
          _worker_manager->complete_work(log.get());
          // Destruct foreign_ptr on owning shard by moving it into closure.
          std::ignore = std::move(log);
      });
}

bool maintenance_worker::is_active() const {
    return !_gate.is_closed() && !_as.abort_requested()
           && _worker_state == worker_state::active;
}

void maintenance_worker::interrupt_current_job(const model::ntp& ntp) {
    if (_compaction_inflight_ntp == ntp) {
        vlog(
          maintenance_log.debug, "Interrupting compaction job for CTP {}", ntp);
        _compaction_job_state = maintenance_job_state::soft_stop;
    }

    for (auto& ctx : _leveling_jobs) {
        if (ctx.ntp == ntp) {
            vlog(
              maintenance_log.debug,
              "Interrupting leveling job for CTP {}",
              ntp);
            ctx.state = maintenance_job_state::soft_stop;
        }
    }
}

void maintenance_worker::terminate_current_job(const model::ntp& ntp) {
    if (_compaction_inflight_ntp == ntp) {
        vlog(
          maintenance_log.debug, "Terminating compaction job for CTP {}", ntp);
        _compaction_job_state = maintenance_job_state::hard_stop;
    }

    for (auto& ctx : _leveling_jobs) {
        if (ctx.ntp == ntp) {
            vlog(
              maintenance_log.debug,
              "Terminating leveling job for CTP {}",
              ntp);
            ctx.state = maintenance_job_state::hard_stop;
        }
    }
}

void maintenance_worker::interrupt_all_jobs() {
    if (_compaction_inflight_ntp.has_value()) {
        vlog(
          maintenance_log.debug,
          "Interrupting compaction job for CTP {}",
          _compaction_inflight_ntp);
    }
    _compaction_job_state = maintenance_job_state::soft_stop;

    for (auto& ctx : _leveling_jobs) {
        vlog(
          maintenance_log.debug,
          "Interrupting leveling job for CTP {}",
          ctx.ntp);
        ctx.state = maintenance_job_state::soft_stop;
    }
}

void maintenance_worker::terminate_all_jobs() {
    if (_compaction_inflight_ntp.has_value()) {
        vlog(
          maintenance_log.debug,
          "Terminating compaction job for CTP {}",
          _compaction_inflight_ntp);
    }
    _compaction_job_state = maintenance_job_state::hard_stop;

    for (auto& ctx : _leveling_jobs) {
        vlog(
          maintenance_log.debug,
          "Terminating leveling job for CTP {}",
          ctx.ntp);
        ctx.state = maintenance_job_state::hard_stop;
    }
}

ss::future<> maintenance_worker::pause_worker() {
    ss::promise<> p;
    _worker_update_queue.submit(
      [&, this] { return do_pause_worker().finally([&] { p.set_value(); }); });
    co_await p.get_future();
}

ss::future<> maintenance_worker::do_pause_worker() {
    // If worker is `stopped`, we shouldn't be able to resume it. If it is
    // already `paused`, this is a no-op.
    if (_worker_state != worker_state::active) {
        co_return;
    }

    vlog(
      maintenance_log.info,
      "Pausing compaction worker on shard {}",
      ss::this_shard_id());

    interrupt_all_jobs();

    _worker_state = worker_state::paused;
    // Signal both CVs so work loops observe is_active() == false and exit.
    alert_compaction();
    alert_leveling();
    co_await clear_compaction_fut();
    co_await clear_leveling_fut();

    vlog(
      maintenance_log.info,
      "Paused compaction worker on shard {}",
      ss::this_shard_id());
}

ss::future<> maintenance_worker::resume_worker() {
    ss::promise<> p;
    _worker_update_queue.submit(
      [&, this] { return do_resume_worker().finally([&] { p.set_value(); }); });
    co_await p.get_future();
}

ss::future<> maintenance_worker::do_resume_worker() {
    // If worker is `stopped`, we shouldn't be able to resume it. If it is
    // already `active`, this is a no-op.
    if (_worker_state != worker_state::paused) {
        co_return;
    }

    // Set state back to active and start new background loops.
    _worker_state = worker_state::active;
    start_compaction_loop();
    start_leveling_loop();
    vlog(
      maintenance_log.info,
      "Resumed compaction worker on shard {}",
      ss::this_shard_id());
}

void maintenance_worker::alert_compaction() { _compaction_cv.signal(); }

void maintenance_worker::alert_leveling() { _leveling_cv.signal(); }

ss::future<> maintenance_worker::initialize_map() {
    if (_map) {
        co_return;
    }

    auto compaction_mem_bytes
      = memory_groups().cloud_topics_compaction_reserved_memory();
    auto compaction_map = std::make_unique<compaction::hash_key_offset_map>();
    co_await compaction_map->initialize(compaction_mem_bytes);
    _map = std::move(compaction_map);
}

} // namespace cloud_topics::l1
