/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include "utils/prefix_logger.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l1 {

namespace {

// Confirms on `worker_manager_shard` (the home shard of every maintenance
// job's meta, and the owner shard of its `foreign_ptr`) that the job's CTP is
// still managed. `link` is owned and mutated on that shard, so the read is
// marshalled there; reading it from a worker shard would be a data race.
ss::future<bool> is_linked(const log_compaction_meta* meta) {
    return ss::smp::submit_to(worker_manager::worker_manager_shard, [meta] {
        return meta->link.is_linked();
    });
}

ss::future<bool> may_level(const log_compaction_meta* meta) {
    return ss::smp::submit_to(worker_manager::worker_manager_shard, [meta] {
        return meta->link.is_linked()
               && !meta->compaction.inflight_shard.has_value();
    });
}

} // namespace

compaction_worker::compaction_worker(
  worker_manager* worker_manager,
  io* io,
  metastore* metastore,
  cluster::metadata_cache* metadata_cache,
  ss::scheduling_group compaction_sg,
  level_one_reader_probe* l1_reader_probe,
  cloud_topics::level_zero_notifier* notifier)
  : _worker_update_queue([](const std::exception_ptr& ex) {
      vlog(
        compaction_log.error,
        "Unexpected compaction worker update queue error: {}",
        ex);
  })
  , _compaction_poll_interval(
      config::shard_local_cfg().cloud_topics_compaction_interval_ms.bind())
  , _leveling_poll_interval(
      config::shard_local_cfg().cloud_topics_leveling_interval_ms.bind())
  , _leveling_max_concurrent_jobs(
      config::shard_local_cfg()
        .cloud_topics_max_concurrent_leveling_jobs_per_shard.bind())
  , _leveling_sem(
      _leveling_max_concurrent_jobs(), "cloud_topics::worker::leveling")
  , _upload_part_size(config::shard_local_cfg().cloud_topics_upload_part_size())
  , _worker_manager(worker_manager)
  , _io(io)
  , _metastore(metastore)
  , _metadata_cache(metadata_cache)
  , _compaction_sg(compaction_sg)
  , _l1_reader_probe(l1_reader_probe)
  , _notifier(notifier) {
    _compaction_poll_interval.watch([this]() { alert_compaction_fiber(); });
    _leveling_poll_interval.watch([this]() { alert_leveling_fiber(); });
    _leveling_max_concurrent_jobs.watch([this]() {
        _leveling_sem.set_capacity(_leveling_max_concurrent_jobs());
    });
}

ss::future<> compaction_worker::start() {
    _probe.setup_metrics();
    co_return;
}

ss::future<> compaction_worker::stop() {
    _as.request_abort();

    co_await _worker_update_queue.shutdown();

    _worker_target_compaction_state = worker_state::stopped;
    _worker_target_leveling_state = worker_state::stopped;

    _compaction_cv.broken();
    _leveling_cv.broken();

    terminate_compaction_job();
    terminate_leveling_jobs();

    auto close_fut = _gate.close();

    co_await clear_compaction_work_fut();
    co_await clear_leveling_work_fut();

    if (_map) {
        co_await _map->initialize(0);
        _map.reset();
    }

    co_await std::move(close_fut);
}

void compaction_worker::resume_compaction_work_loop() {
    if (_worker_target_compaction_state == worker_state::stopped) {
        return;
    }
    _worker_target_compaction_state = worker_state::active;
    if (_compaction_work_fut.has_value()) {
        return;
    }
    _compaction_work_fut = ssx::spawn_with_gate_then(_gate, [this]() {
        return ss::with_scheduling_group(
          _compaction_sg, [this]() { return compaction_work_loop(); });
    });
}

void compaction_worker::resume_leveling_work_loop() {
    if (_worker_target_leveling_state == worker_state::stopped) {
        return;
    }
    _worker_target_leveling_state = worker_state::active;
    if (_leveling_work_fut.has_value()) {
        return;
    }
    _leveling_work_fut = ssx::spawn_with_gate_then(_gate, [this]() {
        return ss::with_scheduling_group(
          _compaction_sg, [this]() { return leveling_work_loop(); });
    });
}

ss::future<> compaction_worker::pause_compaction_work_loop() {
    if (_worker_target_compaction_state == worker_state::stopped) {
        co_return;
    }
    _worker_target_compaction_state = worker_state::paused;
    if (!_compaction_work_fut.has_value()) {
        co_return;
    }
    vlog(compaction_log.info, "Pausing compaction loop on worker");
    interrupt_compaction_job();
    alert_compaction_fiber();
    co_await clear_compaction_work_fut();
}

ss::future<> compaction_worker::pause_leveling_work_loop() {
    if (_worker_target_leveling_state == worker_state::stopped) {
        co_return;
    }
    _worker_target_leveling_state = worker_state::paused;
    if (!_leveling_work_fut.has_value()) {
        co_return;
    }
    vlog(compaction_log.info, "Pausing leveling loop on worker");
    interrupt_leveling_jobs();
    alert_leveling_fiber();
    co_await clear_leveling_work_fut();

    // `clear_leveling_work_fut` only awaits the loop future. Leveling jobs
    // are backgrounded, and leveling is not effectively paused until they
    // resolve.
    co_await _leveling_drained_cv.wait(
      [this] { return _inflight_leveling.empty(); });
}

ss::future<> compaction_worker::compaction_work_loop() {
    vlog(compaction_log.info, "Started compaction work loop");
    while (should_run_compaction()) {
        auto poll_interval = _compaction_poll_interval();
        try {
            co_await _compaction_cv.wait(_compaction_poll_interval());
        } catch (const ss::condition_variable_timed_out&) {
            // Fall through
        }

        if (poll_interval != _compaction_poll_interval()) {
            // Cluster config was changed while waiting.
            continue;
        }

        while (should_run_compaction()) {
            auto maybe_work
              = co_await try_acquire_compaction_work_from_manager();

            if (!maybe_work.has_value()) {
                break;
            }

            auto work = std::move(maybe_work).value();

            auto ntp = work->meta->ntp;

            auto compact_fut = co_await ss::coroutine::as_future(
              compact_log(work.get()));
            co_await complete_compaction_work_on_manager(std::move(work));

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
                  ntp);
            }
        }
    }
}

ss::future<> compaction_worker::level_range(
  foreign_leveling_job_ptr job, ssx::semaphore_units u) {
    auto ntp = job->meta->ntp;
    auto level_fut = co_await ss::coroutine::as_future(
      do_level_range(job.get()));
    co_await complete_leveling_work_on_manager(std::move(job));
    if (level_fut.failed()) {
        auto eptr = level_fut.get_exception();
        auto lvl = ssx::is_shutdown_exception(eptr) ? ss::log_level::debug
                                                    : ss::log_level::warn;
        vlogl(
          compaction_log,
          lvl,
          "Caught exception {} while leveling {}",
          eptr,
          ntp);
    }
    u.return_all();
    alert_leveling_fiber();
}

ss::future<> compaction_worker::leveling_work_loop() {
    vlog(compaction_log.info, "Started leveling work loop");
    while (should_run_leveling()) {
        auto poll_interval = _leveling_poll_interval();
        try {
            co_await _leveling_cv.wait(poll_interval);
        } catch (const ss::condition_variable_timed_out&) {
            // Fall through
        }

        if (poll_interval != _leveling_poll_interval()) {
            // Cluster config was changed while waiting.
            continue;
        }

        while (should_run_leveling()) {
            auto units_opt = _leveling_sem.try_get_units(1);
            if (!units_opt.has_value()) {
                break;
            }

            auto units = std::move(units_opt).value();

            auto maybe_job = co_await try_acquire_leveling_work_from_manager();
            if (!maybe_job.has_value()) {
                break;
            }

            auto job = std::move(maybe_job).value();

            ssx::spawn_with_gate(
              _gate,
              [this, job = std::move(job), units = std::move(units)]() mutable {
                  return level_range(std::move(job), std::move(units));
              });
        }
    }
}

ss::future<> compaction_worker::clear_compaction_work_fut() {
    if (_compaction_work_fut.has_value()) {
        co_await std::move(_compaction_work_fut).value();
        _compaction_work_fut.reset();
    }
}

ss::future<> compaction_worker::clear_leveling_work_fut() {
    if (_leveling_work_fut.has_value()) {
        co_await std::move(_leveling_work_fut).value();
        _leveling_work_fut.reset();
    }
}

ss::future<> compaction_worker::compact_log(compaction_job* job) {
    if (!should_run_compaction()) {
        co_return;
    }

    if (!job) {
        co_return;
    }

    if (!co_await is_linked(job->meta.get())) {
        co_return;
    }

    // If there was a concurrent race with a request to cancel/stop an inflight
    // compaction, early return after resetting state to `idle`.
    if (
      _compaction_job_state == compaction_job_state::soft_stop
      || _compaction_job_state == compaction_job_state::hard_stop) {
        _compaction_job_state = compaction_job_state::idle;
        co_return;
    }

    auto tidp = job->meta->tidp;
    auto ntp = job->meta->ntp;

    auto ctxlog = prefix_logger(compaction_log, fmt::format("{}", ntp));

    vlog(ctxlog.info, "Compacting CTP");

    _compaction_job_state = compaction_job_state::running;
    _inflight_ntp = ntp;

    const auto& info_and_ts = job->info_and_ts;
    auto compaction_offsets = metastore::compaction_offsets_response{
      .dirty_ranges = info_and_ts.info.offsets_response.dirty_ranges,
      .removable_tombstone_ranges
      = info_and_ts.info.offsets_response.removable_tombstone_ranges};
    auto expected_compaction_epoch = info_and_ts.info.compaction_epoch;
    auto start_offset = info_and_ts.info.start_offset;
    auto max_compactible_offset = info_and_ts.max_compactible_offset;

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
      _l1_reader_probe,
      ctxlog);

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
      _upload_part_size,
      _probe,
      ctxlog,
      l1::object_builder::options{
        .indexing_interval
        = config::shard_local_cfg().cloud_topics_l1_indexing_interval(),
      },
      _notifier);
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
          ctxlog, log_lvl, "Caught exception {} while compacting CTP.", eptr);

        // Don't let failed compaction runs contribute to the histogram.
        m->cancel();
    } else {
        vlog(ctxlog.info, "Finished compacting CTP");
    }

    _compaction_job_state = compaction_job_state::idle;
    _inflight_ntp.reset();
}

ss::future<> compaction_worker::do_level_range(leveling_job* job) {
    if (!should_run_leveling()) {
        co_return;
    }

    if (!job || !job->meta) {
        co_return;
    }

    auto handle = ss::make_lw_shared<leveling_job_handle>();
    handle->state = compaction_job_state::running;
    inflight_key key{
      .tidp = job->meta->tidp, .base_offset = job->range.base_offset};
    _inflight_leveling.emplace(key, handle);
    auto cleanup = ss::defer([this, key] {
        _inflight_leveling.erase(key);
        if (_inflight_leveling.empty()) {
            _leveling_drained_cv.signal();
        }
    });

    if (!co_await may_level(job->meta.get())) {
        co_return;
    }

    if (
      handle->state == compaction_job_state::soft_stop
      || handle->state == compaction_job_state::hard_stop) {
        co_return;
    }

    auto tidp = job->meta->tidp;
    auto ntp = job->meta->ntp;
    auto ctxlog = prefix_logger(
      compaction_log,
      fmt::format(
        "leveling/{}/({}~{})",
        ntp,
        job->range.base_offset,
        job->range.last_offset));

    vlog(ctxlog.info, "Leveling range ({} bytes)", job->range.size_bytes);

    chunked_vector<levelable_range> single_range{job->range};

    auto src = std::make_unique<leveling_source>(
      ntp,
      tidp,
      std::move(single_range),
      _metastore,
      _io,
      _as,
      handle->state,
      ctxlog);
    auto sink = std::make_unique<leveling_sink>(
      tidp,
      job->epoch,
      _io,
      _metastore,
      _as,
      config::shard_local_cfg()
        .cloud_topics_reconciliation_max_object_size.bind(),
      _upload_part_size,
      _probe,
      ctxlog,
      l1::object_builder::options{
        .indexing_interval
        = config::shard_local_cfg().cloud_topics_l1_indexing_interval(),
      });
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    auto m = _probe.auto_leveling_measurement();

    auto level_fut = co_await ss::coroutine::as_future(
      std::move(reducer).run());

    if (level_fut.failed()) {
        auto eptr = level_fut.get_exception();
        auto lvl = ssx::is_shutdown_exception(eptr) ? ss::log_level::debug
                                                    : ss::log_level::warn;
        vlogl(ctxlog, lvl, "Caught exception while leveling range: {}", eptr);
        m->cancel();
    } else {
        vlog(ctxlog.info, "Finished leveling range");
    }
}

ss::future<std::optional<foreign_compaction_job_ptr>>
compaction_worker::try_acquire_compaction_work_from_manager() {
    co_return co_await ss::smp::submit_to(
      worker_manager::worker_manager_shard,
      [this, shard = ss::this_shard_id()]() {
          return _worker_manager->try_acquire_compaction_work(shard);
      });
}

ss::future<> compaction_worker::complete_compaction_work_on_manager(
  foreign_compaction_job_ptr job) {
    co_return co_await ss::smp::submit_to(
      worker_manager::worker_manager_shard, [this, job = std::move(job)] {
          _worker_manager->complete_compaction_work(job.get());
          // Destruct foreign_ptr on owning shard by moving it into closure.
          std::ignore = std::move(job);
      });
}

ss::future<std::optional<foreign_leveling_job_ptr>>
compaction_worker::try_acquire_leveling_work_from_manager() {
    co_return co_await ss::smp::submit_to(
      worker_manager::worker_manager_shard,
      [this, shard = ss::this_shard_id()]() {
          return _worker_manager->try_acquire_leveling_work(shard);
      });
}

ss::future<> compaction_worker::complete_leveling_work_on_manager(
  foreign_leveling_job_ptr job) {
    co_return co_await ss::smp::submit_to(
      worker_manager::worker_manager_shard,
      [this, job = std::move(job), shard = ss::this_shard_id()] {
          _worker_manager->complete_leveling_work(job.get(), shard);
          // Destruct foreign_ptr on owning shard by moving it into closure.
          std::ignore = std::move(job);
      });
}

bool compaction_worker::should_run_compaction() const {
    return !_gate.is_closed() && !_as.abort_requested()
           && _worker_target_compaction_state == worker_state::active;
}

bool compaction_worker::should_run_leveling() const {
    return !_gate.is_closed() && !_as.abort_requested()
           && _worker_target_leveling_state == worker_state::active;
}

void compaction_worker::interrupt_compaction_job() {
    if (_inflight_ntp.has_value()) {
        vlog(
          compaction_log.debug,
          "Interrupting compaction job for CTP {}",
          _inflight_ntp);
    }
    _compaction_job_state = compaction_job_state::soft_stop;
}

void compaction_worker::terminate_compaction_job() {
    if (_inflight_ntp.has_value()) {
        vlog(
          compaction_log.debug,
          "Terminating compaction job for CTP {}",
          _inflight_ntp);
    }
    _compaction_job_state = compaction_job_state::hard_stop;
}

void compaction_worker::interrupt_leveling_jobs() {
    for (auto& [_, handle] : _inflight_leveling) {
        handle->state = compaction_job_state::soft_stop;
    }
}

ss::future<> compaction_worker::pause_worker(maintenance_job_type kind) {
    ss::promise<> p;
    _worker_update_queue.submit([&, this] {
        return do_pause_worker(kind).finally([&] { p.set_value(); });
    });
    co_await p.get_future();
}

ss::future<> compaction_worker::do_pause_worker(maintenance_job_type kind) {
    const bool pause_compaction = kind == maintenance_job_type::compaction
                                  || kind == maintenance_job_type::all;
    const bool pause_leveling = kind == maintenance_job_type::leveling
                                || kind == maintenance_job_type::all;

    if (pause_compaction) {
        co_await pause_compaction_work_loop();
    }

    if (pause_leveling) {
        co_await pause_leveling_work_loop();
    }
}

ss::future<> compaction_worker::resume_worker(maintenance_job_type kind) {
    ss::promise<> p;
    _worker_update_queue.submit([&, this] {
        return do_resume_worker(kind).finally([&] { p.set_value(); });
    });
    co_await p.get_future();
}

ss::future<> compaction_worker::do_resume_worker(maintenance_job_type kind) {
    const bool resume_compaction = kind == maintenance_job_type::compaction
                                   || kind == maintenance_job_type::all;
    const bool resume_leveling = kind == maintenance_job_type::leveling
                                 || kind == maintenance_job_type::all;

    if (resume_compaction) {
        resume_compaction_work_loop();
    }

    if (resume_leveling) {
        resume_leveling_work_loop();
    }

    co_return;
}

void compaction_worker::alert_compaction_fiber() { _compaction_cv.signal(); }

void compaction_worker::alert_leveling_fiber() { _leveling_cv.signal(); }

void compaction_worker::terminate_leveling_jobs() {
    for (auto& [_, handle] : _inflight_leveling) {
        handle->state = compaction_job_state::hard_stop;
    }
}

void compaction_worker::terminate_leveling_jobs_for_tidp(
  model::topic_id_partition tidp) {
    for (auto& [key, handle] : _inflight_leveling) {
        if (key.tidp == tidp) {
            handle->state = compaction_job_state::hard_stop;
            vlog(
              compaction_log.debug,
              "Terminating leveling range for CTP {} (base {})",
              tidp,
              key.base_offset);
        }
    }
}

ss::future<> compaction_worker::initialize_map() {
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
