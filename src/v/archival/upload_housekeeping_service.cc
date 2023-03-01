/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/upload_housekeeping_service.h"

#include "archival/fwd.h"
#include "archival/logger.h"
#include "archival/types.h"
#include "cloud_storage/remote.h"
#include "config/configuration.h"
#include "ssx/future-util.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <chrono>
#include <exception>
#include <functional>
#include <variant>

namespace archival {

std::ostream& operator<<(std::ostream& o, housekeeping_state s) {
    switch (s) {
    case housekeeping_state::idle:
        o << "idle";
        break;
    case housekeeping_state::active:
        o << "active";
        break;
    case housekeeping_state::pause:
        o << "pause";
        break;
    case housekeeping_state::draining:
        o << "draining";
        break;
    case housekeeping_state::stopped:
        o << "stopped";
        break;
    };
    return o;
}

upload_housekeeping_service::upload_housekeeping_service(
  ss::sharded<cloud_storage::remote>& api, ss::scheduling_group sg)
  : _remote(api.local())
  , _idle_timeout(
      config::shard_local_cfg().cloud_storage_idle_timeout_ms.bind())
  , _epoch_duration(
      config::shard_local_cfg().cloud_storage_housekeeping_interval_ms.bind())
  , _api_idle_threshold(
      config::shard_local_cfg().cloud_storage_idle_threshold_rps.bind())
  , _time_jitter(100ms)
  , _rtc(_as)
  , _ctxlog(archival_log, _rtc)
  , _filter(_rtc)
  , _workflow(_rtc, sg)
  , _api_utilization(
      std::make_unique<sliding_window_t>(0.0, _idle_timeout(), ma_resolution)) {
    _idle_timer.set_callback([this] { return idle_timer_callback(); });
    _epoch_timer.set_callback([this] { return epoch_timer_callback(); });
    _idle_timeout.watch([this] {
        auto initial = _api_utilization->get();
        _api_utilization = std::make_unique<sliding_window_t>(
          initial, _idle_timeout(), ma_resolution);
    });
}

upload_housekeeping_service::~upload_housekeeping_service() {}

ss::future<> upload_housekeeping_service::start() {
    _workflow.start();
    _epoch_timer.arm_periodic(_epoch_duration());
    ssx::spawn_with_gate(_gate, [this]() { return bg_idle_loop(); });
    return ss::now();
}

ss::future<> upload_housekeeping_service::stop() {
    _idle_timer.cancel();
    _epoch_timer.cancel();
    _as.request_abort();
    _filter.cancel();
    co_await _workflow.stop();
    co_await _gate.close();
}

housekeeping_workflow& upload_housekeeping_service::workflow() {
    return _workflow;
}

ss::future<> upload_housekeeping_service::bg_idle_loop() {
    ss::gate::holder holder(_gate);
    while (!_as.abort_requested()) {
        auto event = co_await _remote.subscribe(_filter);
        double weight = 0;
        switch (event) {
        // Write path events
        case cloud_storage::api_activity_notification::manifest_upload:
        case cloud_storage::api_activity_notification::segment_upload:
        case cloud_storage::api_activity_notification::segment_delete:
            weight = 1;
            break;
        // Read path events
        case cloud_storage::api_activity_notification::manifest_download:
        case cloud_storage::api_activity_notification::segment_download:
            weight = 1;
            break;
        };
        _api_utilization->update(weight, ss::lowres_clock::now());
        if (_api_utilization->get() >= _api_idle_threshold()) {
            // Restart the timer and delay idle timeout
            rearm_idle_timer();
            if (_workflow.state() == housekeeping_state::active) {
                // Try to pause the housekeeping workflow
                _workflow.pause();
            }
            // NOTE: do not pause if workflow is in housekeeping_state::draining
            // state.
        }
    }
}

void upload_housekeeping_service::rearm_idle_timer() {
    _idle_timer.rearm(
      ss::lowres_clock::now() + _idle_timeout()
      + _time_jitter.next_jitter_duration());
}

void upload_housekeeping_service::idle_timer_callback() {
    vlog(_ctxlog.debug, "Cloud storage is idle");
    if (_workflow.state() == housekeeping_state::idle) {
        vlog(_ctxlog.debug, "Activating upload housekeeping");
        _workflow.resume(false);
    }
}

void upload_housekeeping_service::epoch_timer_callback() {
    vlog(_ctxlog.debug, "Cloud storage housekeeping epoch");
    if (_workflow.state() != housekeeping_state::draining) {
        vlog(
          _ctxlog.debug, "Housekeeping epoch timeout, draining the job queue");
        _workflow.resume(true);
    }
}

void upload_housekeeping_service::register_jobs(
  std::vector<std::reference_wrapper<housekeeping_job>> jobs) {
    for (auto ref : jobs) {
        _workflow.register_job(ref.get());
    }
}

void upload_housekeeping_service::deregister_jobs(
  std::vector<std::reference_wrapper<housekeeping_job>> jobs) {
    for (auto ref : jobs) {
        _workflow.deregister_job(ref.get());
    }
}

housekeeping_workflow::housekeeping_workflow(
  retry_chain_node& parent,
  ss::scheduling_group sg,
  std::optional<std::reference_wrapper<upload_housekeeping_probe>> probe)
  : _parent(parent)
  , _sg(sg)
  , _probe(probe) {}

void housekeeping_workflow::register_job(housekeeping_job& job) {
    job.acquire();
    _pending.push_back(job);
    _cvar.signal();
}

void housekeeping_workflow::deregister_job(housekeeping_job& job) {
    auto it = std::find_if(
      _running.begin(), _running.end(), [&job](const housekeeping_job& other) {
          return &other == &job;
      });
    auto is_running = it != _running.end();
    if (is_running) {
        // The job is currently executed by the background fiber.
        // We can't remove it from the list until it finishes. If the
        // job was interrupted it wouldn't be moved to the next list.
        vlog(
          archival_log.debug,
          "interrupting the running job, it will be"
          "removed upon exit");
        job.interrupt();
    } else {
        vlog(archival_log.debug, "removing pending job");
        job.interrupt();
        job.release();
        job._hook.unlink();
    }

    vlog(
      archival_log.debug,
      "deregistered job, current backlog {}, num completed jobs {}, num "
      "running jobs {}",
      _pending.size(),
      _executed.size(),
      _running.size());
}

void housekeeping_workflow::start() {
    ssx::spawn_with_gate(_gate, [this] {
        return ss::with_scheduling_group(_sg, [this] { return run_jobs_bg(); });
    });
}

struct job_exec_timer {
    std::chrono::microseconds total{std::chrono::milliseconds(0)};

    struct raii_wrapper {
        explicit raii_wrapper(job_exec_timer& tm)
          : _tm(tm)
          , _start(std::chrono::steady_clock::now()) {}

        ~raii_wrapper() {
            if (!_moved) {
                auto now = std::chrono::steady_clock::now();
                _tm.get().total
                  += std::chrono::duration_cast<std::chrono::microseconds>(
                    now - _start);
            }
        }

        raii_wrapper(raii_wrapper&& other) noexcept
          : _tm(other._tm)
          , _start(other._start) {
            other._moved = true;
        }

        raii_wrapper& operator=(raii_wrapper&& other) noexcept {
            _tm = other._tm;
            _start = other._start;
            other._moved = true;
            return *this;
        }

        raii_wrapper(const raii_wrapper&) = delete;
        raii_wrapper& operator=(const raii_wrapper&) = delete;

        std::reference_wrapper<job_exec_timer> _tm;
        std::chrono::steady_clock::time_point _start;
        bool _moved{false};
    };

    raii_wrapper time() { return raii_wrapper(*this); }

    void reset() { total = std::chrono::microseconds(0); }
};

bool housekeeping_workflow::jobs_available() const {
    return !_pending.empty()
           && (_state == housekeeping_state::active || _state == housekeeping_state::draining);
}

ss::future<> housekeeping_workflow::run_jobs_bg() {
    ss::gate::holder h(_gate);
    // Holds number of jobs executed in current round
    size_t jobs_executed = 0;
    // Holds number of jobs failed in current round
    size_t jobs_failed = 0;
    // Tracks time of the current housekeeping run
    auto start_time = ss::lowres_clock::now();
    // Tracks job execution time
    job_exec_timer exec_timer{};
    run_quota_t quota = run_quota_t(
      max_reuploads_per_run * static_cast<int32_t>(_pending.size()));
    while (!_as.abort_requested()) {
        vlog(
          archival_log.debug,
          "housekeeping_workflow, BG job, state: {}, backlog: {}",
          _state,
          _pending.size());
        // When the state is active or draining
        // the loop is processing jobs in round-robin fashion until the
        // backlog size reaches zero. After that the status changes to idle and
        // backlog size to the total number of housekeeping jobs.
        co_await _cvar.wait([this] { return jobs_available(); });
        // There is a scheduling event between the predicate (jobs_available())
        // returning true and the ss::condition_variable::wait method returning,
        // meaning that upon the return from this method, there is no guarantee
        // that the predicate is still true. This issue was seen in
        // https://github.com/redpanda-data/redpanda/issues/8964.
        if (!jobs_available()) {
            continue;
        }
        if (_as.abort_requested()) {
            co_return;
        }
        vassert(
          !_pending.empty(),
          "housekeeping_workflow: pendings empty, state {}, backlog {}, "
          "in-flight {}, completed {}",
          _state,
          _pending.size(),
          _running.size(),
          _executed.size());
        vassert(
          _running.empty(),
          "The list of running jobs is not empty, "
          "state {}, backlog {}, in-flight {}, completed {}",
          _state,
          _pending.size(),
          _running.size(),
          _executed.size());
        _running.splice(_running.begin(), _pending, _pending.begin());
        {
            ss::gate::holder hh(_exec_gate);
            try {
                auto r = exec_timer.time();
                auto res = co_await _running.front().run(_parent, quota);
                jobs_executed++;
                quota = res.remaining;
                maybe_update_probe(res);
            } catch (...) {
                vlog(
                  archival_log.warn,
                  "upload housekeeping job error: {}",
                  std::current_exception());
                jobs_failed++;
                maybe_update_probe(
                  {.status = housekeeping_job::run_status::failed});
            }
            if (!_running.front().interrupted()) {
                // The job is pushed to the executed list to be
                // reused in the next housekeeping cycle.
                _executed.splice(_executed.begin(), _running);
            } else {
                // If the job was interrupted it's never returned
                // to the list of executed jobs and never accessd by
                // the workflow.
                _running.front().release();
                _running.clear();
            }
        }
        vassert(
          _running.empty(),
          "The list of running jobs is expected to be empty, "
          "state {}, backlog {}, in-flight {}, completed {}",
          _state,
          _pending.size(),
          _running.size(),
          _executed.size());
        if (_pending.empty()) {
            auto full_time = ss::lowres_clock::now() - start_time;
            vlog(
              archival_log.info,
              "housekeeping_workflow, transition to idle state from {}, {} "
              "jobs executed, {} jobs failed. Housekeeping round lasted "
              "approx. {} sec. Job execution time in the round: {} sec",
              _state,
              jobs_executed,
              jobs_failed,
              std::chrono::duration_cast<std::chrono::seconds>(full_time)
                .count(),
              std::chrono::duration_cast<std::chrono::seconds>(exec_timer.total)
                .count());
            _state = housekeeping_state::idle;
            std::swap(_pending, _executed);
            jobs_failed = 0;
            jobs_executed = 0;
            start_time = ss::lowres_clock::now();
            exec_timer.reset();
            quota = run_quota_t(
              max_reuploads_per_run * static_cast<int32_t>(_pending.size()));
            if (_probe.has_value()) {
                _probe->get().housekeeping_rounds(1);
            }
        }
    }
}

void housekeeping_workflow::maybe_update_probe(
  const housekeeping_job::run_result& res) {
    if (!_probe.has_value()) {
        return;
    }
    auto& probe = _probe->get();
    int is_ok = 0;
    switch (res.status) {
    case housekeeping_job::run_status::ok:
        is_ok = 1;
    case housekeeping_job::run_status::failed:
        probe.housekeeping_jobs(is_ok);
        probe.housekeeping_jobs_failed(1 - is_ok);
        probe.job_cloud_segment_reuploads(res.cloud_reuploads);
        probe.job_local_segment_reuploads(res.local_reuploads);
        probe.job_metadata_reuploads(res.manifest_uploads);
        probe.job_metadata_syncs(res.metadata_syncs);
        probe.job_segment_deletions(res.deletions);
        break;
    case housekeeping_job::run_status::skipped:
        probe.housekeeping_jobs_skipped(1);
        break;
    };
}

void housekeeping_workflow::resume(bool drain) {
    if (
      _state == housekeeping_state::draining
      || _state == housekeeping_state::stopped) {
        return;
    }
    if (drain == true) {
        _state = housekeeping_state::draining;
        if (_probe.has_value()) {
            _probe->get().housekeeping_drains(1);
        }
    } else {
        _state = housekeeping_state::active;
        if (_probe.has_value()) {
            _probe->get().housekeeping_resumes(1);
        }
    }
    vlog(
      archival_log.debug,
      "housekeeping_workflow::resume, state: {}, backlog: {}",
      _state,
      _pending.size());
    _cvar.signal();
}

void housekeeping_workflow::pause() {
    if (_state == housekeeping_state::active) {
        _state = housekeeping_state::pause;
        if (_probe.has_value()) {
            _probe->get().housekeeping_pauses(1);
        }
    }
    // Can't pause draining or stopping states.
    // Doesn't make any sense to pause idle state.
}

ss::future<> housekeeping_workflow::stop() {
    vlog(
      archival_log.info,
      "stopping upload housekeeping workflow, num pending jobs: {}, num "
      "executed jobs: {}, num running jobs {}, waiting until running job stops",
      _pending.size(),
      _executed.size(),
      _running.size());
    // At this point if _running is not empty then it's expected that
    // it'd be removed when the execution of the job will be copleted.
    // This is because the owner of the job is required to deregister its
    // jobs before the housekeeping service is stopped.
    co_await _exec_gate.close();
    auto all_jobs = _running.size() + _executed.size() + _pending.size();
    vassert(all_jobs == 0, "Not all jobs are deregistered", all_jobs);
    _as.request_abort();
    _cvar.broken();
    co_await _gate.close();
}

housekeeping_state housekeeping_workflow::state() const { return _state; }

bool housekeeping_workflow::has_active_job() const { return !_running.empty(); }

} // namespace archival
