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
#include "config/configuration.h"
#include "ssx/future-util.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

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
  ss::sharded<cloud_storage::remote>& api)
  : _remote(api.local())
  , _idle_timeout(
      config::shard_local_cfg().cloud_storage_idle_timeout_ms.bind())
  , _epoch_duration(
      config::shard_local_cfg().cloud_storage_housekeeping_interval_ms.bind())
  , _time_jitter(100ms)
  , _rtc(_as)
  , _ctxlog(archival_log, _rtc)
  , _filter(_rtc)
  , _workflow(_rtc) {
    _idle_timer.set_callback([this] { return idle_timer_callback(); });
    _epoch_timer.set_callback([this] { return epoch_timer_callback(); });
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
        co_await _remote.subscribe(_filter);
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

void upload_housekeeping_service::rearm_idle_timer() {
    _idle_timer.rearm(
      ss::lowres_clock::now() + _idle_timeout()
      + _time_jitter.next_jitter_duration());
}

void upload_housekeeping_service::idle_timer_callback() {
    vlog(_ctxlog.info, "Cloud storage is idle");
    if (_workflow.state() == housekeeping_state::idle) {
        vlog(_ctxlog.info, "Activating upload housekeeping");
        _workflow.resume(false);
    }
}

void upload_housekeeping_service::epoch_timer_callback() {
    vlog(_ctxlog.info, "Cloud storage housekeeping epoch");
    if (_workflow.state() != housekeeping_state::draining) {
        vlog(
          _ctxlog.info, "Housekeeping epoch timeout, draining the job queue");
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

housekeeping_workflow::housekeeping_workflow(retry_chain_node& parent)
  : _parent(parent) {}

void housekeeping_workflow::register_job(housekeeping_job& job) {
    _pending.push_back(job);
    _current_backlog++;
}

void housekeeping_workflow::deregister_job(housekeeping_job& job) {
    if (_current_job.has_value() && &(_current_job->get()) == &job) {
        // Current job is running
        _current_job->get().interrupt();
    } else {
        job.interrupt();
        job._hook.unlink();
    }
    _current_backlog--;
}

void housekeeping_workflow::start() {
    ssx::spawn_with_gate(_gate, [this] { return run_jobs_bg(); });
}

ss::future<> housekeeping_workflow::run_jobs_bg() {
    ss::gate::holder h(_gate);
    while (!_as.abort_requested()) {
        vlog(
          archival_log.debug,
          "housekeeping_workflow, BG job, state: {}, backlog: {}",
          _state,
          _current_backlog);
        // When the state is active or draining
        // the loop is processing jobs in round-robin fashion until the
        // backlog size reaches zero. After that the status changes to idle and
        // backlog size to the total number of housekeeping jobs.
        co_await _cvar.wait([this] {
            return _current_backlog > 0
                   && (_state == housekeeping_state::active || _state == housekeeping_state::draining);
        });
        auto& top = _pending.front();
        top._hook.unlink();
        _current_job = std::ref(top);
        co_await top.run(_parent);
        _current_job.reset();
        if (!top.interrupted()) {
            // If the job was interrupted it's never returned
            // to the list of pending jobs and never accessd by
            // the workflow.
            _pending.push_back(top);
        }
        _current_backlog--;
        if (_current_backlog == 0) {
            vlog(
              archival_log.debug,
              "housekeeping_workflow, transition to idle state from {}",
              _state);
            _state = housekeeping_state::idle;
            _current_backlog = _pending.size();
        }
    }
}

void housekeeping_workflow::resume(bool drain) {
    if (
      _state == housekeeping_state::draining
      || _state == housekeeping_state::stopped) {
        return;
    }
    if (drain == true) {
        _state = housekeeping_state::draining;
    } else {
        _state = housekeeping_state::active;
    }
    vlog(
      archival_log.debug,
      "housekeeping_workflow::resume, state: {}, backlog: {}",
      _state,
      _current_backlog);
    _cvar.signal();
}

void housekeeping_workflow::pause() {
    if (_state == housekeeping_state::active) {
        _state = housekeeping_state::pause;
    }
    // Can't pause draining or stopping states.
    // Doesn't make any sense to pause idle state.
}

ss::future<> housekeeping_workflow::stop() {
    _as.request_abort();
    _cvar.broken();
    if (_current_job.has_value()) {
        _current_job->get().interrupt();
    }
    return _gate.close();
}

housekeeping_state housekeeping_workflow::state() const { return _state; }

bool housekeeping_workflow::has_active_job() const {
    return _current_job.has_value();
}

} // namespace archival