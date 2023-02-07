/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "archival/fwd.h"
#include "archival/probe.h"
#include "archival/types.h"
#include "cloud_storage/remote.h"
#include "config/bounded_property.h"
#include "random/simple_time_jitter.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/moving_average.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <boost/intrusive/list_hook.hpp>

#include <chrono>

namespace archival {

enum class housekeeping_state {
    // Housekeeping is on pause
    idle,
    // Housekeeping is running
    active,
    // Housekeeping is paused
    pause,
    // End of the housekeeping epoch, draining the job queue
    draining,
    // Terminal state
    stopped,
};

std::ostream& operator<<(std::ostream& o, housekeeping_state s);

/// Controls housekeeping jobs
///
/// The housekeeping workflow runs in epochs. During each
/// epoch all housekeeping jobs has to be executed at least
/// once. The service can be idle or active. To start the new
/// epoch the backlog has to be 0 and the workflow should be in
/// the idle state.
class housekeeping_workflow {
public:
    using probe_opt_t
      = std::optional<std::reference_wrapper<upload_housekeeping_probe>>;

    explicit housekeeping_workflow(
      retry_chain_node& parent,
      ss::scheduling_group sg = ss::default_scheduling_group(),
      probe_opt_t probe = std::nullopt);

    void register_job(housekeeping_job&);

    void deregister_job(housekeeping_job&);

    void start();

    /// Resume or start jobs
    ///
    /// If 'drain' is true, the workflow will continue executing jobs until the
    /// backlog will become empty. After that the workflow will become idle.
    /// If 'drain' is false, the workflow will continue executing jobs until the
    /// backlog will become empty or the workflow will be put on pause by
    /// calling a 'pause' method. When the backlog will become empty the
    /// workflow will go into idle state until the next call to 'resume' methd.
    void resume(bool drain);

    /// Pause running jobs.
    ///
    /// The workflow will execute current job to the end and will stop.
    void pause();

    /// Stop the workflow
    ss::future<> stop();

    /// Housekeeping state
    housekeeping_state state() const;

    /// Returns true if one of the jobs is executed right now
    bool has_active_job() const;

private:
    ss::future<> run_jobs_bg();
    using probe_upd_func = ss::noncopyable_function<void(
      std::reference_wrapper<upload_housekeeping_probe>)>;

    /// Update metrics
    void maybe_update_probe(const housekeeping_job::run_result& res);

    bool jobs_available() const;

    ss::gate _gate;
    ss::gate _exec_gate;
    ss::abort_source _as;
    retry_chain_node& _parent;
    ss::scheduling_group _sg;
    probe_opt_t _probe;
    housekeeping_state _state{housekeeping_state::idle};
    intrusive_list<housekeeping_job, &housekeeping_job::_hook> _pending;
    intrusive_list<housekeeping_job, &housekeeping_job::_hook> _running;
    intrusive_list<housekeeping_job, &housekeeping_job::_hook> _executed;
    ss::condition_variable _cvar;
};

/// Housekeeping service is used to perform periodic
/// data maintenence tasks. Every ntp_archiver_service
/// provides the list of periodic jobs. The service tries
/// to run all jobs once per epoch.
///
/// The service is running in the background. When the
/// remote api is idle it activates and performs the
/// housekeeping tasks.
///
/// If the api is not idle the service will be triggered
/// after 'epoch' timeout. When this happens the service
/// will be in an idle state for at least 'idle-timeout'
/// milliseconds.
///
/// **FSM state transition diagram**
///
///             ┌──────────┐              ┌───────────┐
/// Epoch timer │          │ Idle timeout │           │  Epoch timer
///     ┌───────┤  Idle    ├─────────────►│  Active   ├──────────────────┐
///     │       │          │              │           │                  │
///     │       └──────────┘              └─────────┬─┘                  │
///     │   All jobs ▲                     Idle  ▲  │ Cloud storage      │
///     │   are comp │                     time  │  │ activity (except   │
///     │   leted    │                     out   │  ▼ housekeeping)      │
///     │       ┌────┴─────┐              ┌──────┴────┐                  │
///     │       │          │  Epoch timer │           │                  │
///     └──────►│ Draining │◄─────────────┤  Pause    │                  │
/// Epoch timer │          │              │           │                  │
///             └──────────┘              └───────────┘                  │
///                  ▲        Epoch timer                                │
///                  └───────────────────────────────────────────────────┘
///
/// The service uses two timers and subscribes to cloud_storage::remote
/// notifications. The idle timer fires frequently and triggers transition to
/// the active state. Every time the service receives notification from the
/// cloud_storage::remote it computes the utilization of the cloud storage api
/// and resets the timer if it's above the threshold so if the cloud storage
/// is busy the idle timer will never be triggered and its callback will never
/// be called.
///
/// The 'epoch' timer runs less frequently. Notifications from the cloud
/// storage do not reset it. So it will be triggered eventually if the cloud
/// storage is always busy. The callback of the epoch timer initiates transition
/// to the active state. It's purpose is to make sure that the housekeeping job
/// is perfomed even when the cloud storage is always busy. This will drain the
/// backlog (the job queue) so it's guaranteed that every job is executed at
/// least once per epoch.
///
/// The noticifactions from the cloud storage API reset the idle timer so if the
/// cloud storage is busy it will never be triggered. If any of the jobs uses
/// the cloud storage API the notifications from it will be ignored.
///
/// The service might be stopped by calling 'stop' method. In this case method
/// 'interrupt' of the job will be called if there is a job which is executing
/// at the moment. The 'ntp_archiver_service' has to deregister and stop its
/// job before deletion.
///
/// The following configuration parameters are used:
/// - cloud_storage_idle_timeout_ms: the service goes into active state when
///   remote api is not active for cloud_storage_idle_timeout_ms milliseconds.
/// - cloud_storage_housekeeping_interval_ms: epoch timeout. The service goes
///   active state forcibly, even if remote api is not idle.
/// - cloud_storage_segment_upload_timeout x 3: if service is stopping being
///   active for longer than 3x segment upload timeout the state is stopped
///   forcibly and the error is generated.
class upload_housekeeping_service {
public:
    explicit upload_housekeeping_service(
      ss::sharded<cloud_storage::remote>& api,
      ss::scheduling_group sg = ss::default_scheduling_group());

    upload_housekeeping_service(const upload_housekeeping_service&) = delete;
    upload_housekeeping_service(upload_housekeeping_service&&) = delete;
    upload_housekeeping_service& operator=(const upload_housekeeping_service&)
      = delete;
    upload_housekeeping_service& operator=(upload_housekeeping_service&&)
      = delete;

    ~upload_housekeeping_service();

    /// Register housekeeping jobs
    void
    register_jobs(std::vector<std::reference_wrapper<housekeeping_job>> jobs);

    void
    deregister_jobs(std::vector<std::reference_wrapper<housekeeping_job>> jobs);

    /// Start/stop the service
    ss::future<> start();
    ss::future<> stop();

    /// Return workflow object
    housekeeping_workflow& workflow();

private:
    ss::future<> bg_idle_loop();
    void rearm_idle_timer();
    void idle_timer_callback();
    void epoch_timer_callback();

private:
    ss::gate _gate;
    ss::abort_source _as;
    /// Cloud storage API
    cloud_storage::remote& _remote;
    /// Idle timer, used to detect idle state
    ss::timer<ss::lowres_clock> _idle_timer;
    ss::timer<ss::lowres_clock> _epoch_timer;
    /// Idle timeout, the service is activated when remote
    /// api is idle for longer than '_idle_timeout'
    config::binding<std::chrono::milliseconds> _idle_timeout;
    /// Timeout that defines the duration of epoch
    config::binding<std::chrono::milliseconds> _epoch_duration;
    /// Idle threshold
    config::binding<double> _api_idle_threshold;
    /// Jitter for timers
    simple_time_jitter<ss::lowres_clock> _time_jitter;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    cloud_storage::remote::event_filter _filter;
    upload_housekeeping_probe _probe;
    housekeeping_workflow _workflow;
    static constexpr auto ma_resolution = 20ms;
    using sliding_window_t = timed_moving_average<double, ss::lowres_clock>;
    std::unique_ptr<sliding_window_t> _api_utilization;
};

} // namespace archival
