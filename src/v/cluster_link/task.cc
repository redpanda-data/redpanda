/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/task.h"

#include "cluster_link/errc.h"
#include "cluster_link/logger.h"
#include "ssx/future-util.h"

namespace cluster_link {

class task::runner {
public:
    explicit runner(task* task)
      : _task(task) {}

    runner(const runner&) = delete;
    runner& operator=(const runner&) = delete;
    runner(runner&&) = delete;
    runner& operator=(runner&&) = delete;

    virtual ~runner() = default;

    ss::future<> start() {
        vlog(_task->logger().debug, "Beginning task runner");
        _timer.set_callback([this] {
            ssx::spawn_with_gate(_gate, [this] {
                return run_task().finally(
                  [this] { _timer.arm(_task->_run_interval); });
            });
        });
        // Before arming the timer, run the task once initially
        co_await run_task();
        _timer.arm(_task->_run_interval);
    }
    ss::future<> stop() {
        vlog(_task->logger().debug, "Stopping task runner");
        _timer.cancel();
        co_await _gate.close();
        vlog(_task->logger().debug, "Task runner stopped");
    }

    void set_task_interval(ss::lowres_clock::duration interval) {
        vlog(
          _task->logger().trace, "set_task_interval called with {}", interval);
        if (!_timer.armed()) {
            // can only happen when a task is currently running;
            // its continuation will rearm the timer
            return;
        }

        auto cur_timeout = _timer.get_timeout();

        // Re-arm the timer to run with the new interval, calculate the new
        // timepoint for when the timer should run
        _timer.cancel();
        _timer.arm((cur_timeout - _task->_run_interval) + interval);
    }

private:
    ss::future<> run_task() {
        vlog(_task->logger().trace, "run_task started");
        try {
            vlog(_task->logger().trace, "running task");
            co_await _task->run_impl();
        } catch (const std::exception& e) {
            vlog(_task->logger().error, "task failed: {}", e.what());
            auto res = _task->change_state(
              model::task_state::faulted,
              ssx::sformat(
                "{} failed with error: {}", _task->name(), e.what()));
            vassert(res.has_value(), "Failed to change state to faulted");
        }
    }

private:
    ss::gate _gate;
    ss::timer<ss::lowres_clock> _timer;

    task* _task;
};

task::task(
  link* link, ss::lowres_clock::duration run_interval, ss::sstring name)
  : _link(link)
  , _run_interval(run_interval)
  , _name(std::move(name))
  , _logger(cllog, _name) {}

task::~task() = default;

ss::future<result<void>> task::start() {
    vlog(logger().trace, "start called");
    if (_task_runner) {
        vlog(logger().debug, "task already started");
        co_return err_info(errc::task_already_running);
    }
    BOOST_OUTCOME_CO_TRYX(change_state(
      model::task_state::active, ssx::sformat("{} has started", name())));

    _task_runner = std::make_unique<runner>(this);
    co_await _task_runner->start();

    co_return outcome::success();
}

ss::future<result<void>> task::stop() {
    vlog(logger().trace, "stop called");
    auto res = change_state(
      model::task_state::not_running, ssx::sformat("{} has stopped", name()));
    vassert(res.has_value(), "Failed to change state to not_running");
    if (_task_runner) {
        co_await _task_runner->stop();
        _task_runner.reset();
    }
    co_return outcome::success();
}

ss::future<result<void>> task::pause() {
    vlog(logger().trace, "pause called");
    BOOST_OUTCOME_CO_TRYX(change_state(
      model::task_state::paused, ssx::sformat("{} has paused", name())));
    if (_task_runner) {
        co_await _task_runner->stop();
        _task_runner.reset();
    }
    co_return outcome::success();
}

const ss::sstring& task::name() const noexcept { return _name; }

task::notification_id task::register_for_updates(task_status_cb cb) {
    return _callbacks.register_cb(std::move(cb));
}
void task::unregister_for_updates(notification_id id) {
    _callbacks.unregister_cb(id);
}

model::task_state task::get_state() const noexcept { return _state; }

model::task_status_report task::get_status_report() const {
    model::task_status_report report;
    report.task_name = name();
    report.task_state = get_state();
    report.task_state_reason = _last_state_change_response;
    return report;
}

result<model::task_state>
task::change_state(model::task_state new_state, ss::sstring reason) {
    vlog(
      logger().trace,
      "requesting state change from {} to {}: {}",
      _state,
      new_state,
      reason);
    if (_state == new_state) {
        return _state;
    }

    if (!valid_previous_state(new_state)) {
        return err_info(
          errc::invalid_task_state_change,
          fmt::format(
            "Cannot change state from {} to {}: {}",
            _state,
            new_state,
            reason));
    }

    auto prev_state = _state;
    _state = new_state;
    _last_state_change_response = reason;
    run_callbacks(
      {.prev = prev_state, .cur = new_state, .reason = std::move(reason)});
    return prev_state;
}

void task::set_run_interval(ss::lowres_clock::duration interval) {
    vlog(logger().trace, "set_run_interval called with {}", interval);

    if (_task_runner) {
        _task_runner->set_task_interval(interval);
    }
    _run_interval = interval;
}

prefix_logger& task::logger() { return _logger; }

void task::run_callbacks(const state_change& change) {
    _callbacks.notify(name(), change);
}

bool task::valid_previous_state(model::task_state st) const {
    switch (st) {
    case model::task_state::paused:
        return _state == model::task_state::active
               || _state == model::task_state::link_unavailable
               || _state == model::task_state::faulted;
    case model::task_state::link_unavailable:
        return _state == model::task_state::active;
    // Always valid to change to not_running, active or faulted
    case model::task_state::not_running:
    case model::task_state::active:
    case model::task_state::faulted:
        return true;
    }
}

link* task::get_link() const noexcept { return _link; }
} // namespace cluster_link

auto fmt::formatter<cluster_link::task::state_change>::format(
  const cluster_link::task::state_change& t, format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(), "{{prev={}, cur={}, reason={}}}", t.prev, t.cur, t.reason);
}
