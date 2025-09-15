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

#pragma once

#include "base/seastarx.h"
#include "cluster_link/errc.h"
#include "cluster_link/fwd.h"
#include "cluster_link/model/types.h"
#include "utils/notification_list.h"
#include "utils/prefix_logger.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/bool_class.hh>

#include <fmt/format.h>

namespace cluster_link {

/**
 * @brief Abstract class that defines a task in cluster linking
 */
class task {
    class runner;

public:
    /// Creates a task with a specified run interval and name.  Also includes a
    /// pointer back to the owning link so the task has access to the link's
    /// cluster connection
    task(link* link, ss::lowres_clock::duration run_interval, ss::sstring name);
    task(const task&) = delete;
    task& operator=(const task&) = delete;
    task(task&&) = delete;
    task& operator=(task&&) = delete;
    virtual ~task();

    /// Starts/resumes the task
    /// Is virtual so these methods can be overriden by test classes
    virtual ss::future<result<void>> start();
    /// Stops the task from running
    /// Is virtual so these methods can be overriden by test classes
    virtual ss::future<result<void>> stop() noexcept;
    /// Pauses the task, prevening it from running until resumed
    ss::future<result<void>> pause();
    /// Returns the name of the task
    const ss::sstring& name() const noexcept;

    /// Returns true if the task should be started on the current node shard
    bool should_start(ss::shard_id shard, ::model::node_id current_node) const;

    /// Returns true if the task should be stopped on the current node shard
    bool should_stop(ss::shard_id shard, ::model::node_id current_node) const;
    /// Updates config of the task
    virtual void update_config(const model::metadata&) = 0;

    struct state_change {
        model::task_state prev;
        model::task_state cur;
        ss::sstring reason;
    };

    using notification_id = named_type<size_t, struct task_notification_tag>;
    /// Callback to be notified when a task changes state.  The parameters are
    /// the task name, the state change info, and a reason
    using task_status_cb
      = ss::noncopyable_function<void(std::string_view, state_change)>;

    /// Registers for notification of task state change
    notification_id register_for_updates(task_status_cb);
    /// Unregisters state change notification
    void unregister_for_updates(notification_id);

    /// Returns the current state of the task
    model::task_state get_state() const noexcept;

    /// Returns the status report for this task
    model::task_status_report get_status_report() const;

protected:
    /// Returns true if the task should be started on the current node shard,
    /// this is only called when the task state allows it to be started
    virtual bool should_start_impl(ss::shard_id, ::model::node_id) const = 0;

    /// Returns true if the task should be stopped on the current node shard,
    /// this is only called when the task state allows it to be stopped
    virtual bool should_stop_impl(ss::shard_id, ::model::node_id) const = 0;

    /// Used by the implementation to change the state of the task
    /// \return The previous state
    result<model::task_state> change_state(model::task_state, ss::sstring = "");
    /// Changes the run interval
    void set_run_interval(ss::lowres_clock::duration);
    /// Returns the logger
    prefix_logger& logger();
    /// Implementation of the task logic that will run periodically
    virtual ss::future<> run_impl() = 0;
    /// Returns the owning link
    link* get_link() const noexcept;

private:
    /// Executes all callbacks on state change
    void run_callbacks(const state_change&);
    /// Validates that the state change is valid
    bool valid_previous_state(model::task_state st) const;

private:
    link* _link;
    ss::lowres_clock::duration _run_interval;
    task_status_cb _status_cb;
    model::task_state _state{model::task_state::stopped};
    ss::sstring _last_state_change_response;

    notification_list<task_status_cb, notification_id> _callbacks;

    ss::sstring _name;
    prefix_logger _logger;

    std::unique_ptr<runner> _task_runner{nullptr};
};
/**
 * Task that is locked to the controller shard. Runs only when the current node
 * is the controller leader.
 */
class controller_locked_task : public task {
public:
    controller_locked_task(
      link* link, ss::lowres_clock::duration run_interval, ss::sstring name);

protected:
    /// Returns true if the task should be started on the current node shard
    bool should_start_impl(ss::shard_id, ::model::node_id) const override;
    /// Returns true if the task should be stopped on the current node shard
    bool should_stop_impl(ss::shard_id, ::model::node_id) const override;

    bool is_controller_leader(ss::shard_id, ::model::node_id) const;
};
class task_factory {
public:
    task_factory() = default;
    task_factory(const task_factory&) = delete;
    task_factory& operator=(const task_factory&) = delete;
    task_factory(task_factory&&) = delete;
    task_factory& operator=(task_factory&&) = delete;
    virtual ~task_factory() = default;

    /// Returns the name of the task that this factory creates
    virtual std::string_view created_task_name() const noexcept = 0;

    /// Creates a new task through the factory.  Provides the owning link
    virtual std::unique_ptr<task> create_task(link* link) = 0;
};
} // namespace cluster_link

template<>
struct fmt::formatter<cluster_link::task::state_change>
  : fmt::formatter<string_view> {
    auto
    format(const cluster_link::task::state_change& t, format_context& ctx) const
      -> decltype(ctx.out());
};
