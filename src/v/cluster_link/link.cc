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

#include "cluster_link/link.h"

#include "cluster_link/logger.h"
#include "cluster_link/manager.h"
#include "model/namespace.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace cluster_link {
namespace {
ss::future<result<void>> start_task(task* t) {
    result<void> res = outcome::success();
    try {
        co_return co_await t->start();
    } catch (const std::exception& e) {
        res = err_info(
          errc::failed_to_start_task,
          ssx::sformat("Failed to start task {}: {}", t->name(), e.what()));
    }

    co_return res;
}
ss::future<result<void>> stop_task(task* t) {
    result<void> res = outcome::success();
    try {
        co_return co_await t->stop();
    } catch (const std::exception& e) {
        res = err_info(
          errc::failed_to_start_task,
          ssx::sformat("Failed to stop task {}: {}", t->name(), e.what()));
    }

    co_return res;
}
} // namespace

using kafka::data::rpc::partition_leader_cache;
using kafka::data::rpc::partition_manager;
using kafka::data::rpc::topic_metadata_cache;

link::link(
  ::model::node_id self,
  model::id_t link_id,
  manager* manager,
  ss::lowres_clock::duration task_reconciler_interval,
  model::metadata config,
  kafka::client::cluster cluster_connection)
  : _self(self)
  , _link_id(link_id)
  , _manager(manager)
  , _config(std::move(config))
  , _cluster_connection(std::move(cluster_connection))
  , _task_reconciler_interval(task_reconciler_interval) {}

ss::future<> link::start() {
    vlog(
      cllog.info, "Starting cluster link {} ({})", _config.name, _config.uuid);
    // Allow exception to propagate to the caller
    co_await _cluster_connection.start();
    co_await run_task_reconciler();
    _task_reconciler.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] { return run_task_reconciler(); });
    });
    _task_reconciler.arm_periodic(_task_reconciler_interval);
}

ss::future<> link::stop() {
    vlog(
      cllog.info, "Stopping cluster link {} ({})", _config.name, _config.uuid);
    _task_reconciler.cancel();
    _as.request_abort();
    co_await _gate.close();

    for (auto& [_, t] : _tasks) {
        vlog(
          cllog.info,
          "Stopping task {} for cluster link {}",
          t->name(),
          _config.name);
        auto res = co_await stop_task(t.get());
        if (!res) {
            if (res.assume_error().code() == errc::task_not_running) {
                // that's ok, keep going
                continue;
            }
            vlog(
              cllog.error,
              "Failed to stop task {}: {}",
              t->name(),
              res.assume_error().message());
        }
    }

    try {
        co_await _cluster_connection.stop();
    } catch (const std::exception& e) {
        vlog(cllog.warn, "Error shutting down cluster connection: {}", e);
    }

    vlog(cllog.info, "Stopped link {} ({})", _config.name, _config.uuid);
}

ss::future<result<void>> link::register_task(task_factory* tf) {
    vlog(
      cllog.debug,
      "Registering task factory {} for cluster link {} ({})",
      tf->created_task_name(),
      _config.name,
      _config.uuid);
    auto t = tf->create_task(this);

    if (!t) {
        co_return err_info(
          errc::task_creation_failed,
          ssx::sformat(
            "Failed to create task from factory {} for cluster link {}",
            tf->created_task_name(),
            _config.name));
    }

    co_return co_await do_register_task(std::move(t));
}

void link::update_config(model::metadata config) {
    vlog(
      cllog.debug,
      "Updating cluster link {} ({}): {}",
      _config.name,
      _config.uuid,
      config);
    _config = std::move(config);
    for (auto& [_, t] : _tasks) {
        vlog(cllog.trace, "Updating config for task {}", t->name());
        t->update_config(_config);
    }
}

ss::future<>
link::handle_on_leadership_change(::model::ntp ntp, ntp_leader is_ntp_leader) {
    vlog(
      cllog.trace,
      "Cluster link {} handling leadership change for {}: {}",
      _config.name,
      ntp,
      is_ntp_leader);

    // todo: add debouncing here so that we do not trigger multiple
    // reconciliation loops at once.
    co_await run_task_reconciler();
}

const model::metadata& link::config() const { return _config; }

bool link::task_is_registered(std::string_view name) const noexcept {
    return _tasks.contains(ss::sstring{name});
}

link::task_state_notification_id
link::register_for_task_state_changes(task_state_change_cb cb) {
    return _task_state_change_notifications.register_cb(std::move(cb));
}

void link::unregister_for_task_state_changes(
  task_state_notification_id id) noexcept {
    _task_state_change_notifications.unregister_cb(id);
}

model::link_task_status_report link::get_task_status_report() const {
    model::link_task_status_report report;
    report.link_name = _config.name;
    report.task_status_reports.reserve(_tasks.size());
    for (const auto& [name, t] : _tasks) {
        report.task_status_reports.emplace(name, t->get_status_report());
    }
    return report;
}

ss::future<::cluster::cluster_link::errc>
link::add_mirror_topic(model::add_mirror_topic_cmd cmd) {
    return _manager->add_mirror_topic(_link_id, std::move(cmd));
}

ss::future<::cluster::cluster_link::errc>
link::update_mirror_topic_state(model::update_mirror_topic_state_cmd cmd) {
    return _manager->update_mirror_topic_state(_link_id, std::move(cmd));
}

ss::future<::cluster::cluster_link::errc> link::update_mirror_topic_properties(
  model::update_mirror_topic_properties_cmd cmd) {
    return _manager->update_mirror_topic_properties(_link_id, std::move(cmd));
}

const model::metadata& link::get_config() const noexcept { return _config; }

topic_metadata_cache& link::topic_metadata_cache() noexcept {
    return _manager->topic_metadata_cache();
}

partition_leader_cache& link::partition_leader_cache() noexcept {
    return _manager->partition_leader_cache();
}

const partition_leader_cache& link::partition_leader_cache() const noexcept {
    return _manager->partition_leader_cache();
}

partition_manager& link::partition_manager() noexcept {
    return _manager->partition_manager();
}

const partition_manager& link::partition_manager() const noexcept {
    return _manager->partition_manager();
}

kafka::client::cluster& link::get_cluster_connection() noexcept {
    return _cluster_connection;
}

std::optional<chunked_hash_map<::model::topic, model::mirror_topic_metadata>>
link::get_mirror_topics_for_link() const {
    return _manager->get_mirror_topics_for_link(_link_id);
}

bool link::should_start_task(task* t) const {
    return t->should_start(ss::this_shard_id(), _self);
}

bool link::should_stop_task(task* t) const {
    return t->should_stop(ss::this_shard_id(), _self);
}

ss::future<> link::run_task_reconciler() {
    auto fut = co_await ss::coroutine::as_future(
      _task_reconciler_mutex.get_units(_as));
    if (fut.failed()) {
        // abort source triggered, exit early
        co_return;
    }
    auto units = std::move(fut).get();

    vlog(
      cllog.trace, "Running task reconciler for cluster link {}", _config.name);
    // Iterate over all tasks and reconcile their state
    for (auto& [name, t] : _tasks) {
        if (!_as.abort_requested() && should_start_task(t.get())) {
            vlog(
              cllog.info,
              "Reconciler starting task {} for cluster link {}",
              name,
              _config.name);
            auto res = co_await start_task(t.get());
            if (!res) {
                vlog(
                  cllog.error,
                  "Failed to start task {}: {}",
                  name,
                  res.assume_error().message());
            }
        }

        if (should_stop_task(t.get())) {
            vlog(
              cllog.debug,
              "Reconciler stopping task {} for cluster link {}",
              name,
              _config.name);
            auto res = co_await stop_task(t.get());
            if (!res) {
                if (res.assume_error().code() == errc::task_not_running) {
                    // that's ok, keep going
                    continue;
                }
                vlog(
                  cllog.error,
                  "Failed to stop task {}: {}",
                  name,
                  res.assume_error().message());
            }
        }
    }
}

ss::future<result<void>> link::do_register_task(std::unique_ptr<task> t) {
    vlog(
      cllog.debug,
      "Registering task {} for cluster link {} ({})",
      t->name(),
      _config.name,
      _config.uuid);
    if (_tasks.contains(t->name())) {
        auto msg = ssx::sformat(
          "Task named '{}' already exists for link {}",
          t->name(),
          _config.name);
        vlog(cllog.warn, "{}", msg);
        co_return err_info(
          errc::task_already_registered_on_link, std::move(msg));
    }

    result<void> res = outcome::success();
    // Do not need to unregister the task callback as the task lifetime is
    // managed by the link
    t->register_for_updates([this](
                              std::string_view task_name,
                              task::state_change change) {
        vlog(
          cllog.debug, "Task {} reported state change: {}", task_name, change);
        _task_state_change_notifications.notify(
          _config.name, task_name, change);
    });
    // If we register a task after the link has started, then check to see
    // if it should start and do so
    if (!_as.abort_requested() && should_start_task(t.get())) {
        vlog(cllog.info, "Starting task {}", t->name());
        res = co_await start_task(t.get());
        if (!res) {
            vlog(
              cllog.error,
              "Failed to start task {}: {}",
              t->name(),
              res.assume_error().message());
        }
    }
    // Even if the task failed to start, still emplace it into the list, the
    // task reconcilier will re-attempt later
    auto name = t->name();
    _tasks.emplace(std::move(name), std::move(t));

    co_return res;
}
} // namespace cluster_link
