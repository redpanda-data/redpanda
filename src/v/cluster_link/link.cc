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

#include "cluster_link/deps.h"
#include "cluster_link/link_probe.h"
#include "cluster_link/logger.h"
#include "cluster_link/manager.h"
#include "cluster_link/model/types.h"
#include "cluster_link/replication/replication_probe.h"
#include "cluster_link/utils.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/switch_to.hh>

namespace cluster_link {
namespace {
ss::future<cl_result<void>> start_task(task* t) {
    cl_result<void> res = outcome::success();
    try {
        co_return co_await t->start();
    } catch (const std::exception& e) {
        res = err_info(
          errc::failed_to_start_task,
          ssx::sformat("Failed to start task {}: {}", t->name(), e.what()));
    }

    co_return res;
}
ss::future<cl_result<void>> stop_task(task* t) {
    cl_result<void> res = outcome::success();
    try {
        co_return co_await t->stop();
    } catch (const std::exception& e) {
        res = err_info(
          errc::failed_to_stop_task,
          ssx::sformat("Failed to stop task {}: {}", t->name(), e.what()));
    }

    co_return res;
}
ss::future<cl_result<void>> pause_task(task* t) {
    cl_result<void> res = outcome::success();
    try {
        co_return co_await t->pause();
    } catch (const std::exception& e) {
        res = err_info(
          errc::failed_to_pause_task,
          ssx::sformat("Failed to pause task {}: {}", t->name(), e.what()));
    }

    co_return res;
}

bool mirror_active_state(model::mirror_topic_status status) {
    switch (status) {
    case model::mirror_topic_status::active:
        return true;
    case model::mirror_topic_status::failed:
    case model::mirror_topic_status::paused:
    case model::mirror_topic_status::failing_over:
    case model::mirror_topic_status::failed_over:
    case model::mirror_topic_status::promoting:
    case model::mirror_topic_status::promoted:
        return false;
    }
}
} // namespace

using kafka::data::rpc::partition_leader_cache;
using kafka::data::rpc::partition_manager;
using kafka::data::rpc::topic_creator;
using kafka::data::rpc::topic_metadata_cache;

link::link(
  ::model::node_id self,
  model::id_t link_id,
  manager* manager,
  ss::lowres_clock::duration task_reconciler_interval,
  model::metadata_ptr config,
  std::unique_ptr<kafka::client::cluster> cluster_connection,
  std::unique_ptr<replication::link_configuration_provider> config_provider,
  std::unique_ptr<replication::data_source_factory> data_source_factory,
  std::unique_ptr<replication::data_sink_factory> data_sink_factory)
  : _self(self)
  , _link_id(link_id)
  , _manager(manager)
  , _config(std::move(config))
  , _cluster_connection(std::move(cluster_connection))
  , _replication_mgr(
      _manager->scheduling_group(),
      std::move(config_provider),
      std::move(data_source_factory),
      std::move(data_sink_factory),
      replication::replication_probe::configuration{
        .group_name = link_probe::shadow_link_group,
        .labels = {link_probe::shadow_link_name(_config->name)}})
  , _task_reconciler_interval(task_reconciler_interval)
  , _probe{} {}

link::~link() noexcept = default;

ss::future<> link::start() {
    vlog(
      cllog.info,
      "Starting cluster link {} ({})",
      _config->name,
      _config->uuid);

    // Allow exception to propagate to the caller
    _probe = std::make_unique<link_probe>(*this);

    co_await _cluster_connection->start();
    co_await _replication_mgr.start(_probe->get_link_data());
    co_await run_task_reconciler();
    _task_reconciler.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] {
            return ss::with_scheduling_group(
              _manager->scheduling_group(),
              [this] { return run_task_reconciler(); });
        });
    });
    _task_reconciler.arm_periodic(_task_reconciler_interval);
}

ss::future<> link::stop() noexcept {
    vlog(
      cllog.info,
      "Stopping cluster link {} ({})",
      _config->name,
      _config->uuid);
    _task_reconciler.cancel();
    _as.request_abort();
    co_await _replication_mgr.stop();
    co_await _gate.close();

    for (auto& [_, t] : _tasks) {
        vlog(
          cllog.info,
          "Stopping task {} for cluster link {}",
          t->name(),
          _config->name);
        auto res_f = co_await ss::coroutine::as_future(stop_task(t.get()));
        if (res_f.failed()) {
            auto exception = res_f.get_exception();
            vlog(
              cllog.warn, "Failed to stop task {}: {}", t->name(), exception);
            continue;
        }
        auto res = res_f.get();
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
        co_await _cluster_connection->stop();
    } catch (const std::exception& e) {
        vlog(cllog.warn, "Error shutting down cluster connection: {}", e);
    }

    _probe.reset(nullptr);

    vlog(cllog.info, "Stopped link {} ({})", _config->name, _config->uuid);
}

ss::future<cl_result<void>> link::register_task(task_factory* tf) {
    co_await ss::coroutine::switch_to(_manager->scheduling_group());
    vlog(
      cllog.debug,
      "Registering task factory {} for cluster link {} ({})",
      tf->created_task_name(),
      _config->name,
      _config->uuid);
    auto t = tf->create_task(this);

    if (!t) {
        co_return err_info(
          errc::task_creation_failed,
          ssx::sformat(
            "Failed to create task from factory {} for cluster link {}",
            tf->created_task_name(),
            _config->name));
    }

    co_return co_await do_register_task(std::move(t));
}

void link::update_config(
  model::metadata_ptr config, ::model::revision_id revision) {
    vlog(
      cllog.debug,
      "Updating cluster link {} ({}): {} using revision: {}",
      _config->name,
      _config->uuid,
      config,
      revision);
    chunked_vector<::model::topic> new_topics_to_replicate;
    chunked_vector<::model::topic> topics_no_longer_mirroring;
    for (const auto& [topic, m] : config->state.mirror_topics) {
        if (
          !_config->state.mirror_topics.contains(topic)
          && mirror_active_state(m.status)) {
            vlog(cllog.debug, "New topic to replicate: {}", topic);
            new_topics_to_replicate.push_back(topic);
        }
    }
    for (const auto& [topic, _] : _config->state.mirror_topics) {
        if (!config->state.mirror_topics.contains(topic)) {
            topics_no_longer_mirroring.push_back(topic);
        }
    }
    _config = config;
    maybe_update_connection_configuration();

    for (auto& [_, t] : _tasks) {
        vlog(cllog.trace, "Updating config for task {}", t->name());
        t->update_config(*_config);
    }

    // Configuration updates may change whether or not the tasks have been
    // enabled, spawn off a task reconciler fiber to handle this
    ssx::spawn_with_gate(_gate, [this] {
        return ss::with_scheduling_group(_manager->scheduling_group(), [this] {
            return run_task_reconciler();
        });
    });

    // reconcile the replicators. We do not need replicators running
    // if the link is not active or a specific topic in the link is not
    // active.
    if (!requires_active_replicators()) {
        vlog(
          cllog.debug,
          "Link {} is not active, stopping all replicators",
          _config->name);
        _replication_mgr.stop_replicators();
    } else {
        for (const auto& topic : topics_no_longer_mirroring) {
            vlog(
              cllog.debug,
              "Topic {} is no longer mirrored, stopping its replicators",
              topic);
            _replication_mgr.stop_replicators(topic);
        }
        for (const auto& [topic, _] : _config->state.mirror_topics) {
            if (requires_active_replicators(topic)) {
                continue;
            }
            vlog(
              cllog.debug,
              "Topic {} on link {} is not active, stopping its replicators",
              topic,
              _config->name);
            _replication_mgr.stop_replicators(topic);
        }
        handle_new_topics_to_replicate(std::move(new_topics_to_replicate));
    }
}

bool link::requires_active_replicators() const {
    switch (_config->state.status) {
    case model::link_status::active:
        // check below for topic status overrides
        return true;
    case model::link_status::paused:
        return false;
    }
}

bool link::requires_active_replicators(const ::model::topic& topic) const {
    if (!requires_active_replicators()) {
        return false;
    }
    const auto& mts = _config->state.mirror_topics;
    auto it = mts.find(topic);
    if (it == mts.end()) {
        return false;
    }
    return mirror_active_state(it->second.status);
}

ss::future<> link::handle_on_leadership_change(
  ::model::ntp ntp,
  ntp_leader is_ntp_leader,
  std::optional<::model::term_id> term) {
    co_await ss::coroutine::switch_to(_manager->scheduling_group());
    vlog(
      cllog.trace,
      "Shadow link {} handling leadership change for {}: {}, term: {}",
      _config->name,
      ntp,
      is_ntp_leader,
      term);

    const auto& mirror_topics = _config->state.mirror_topics;
    if (mirror_topics.contains(ntp.tp.topic)) {
        vlog(
          cllog.debug,
          "[{}] Leadership change event for partition {}, is_leader: {}",
          _link_id,
          ntp,
          is_ntp_leader);
        auto needs_replicators = requires_active_replicators(ntp.tp.topic);
        if (!needs_replicators) {
            vlog(
              cllog.info,
              "[{}] Topic {} is not active, will stop any active replicators",
              _link_id,
              ntp.tp.topic);
        }
        if (is_ntp_leader && needs_replicators) {
            vassert(
              term, "Term must be set when leadership is assumed: {}", ntp);
            _replication_mgr.start_replicator(ntp, *term);
        } else {
            _replication_mgr.stop_replicator(ntp, term);
        }
    }

    _probe->handle_on_leadership_change(ntp, is_ntp_leader);

    // todo: add debouncing here so that we do not trigger multiple
    // reconciliation loops at once.
    co_await run_task_reconciler();
}

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
    report.link_name = _config->name;
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
link::update_mirror_topic_state(model::update_mirror_topic_status_cmd cmd) {
    return _manager->update_mirror_topic_state(_link_id, std::move(cmd));
}

ss::future<::cluster::cluster_link::errc> link::update_mirror_topic_properties(
  model::update_mirror_topic_properties_cmd cmd) {
    return _manager->update_mirror_topic_properties(_link_id, std::move(cmd));
}

model::metadata_ptr link::get_config() const noexcept { return _config; }

topic_metadata_cache& link::topic_metadata_cache() noexcept {
    return _manager->topic_metadata_cache();
}

partition_leader_cache& link::partition_leader_cache() noexcept {
    return _manager->partition_leader_cache();
}

security_service& link::get_security_service() noexcept {
    return _manager->get_security_service();
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

topic_creator& link::topic_creator() noexcept {
    return _manager->topic_creator();
}

kafka::client::cluster& link::get_cluster_connection() noexcept {
    return *_cluster_connection;
}

consumer_groups_router& link::get_group_router() {
    return _manager->get_group_router();
}

partition_metadata_provider& link::get_partition_metadata_provider() {
    return _manager->get_partition_metadata_provider();
}

kafka_rpc_client_service& link::get_kafka_rpc_client_service() {
    return _manager->get_kafka_rpc_client_service();
}

std::optional<chunked_hash_map<::model::topic, model::mirror_topic_metadata>>
link::get_mirror_topics_for_link() const {
    return _manager->get_mirror_topics_for_link(_link_id);
}

chunked_hash_map<::model::ntp, replication::partition_offsets_report>
link::get_partition_offsets_report() const {
    return _replication_mgr.get_partition_offsets_report();
}

members_table_provider& link::get_members_table_provider() noexcept {
    return _manager->get_members_table_provider();
}

bool link::should_start_task(task* t) const {
    return t->should_start(ss::this_shard_id(), _self);
}

bool link::should_pause_task(task* t) const {
    return t->should_pause(ss::this_shard_id(), _self);
}

bool link::should_stop_task(task* t) const {
    return t->should_stop(ss::this_shard_id(), _self);
}

ss::future<> link::run_task_reconciler() {
    /**
     * Always run the task reconciler in the manager scheduling
     * group this way task fibers will be run in the correct
     * scheduling group
     */
    // TODO: consider adding a separate scheduling group per task
    co_await ss::coroutine::switch_to(_manager->scheduling_group());
    auto fut = co_await ss::coroutine::as_future(
      _task_reconciler_mutex.get_units(_as));
    if (fut.failed()) {
        // abort source triggered, exit early
        co_return;
    }
    auto units = std::move(fut).get();

    vlog(
      cllog.trace,
      "Running task reconciler for cluster link {}",
      _config->name);
    // Iterate over all tasks and reconcile their state
    for (auto& [name, t] : _tasks) {
        if (!_as.abort_requested() && should_start_task(t.get())) {
            vlog(
              cllog.info,
              "Reconciler starting task {} for cluster link {}",
              name,
              _config->name);
            auto res = co_await start_task(t.get());
            if (!res) {
                vlog(
                  cllog.error,
                  "Failed to start task {}: {}",
                  name,
                  res.assume_error().message());
            }
        }

        if (!_as.abort_requested() && should_pause_task(t.get())) {
            vlog(
              cllog.info,
              "Reconciler pausing task {} for cluster link {}",
              name,
              _config->name);
            auto res = co_await pause_task(t.get());
            if (!res) {
                vlog(
                  cllog.error,
                  "Failed to pause task {}: {}",
                  name,
                  res.assume_error().message());
            }
        }

        if (should_stop_task(t.get())) {
            vlog(
              cllog.debug,
              "Reconciler stopping task {} for cluster link {}",
              name,
              _config->name);
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

ss::future<cl_result<void>> link::do_register_task(std::unique_ptr<task> t) {
    vlog(
      cllog.debug,
      "Registering task {} for cluster link {} ({})",
      t->name(),
      _config->name,
      _config->uuid);
    if (_tasks.contains(t->name())) {
        auto msg = ssx::sformat(
          "Task named '{}' already exists for link {}",
          t->name(),
          _config->name);
        vlog(cllog.warn, "{}", msg);
        co_return err_info(
          errc::task_already_registered_on_link, std::move(msg));
    }

    cl_result<void> res = outcome::success();
    // Do not need to unregister the task callback as the task lifetime is
    // managed by the link
    t->register_for_updates([this](
                              std::string_view task_name,
                              task::state_change change) {
        vlog(
          cllog.debug, "Task {} reported state change: {}", task_name, change);
        _task_state_change_notifications.notify(
          _config->name, task_name, change);
    });
    // If we register a task after the link has started, then check to see if it
    // should start and do so
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
    if (!_as.abort_requested() && should_pause_task(t.get())) {
        vlog(cllog.info, "Pausing task {}", t->name());
        res = co_await pause_task(t.get());
        if (!res) {
            vlog(
              cllog.error,
              "Failed to pause task {}: {}",
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

void link::maybe_update_connection_configuration() {
    auto kafka_cfg = metadata_to_kafka_config(*_config);
    if (kafka_cfg == _cluster_connection->configuration()) {
        return;
    }

    vlog(
      cllog.info,
      "Updating connection configuration for link {}: {}",
      _config->name,
      kafka_cfg);
    _cluster_connection->update_configuration(std::move(kafka_cfg));
}

void link::handle_new_topics_to_replicate(
  chunked_vector<::model::topic> new_topics) {
    vlog(cllog.debug, "New topics to replicate: {}", new_topics);

    for (const auto& topic : new_topics) {
        auto topic_cfg = _manager->topic_metadata_cache().find_topic_cfg(
          {::model::kafka_namespace, topic});
        if (!topic_cfg) {
            vlog(
              cllog.trace,
              "Topic {} does not exist yet. Leadership changes will trigger "
              "replicators",
              topic);
            continue;
        }
        auto partition_count = topic_cfg->partition_count;
        for (auto p : std::views::iota(int32_t{0}, partition_count)) {
            auto part_id = ::model::partition_id{p};
            auto ntp = ::model::ntp(::model::kafka_namespace, topic, part_id);
            auto leader_node
              = _manager->partition_leader_cache().get_leader_node(ntp);
            if (!leader_node) {
                vlog(
                  cllog.trace,
                  "Topic {} partition {} does not have a leader yet.  "
                  "Leadership changes will trigger replicators",
                  topic,
                  part_id);
                continue;
            }
            if (*leader_node != _self) {
                vlog(cllog.trace, "Not the leader for {}. Skipping", ntp);
                continue;
            }
            if (!_manager->partition_manager().is_current_shard_leader(ntp)) {
                vlog(
                  cllog.trace,
                  "Topic {} partition {} is not on this shard. Skipping",
                  topic,
                  part_id);
                continue;
            }

            auto term = _manager->partition_manager().get_term(ntp);
            if (!term.has_value()) {
                vlog(
                  cllog.trace,
                  "Topic {} partition {} does not have a term. Skipping",
                  topic,
                  part_id);
                continue;
            }

            vlog(cllog.debug, "Starting replicator for {}", ntp);
            _replication_mgr.start_replicator(
              {::model::kafka_namespace, topic, part_id}, *term);
        }
    }
}
} // namespace cluster_link
