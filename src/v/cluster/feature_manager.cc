/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "feature_manager.h"

#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller_service.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/members_table.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/timeout_clock.h"
#include "raft/group_manager.h"

#include <absl/algorithm/container.h>

namespace cluster {

static constexpr std::chrono::seconds status_retry = 5s;

feature_manager::feature_manager(
  ss::sharded<controller_stm>& stm,
  ss::sharded<ss::abort_source>& as,
  ss::sharded<members_table>& members,
  ss::sharded<raft::group_manager>& group_manager,
  ss::sharded<health_monitor_frontend>& hm_frontend,
  ss::sharded<health_monitor_backend>& hm_backend,
  ss::sharded<features::feature_table>& table,
  ss::sharded<rpc::connection_cache>& connection_cache,
  raft::group_id raft0_group)
  : _stm(stm)
  , _as(as)
  , _members(members)
  , _group_manager(group_manager)
  , _hm_frontend(hm_frontend)
  , _hm_backend(hm_backend)
  , _feature_table(table)
  , _connection_cache(connection_cache)
  , _raft0_group(raft0_group)
  , _barrier_state(
      *config::node().node_id(),
      members.local(),
      as.local(),
      _gate,
      [this](
        model::node_id from,
        model::node_id to,
        feature_barrier_tag tag,
        bool entered)
        -> ss::future<result<rpc::client_context<feature_barrier_response>>> {
          auto timeout = 5s;
          return _connection_cache.local()
            .with_node_client<cluster::controller_client_protocol>(
              from,
              ss::this_shard_id(),
              to,
              timeout,
              [from, tag, timeout, entered](
                controller_client_protocol cp) mutable {
                  return cp.feature_barrier(
                    feature_barrier_request{
                      .tag = tag, .peer = from, .entered = entered},
                    rpc::client_opts(model::timeout_clock::now() + timeout));
              });
      }

    ) {}

ss::future<>
feature_manager::start(std::vector<model::node_id>&& cluster_founder_nodes) {
    vlog(clusterlog.info, "Starting...");

    // Register for node health change notifications
    _health_notify_handle = _hm_backend.local().register_node_callback(
      [this](
        node_health_report const& report,
        std::optional<std::reference_wrapper<const node_health_report>>) {
          // If we did not know the node's version or if the report is
          // higher, submit an update.
          auto i = _node_versions.find(report.id);
          if (
            i == _node_versions.end()
            || i->second < report.local_state.logical_version) {
              update_node_version(
                report.id, report.local_state.logical_version);
          }
      });

    // Register for leader notifications
    _leader_notify_handle
      = _group_manager.local().register_leadership_notification(
        [this](
          raft::group_id group,
          model::term_id term,
          std::optional<model::node_id> leader_id) {
            // Should never be called with gate closed: group manager
            // is shut down: we unregister leader notification before
            // closing gate.
            _gate.check();

            if (group != _raft0_group) {
                return;
            }

            // On leadership change, clear our map of node versions: this
            // ensures that we will populate it with fresh data when we next
            // see a health report from each node.
            _node_versions.clear();

            vlog(
              clusterlog.debug, "Controller leader notification term {}", term);
            _am_controller_leader = leader_id == *config::node().node_id();

            // This hook avoids the need for the controller leader to receive
            // its own health report to generate a call to update_node_version.
            // Especially useful on first startup of cluster, where the initial
            // leader is the only node, and can immediately store the cluster
            // version based on its own version alone.
            if (
              _feature_table.local().get_active_version()
                != features::feature_table::get_latest_logical_version()
              && _am_controller_leader) {
                // When I become leader for first time (i.e. when active
                // version is not known yet, proactively persist it)
                vlog(
                  clusterlog.debug,
                  "generating version update for controller leader {} ({})",
                  leader_id.value(),
                  features::feature_table::get_latest_logical_version());
                update_node_version(
                  *config::node().node_id(),
                  features::feature_table::get_latest_logical_version());
            }
        });

    // Detach fiber for active version updater.
    // The writes of active version run in the background, because we
    // do it in response to callbacks (e.g. node version change from
    // health monitor) that expect prompt return, so need to do the slower
    // raft0 write in the background.
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return ss::do_until(
                            [this] { return _as.local().abort_requested(); },
                            [this] { return maybe_update_active_version(); });
                      }).handle_exception([](std::exception_ptr const& e) {
        vlog(clusterlog.warn, "Exception from updater: {}", e);
    });

    // Detach fiber for alerts of possible license violations or notifications
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
        return ss::do_until(
          [this] { return _as.local().abort_requested(); },
          [this] { return maybe_log_license_check_info(); });
    });

    for (const model::node_id n : cluster_founder_nodes) {
        set_node_to_latest_version(n);
    }

    co_return;
}
ss::future<> feature_manager::stop() {
    vlog(clusterlog.info, "Stopping Feature Manager...");
    _group_manager.local().unregister_leadership_notification(
      _leader_notify_handle);
    _hm_backend.local().unregister_node_callback(_health_notify_handle);
    _update_wait.broken();
    co_await _gate.close();
}

ss::future<> feature_manager::maybe_log_license_check_info() {
    auto license_check_retry = std::chrono::seconds(60 * 5);
    auto interval_override = std::getenv(
      "__REDPANDA_LICENSE_CHECK_INTERVAL_SEC");
    if (interval_override != nullptr) {
        try {
            license_check_retry = std::min(
              std::chrono::seconds{license_check_retry},
              std::chrono::seconds{std::stoi(interval_override)});
            vlog(
              clusterlog.info,
              "Overriding default license log annoy interval to: {}s",
              license_check_retry.count());
        } catch (...) {
            vlog(
              clusterlog.error,
              "Invalid license check interval override '{}'",
              interval_override);
        }
    }
    if (_feature_table.local().is_active(features::feature::license)) {
        const auto& cfg = config::shard_local_cfg();
        auto has_gssapi = [&cfg]() {
            return absl::c_any_of(cfg.sasl_mechanisms(), [](const auto& m) {
                return m == "GSSAPI";
            });
        };
        if (
          cfg.cloud_storage_enabled
          || cfg.partition_autobalancing_mode
               == model::partition_autobalancing_mode::continuous
          || has_gssapi()) {
            const auto& license = _feature_table.local().get_license();
            if (!license || license->is_expired()) {
                vlog(
                  clusterlog.warn,
                  "Looks like you’ve enabled a Redpanda Enterprise feature(s) "
                  "without a valid license. Please enter an active Redpanda "
                  "license key (e.g. rpk cluster license set <key>). If you "
                  "don’t have one, please request a new/trial license at "
                  "https://redpanda.com/license-request");
            }
        }
    }
    try {
        co_await ss::sleep_abortable(license_check_retry, _as.local());
    } catch (ss::sleep_aborted) {
        // Shutting down - next iteration will drop out
    }
}

ss::future<> feature_manager::maybe_update_active_version() {
    vlog(clusterlog.debug, "Checking for active version update...");
    bool failed = false;
    try {
        co_await do_maybe_update_active_version();
    } catch (...) {
        // This is fine: exceptions can result from unavailability of
        // raft0 for writes, or unavailability of health monitor
        // data for one of the nodes.
        vlog(
          clusterlog.debug,
          "Exception from updater, will retry ({})",
          std::current_exception());
        failed = true;
    }

    try {
        if (failed) {
            // Sleep for a while before next iteration of our outer do_until
            co_await ss::sleep_abortable(status_retry, _as.local());
        } else {
            // Sleep until we have some updates to process
            co_await _update_wait.wait([this]() { return !_updates.empty(); });
        }
    } catch (ss::condition_variable_timed_out&) {
        // Wait complete - proceed around next loop of do_until
    } catch (ss::broken_condition_variable&) {
        // Shutting down - nextiteration will drop out
    } catch (ss::sleep_aborted&) {
        // Shutting down - next iteration will drop out
    }
}

void feature_manager::update_node_version(
  model::node_id update_node, cluster_version v) {
    vassert(ss::this_shard_id() == backend_shard, "Wrong shard!");

    vlog(
      clusterlog.debug,
      "update_node_version: enqueuing update node={} version={}",
      update_node,
      v);

    _updates.emplace(update_node, v);
    _update_wait.signal();
}

/**
 * Reconcile node versions & health with the feature state, and make
 * updates as needed.
 *
 * This function is allowed to throw: throwing an exception indicates
 * a transient error that the caller should retry after a backoff.
 */
ss::future<> feature_manager::do_maybe_update_active_version() {
    vassert(ss::this_shard_id() == backend_shard, "Wrong shard!");

    // Consume any accumulated updates.  Important to do this even if
    // not leader, so that we drain it and allow maybe_update_active_version
    // to sleep on _update_wait.
    auto updates = std::exchange(_updates, {});

    if (!_am_controller_leader) {
        co_return;
    }

    // Apply updates into _node_versions
    for (const auto& i : updates) {
        auto& [node, v] = i;
        vlog(clusterlog.debug, "Processing update node={} version={}", node, v);
        _node_versions[node] = v;
    }

    // Check if _node_versions indicates a possible active version update
    const auto active_version = _feature_table.local().get_active_version();
    cluster_version max_version = invalid_version;
    for (const auto& i : _node_versions) {
        max_version = std::max(i.second, max_version);
    }
    if (max_version <= active_version) {
        vlog(
          clusterlog.debug,
          "No update, max version {} not ahead of {}",
          max_version,
          active_version);
        co_return;
    }

    // Conditions for updating active version:
    // A) Version must be known for all member nodes
    // B) All member nodes must be up
    // C) All versions must be >= the new active version

    std::map<model::node_id, node_state> node_status;

    // This call in principle can be a network fetch, but in practice
    // we're only doing it immediately after cluster health has just
    // been updated, so do not expect it to go remote.
    auto node_status_v = co_await _hm_frontend.local().get_nodes_status(
      model::timeout_clock::now() + 5s);
    if (node_status_v.has_error()) {
        // Raise exception to trigger backoff+retry
        throw std::runtime_error(fmt::format(
          "Can't update active cluster version, failed to get health "
          "status: {}",
          node_status_v.error()));
    } else {
        for (const auto& i : node_status_v.value()) {
            node_status.emplace(i.id, i);
        }
    }

    // Ensure that our _node_versions contains versions for all
    // nodes in members_table & that they are all sufficiently recent
    const auto& member_table = _members.local();
    for (const auto& node_id : member_table.node_ids()) {
        auto v_iter = _node_versions.find(node_id);
        if (v_iter == _node_versions.end()) {
            vlog(
              clusterlog.debug,
              "Can't update active version to {} because node {} "
              "version unknown",
              max_version,
              node_id);
            co_return;
        } else if (v_iter->second < max_version) {
            vlog(
              clusterlog.debug,
              "Can't update active version to {} because "
              "node {} "
              "version is too low ({})",
              max_version,
              node_id,
              v_iter->second);
            co_return;
        }

        auto state_iter = node_status.find(node_id);
        if (state_iter == node_status.end()) {
            // Unexpected: the health monitor should be populating
            // state for all known members_table nodes, but this
            // could happen if we raced with a decom or node add.
            // Raise exception to trigger backoff+retry
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Can't update active version to {} because node {} "
              "has no health state",
              max_version,
              node_id));

        } else if (!state_iter->second.is_alive) {
            // Raise exception to trigger backoff+retry
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Can't update active version to {} because node {} "
              "is not alive",
              max_version,
              node_id));
        }
    }

    // All node checks passed, we are ready to increment active version
    auto data = feature_update_cmd_data{.logical_version = max_version};

    // Identify any features which should auto-activate in this version
    if (config::shard_local_cfg().features_auto_enable()) {
        /*
         * We auto-enable features if:
         * - They are not disabled
         * - Their required version is satisfied
         * - Their policy is not explicit_only
         * - The global features_auto_enable setting is true.
         */
        for (const auto& fs : _feature_table.local().get_feature_state()) {
            if (
              fs.get_state() == features::feature_state::state::unavailable
              && fs.spec.available_rule
                   == features::feature_spec::available_policy::always
              && max_version >= fs.spec.require_version) {
                vlog(
                  clusterlog.info,
                  "Auto-activating feature {} (logical version {})",
                  fs.spec.name,
                  max_version);
                data.actions.push_back(cluster::feature_update_action{
                  .feature_name = ss::sstring(fs.spec.name),
                  .action = feature_update_action::action_t::activate});
            }
        }
    }

    auto cmd = feature_update_cmd(
      std::move(data),
      0 // unused
    );

    auto timeout = model::timeout_clock::now() + status_retry;
    auto err = co_await replicate_and_wait(
      _stm, _feature_table, _as, std::move(cmd), timeout);
    if (err == errc::not_leader) {
        // Harmless, we lost leadership so the new controller
        // leader is responsible for picking up where we left off.
        co_return;
    } else if (err) {
        // Raise exception to trigger backoff+retry
        throw std::runtime_error(fmt::format(
          "Error storing cluster version {}: {}", max_version, err));
    }

    vlog(clusterlog.info, "Updated cluster (logical version {})", max_version);
}

ss::future<std::error_code>
feature_manager::update_license(security::license&& license) {
    const auto timeout = model::timeout_clock::now() + 5s;

    auto cmd = cluster::feature_update_license_update_cmd(
      cluster::feature_update_license_update_cmd_data{
        .redpanda_license = license},
      0 // unused
    );
    auto err = co_await replicate_and_wait(
      _stm, _feature_table, _as, std::move(cmd), timeout);
    if (err) {
        co_return err;
    }
    vlog(clusterlog.info, "Loaded new license into cluster: {}", license);
    co_return make_error_code(errc::success);
}

ss::future<std::error_code>
feature_manager::write_action(cluster::feature_update_action action) {
    auto feature_id_opt = _feature_table.local().resolve_name(
      action.feature_name);

    // This should  have been validated by admin server before calling us
    vassert(
      feature_id_opt.has_value(),
      "Invalid feature name '{}'",
      action.feature_name);

    // Validate that the feature is in a state compatible with the
    // requested transition.
    bool valid = true;
    auto state = _feature_table.local().get_state(feature_id_opt.value());
    switch (action.action) {
    case cluster::feature_update_action::action_t::complete_preparing:
        // Look up feature by name
        if (state.get_state() != features::feature_state::state::preparing) {
            // Drop this silently, we presume that this is some kind of
            // race and the thing we thought was preparing is either
            // now active or administratively deactivated.
            valid = false;
        }

        break;
    case cluster::feature_update_action::action_t::activate:
        if (
          state.get_state() != features::feature_state::state::available
          && state.get_state() != features::feature_state::state::disabled_clean
          && state.get_state()
               != features::feature_state::state::disabled_active
          && state.get_state()
               != features::feature_state::state::disabled_preparing) {
            valid = false;
        }
        break;
    case cluster::feature_update_action::action_t::deactivate:
        if (
          state.get_state() == features::feature_state::state::disabled_clean
          || state.get_state()
               == features::feature_state::state::disabled_preparing
          || state.get_state()
               == features::feature_state::state::disabled_active) {
            valid = false;
        }
    }

    if (!valid) {
        vlog(
          clusterlog.warn,
          "Dropping feature action {}, feature not in expected state "
          "(state={})",
          action,
          state.get_state());
        return ss::make_ready_future<std::error_code>(cluster::errc::success);
    } else {
        // Construct and dispatch command to log
        auto timeout = model::timeout_clock::now() + status_retry;
        auto data = feature_update_cmd_data{
          .logical_version = _feature_table.local().get_active_version(),
          .actions = {action}};
        auto cmd = feature_update_cmd(
          std::move(data),
          0 // unused
        );
        return replicate_and_wait(
          _stm, _feature_table, _as, std::move(cmd), timeout);
    }
}

void feature_manager::set_node_to_latest_version(const model::node_id node_id) {
    const cluster_version latest
      = features::feature_table::get_latest_logical_version();
    if (const version_map::iterator i = _node_versions.find(node_id);
        i != _node_versions.end()) {
        if (i->second == latest) {
            return; // already there
        }
        vlog(
          clusterlog.info,
          "Overriding a previously set version for {} from {} to {}",
          node_id,
          i->second,
          latest);
    }
    update_node_version(node_id, latest);
}

} // namespace cluster
