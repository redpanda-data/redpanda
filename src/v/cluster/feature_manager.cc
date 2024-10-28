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
#include "cluster/health_monitor_backend.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "config/types.h"
#include "config/validators.h"
#include "features/feature_state.h"
#include "features/feature_table.h"
#include "model/timeout_clock.h"
#include "pandaproxy/schema_registry/schema_id_validation.h"
#include "raft/group_manager.h"
#include "security/role_store.h"
#include "security/types.h"

#include <seastar/core/semaphore.hh>

#include <absl/algorithm/container.h>
#include <fmt/format.h>

#include <stdexcept>

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
  ss::sharded<security::role_store>& role_store,
  ss::sharded<topic_table>& topic_table,
  raft::group_id raft0_group)
  : _stm(stm)
  , _as(as)
  , _members(members)
  , _group_manager(group_manager)
  , _hm_frontend(hm_frontend)
  , _hm_backend(hm_backend)
  , _feature_table(table)
  , _connection_cache(connection_cache)
  , _role_store(role_store)
  , _topic_table(topic_table)
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
        const node_health_report& report,
        std::optional<ss::lw_shared_ptr<const node_health_report>>) {
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
            } else if (_am_controller_leader) {
                // In any case, kick the background update loop when
                // we gain leadership, in case there is work to do like
                // auto-activating some features.
                _update_wait.signal();
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
                            [this] { return maybe_update_feature_table(); });
                      }).handle_exception([](const std::exception_ptr& e) {
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
    _verified_enterprise_license.broken();
    co_await _gate.close();
}

features::enterprise_feature_report
feature_manager::report_enterprise_features() const {
    const auto& cfg = config::shard_local_cfg();
    const auto& node_cfg = config::node();
    auto has_gssapi = [&cfg]() {
        return absl::c_any_of(
          cfg.sasl_mechanisms(), [](const auto& m) { return m == "GSSAPI"; });
    };
    auto has_oidc = []() {
        return config::oidc_is_enabled_kafka()
               || config::oidc_is_enabled_http();
    };
    auto has_schema_id_validation = [&cfg]() {
        return cfg.enable_schema_id_validation()
               != pandaproxy::schema_registry::schema_id_validation_mode::none;
    };
    auto fips_enabled = [&node_cfg]() {
        auto fips_mode = node_cfg.fips_mode();
        return fips_mode == config::fips_mode_flag::permissive
               || fips_mode == config::fips_mode_flag::enabled;
    };
    auto n_roles = _role_store.local().size();
    auto has_non_default_roles
      = n_roles >= 2
        || (n_roles == 1 && !_role_store.local().contains(security::default_role));
    auto leadership_pinning_enabled = [&cfg, this]() {
        if (cfg.default_leaders_preference() != config::leaders_preference{}) {
            return true;
        }
        for (const auto& topic : _topic_table.local().topics_map()) {
            if (topic.second.get_configuration()
                  .properties.leaders_preference) {
                return true;
            }
        }
        return false;
    };

    features::enterprise_feature_report report;
    report.set(
      features::license_required_feature::audit_logging, cfg.audit_enabled());
    report.set(
      features::license_required_feature::cloud_storage,
      cfg.cloud_storage_enabled());
    report.set(
      features::license_required_feature::partition_auto_balancing_continuous,
      cfg.partition_autobalancing_mode()
        == model::partition_autobalancing_mode::continuous);
    report.set(
      features::license_required_feature::core_balancing_continuous,
      cfg.core_balancing_continuous());
    report.set(features::license_required_feature::gssapi, has_gssapi());
    report.set(features::license_required_feature::oidc, has_oidc());
    report.set(
      features::license_required_feature::schema_id_validation,
      has_schema_id_validation());
    report.set(features::license_required_feature::rbac, has_non_default_roles);
    report.set(features::license_required_feature::fips, fips_enabled());
    report.set(
      features::license_required_feature::datalake_iceberg,
      cfg.iceberg_enabled());
    report.set(
      features::license_required_feature::leadership_pinning,
      leadership_pinning_enabled());
    return report;
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
    try {
        co_await ss::sleep_abortable(license_check_retry, _as.local());
    } catch (const ss::sleep_aborted&) {
        // Shutting down - next iteration will drop out
        co_return;
    }
    if (_feature_table.local().is_active(features::feature::license)) {
        auto enterprise_features = report_enterprise_features();
        if (enterprise_features.any()) {
            const auto& license = _feature_table.local().get_license();
            if (!license || license->is_expired()) {
                vlog(
                  clusterlog.warn,
                  "A Redpanda Enterprise Edition license is required to use "
                  "enterprise features: ([{}]). Enter an active license key "
                  "(for example, rpk cluster license set <key>). To request a "
                  "license, see https://redpanda.com/license-request. For more "
                  "information, see "
                  "https://docs.redpanda.com/current/get-started/licenses.",
                  fmt::join(enterprise_features.enabled(), ", "));
            }
        }
    }
}

bool feature_manager::need_to_verify_enterprise_license() {
    return features::is_major_version_upgrade(
      _feature_table.local().get_active_version(),
      _feature_table.local().get_latest_logical_version());
}

void feature_manager::verify_enterprise_license() {
    vlog(clusterlog.debug, "Verifying enterprise license...");

    if (!need_to_verify_enterprise_license()) {
        vlog(clusterlog.debug, "Enterprise license verification skipped...");
        _verified_enterprise_license.signal();
        return;
    }

    const auto& license = _feature_table.local().get_license();
    std::optional<security::license> fallback_license = std::nullopt;
    auto fallback_license_str = std::getenv(
      "REDPANDA_FALLBACK_ENTERPRISE_LICENSE");
    if (fallback_license_str != nullptr) {
        try {
            fallback_license.emplace(
              security::make_license(fallback_license_str));
        } catch (const security::license_exception& e) {
            // Log the error and continue without a fallback license
            vlog(
              clusterlog.warn,
              "Failed to parse fallback license: {}",
              e.what());
        }
    }

    auto invalid = [](const std::optional<security::license>& license) {
        return !license || license->is_expired();
    };
    auto license_missing_or_expired = invalid(license)
                                      && invalid(fallback_license);
    auto enterprise_features = report_enterprise_features();

    vlog(
      clusterlog.info,
      "Verifying enterprise license: active_version={}, latest_version={}, "
      "enterprise_features=[{}], license_missing_or_expired={}{}",
      _feature_table.local().get_active_version(),
      _feature_table.local().get_latest_logical_version(),
      enterprise_features.enabled(),
      license_missing_or_expired,
      fallback_license ? " (detected fallback license)" : "");

    if (enterprise_features.any() && license_missing_or_expired) {
        throw std::runtime_error{fmt::format(
          "A Redpanda Enterprise Edition license is required to use enterprise "
          "features: ([{}]). To add your license, downgrade this broker to the "
          "pre-upgrade version, and enter the active license key (for example, "
          "rpk cluster license set). To request a license, see "
          "https://redpanda.com/license-request. For more information, see "
          "https://docs.redpanda.com/current/get-started/licenses.",
          fmt::join(enterprise_features.enabled(), ", "))};
    }

    _verified_enterprise_license.signal();
}

ss::future<> feature_manager::maybe_update_feature_table() {
    // Before doing any feature enablement or active version update, check that
    // the cluster has an enterprise license if they use enterprise features
    if (need_to_verify_enterprise_license()) {
        vlog(
          clusterlog.debug,
          "Waiting for enterprise license to be verified before checking for "
          "active version updates...");
        // Immediately release the semaphore units because we are only using it
        // to wait for the precondition to be met
        std::ignore = co_await ss::get_units(
          _verified_enterprise_license, 1, _as.local());
    }

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
          "Exception updating cluster version, will retry ({})",
          std::current_exception());
        failed = true;
    }

    try {
        // Even if we didn't advance our logical version, we might have some
        // features to activate due to policy changes across different redpanda
        // binaries with the same logical version
        co_await do_maybe_activate_features();
    } catch (...) {
        vlog(
          clusterlog.debug,
          "Exception activating features, will retry ({})",
          std::current_exception());
        failed = true;
    }

    try {
        if (failed) {
            // Sleep for a while before next iteration of our outer do_until
            co_await ss::sleep_abortable(status_retry, _as.local());
        } else {
            // Sleep until we have some node version updates to process, or
            // some feature activations to do.
            co_await _update_wait.wait([this]() { return updates_pending(); });
        }
    } catch (const ss::condition_variable_timed_out&) {
        // Wait complete - proceed around next loop of do_until
    } catch (const ss::broken_condition_variable&) {
        // Shutting down - nextiteration will drop out
    } catch (const ss::sleep_aborted&) {
        // Shutting down - next iteration will drop out
    }
}

std::vector<std::reference_wrapper<const features::feature_spec>>
feature_manager::auto_activate_features(
  cluster_version original_version, cluster_version effective_version) {
    std::vector<std::reference_wrapper<const features::feature_spec>> result;

    if (config::shard_local_cfg().features_auto_enable()) {
        /*
         * We auto-enable features if:
         * - The global features_auto_enable setting is true.
         * - They are not disabled
         * - One of:
         *   * Their policy is set to `always` and required_version is satisfied
         *     by cluster's current version
         *   * Their policy is set to `new_clusters_only` and required_version
         *     is satisfied by cluster's original version.
         */
        for (const auto& fs : _feature_table.local().get_feature_state()) {
            if ((fs.get_state() == features::feature_state::state::unavailable ||
           fs.get_state() == features::feature_state::state::available) &&
          ((fs.spec.available_rule ==
                features::feature_spec::available_policy::always &&
            effective_version >= fs.spec.require_version) ||
           (fs.spec.available_rule ==
                features::feature_spec::available_policy::new_clusters_only &&
            original_version >= fs.spec.require_version))) {
                result.push_back(fs.spec);
            }
        }
    }

    return result;
}

ss::future<>
feature_manager::replicate_feature_update_cmd(feature_update_cmd_data data) {
    auto new_version = data.logical_version;
    auto cmd = feature_update_cmd(
      std::move(data),
      0 // unused
    );

    auto timeout = model::timeout_clock::now() + status_retry;
    auto err = co_await replicate_and_wait(_stm, _as, std::move(cmd), timeout);
    if (err == errc::not_leader) {
        // Harmless, we lost leadership so the new controller
        // leader is responsible for picking up where we left off.
        co_return;
    } else if (err) {
        // Raise exception to trigger backoff+retry
        throw std::runtime_error(fmt::format(
          "Error storing cluster version {}: {}", new_version, err));
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

        auto is_alive_opt = _hm_frontend.local().is_alive(node_id);
        if (!is_alive_opt.has_value()) {
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

        } else if (is_alive_opt == alive::no) {
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
    for (const auto spec : auto_activate_features(
           _feature_table.local().get_original_version(), max_version)) {
        vlog(
          clusterlog.info,
          "Auto-activating feature {} (logical version {})",
          spec.get().name,
          max_version);
        data.actions.push_back(cluster::feature_update_action{
          .feature_name = ss::sstring(spec.get().name),
          .action = feature_update_action::action_t::activate});
    }

    co_await replicate_feature_update_cmd(std::move(data));

    vlog(clusterlog.info, "Updated cluster (logical version {})", max_version);
}

ss::future<> feature_manager::do_maybe_activate_features() {
    if (!_am_controller_leader) {
        co_return;
    }

    auto activate_features = auto_activate_features(
      _feature_table.local().get_original_version(),
      _feature_table.local().get_active_version());
    if (!activate_features.empty()) {
        vlog(clusterlog.info, "Activating features after upgrade...");

        auto data = feature_update_cmd_data{
          .logical_version = _feature_table.local().get_active_version()};
        for (const auto spec : activate_features) {
            vlog(
              clusterlog.info,
              "Activating feature {} (logical version {})",
              spec.get().name,
              _feature_table.local().get_active_version());
            data.actions.push_back(cluster::feature_update_action{
              .feature_name = ss::sstring(spec.get().name),
              .action = feature_update_action::action_t::activate});
        }

        co_await replicate_feature_update_cmd(std::move(data));
    }
}

ss::future<std::error_code>
feature_manager::update_license(security::license&& license) {
    const auto timeout = model::timeout_clock::now() + 5s;

    auto cmd = cluster::feature_update_license_update_cmd(
      cluster::feature_update_license_update_cmd_data{
        .redpanda_license = license},
      0 // unused
    );
    auto err = co_await replicate_and_wait(_stm, _as, std::move(cmd), timeout);
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
        return replicate_and_wait(_stm, _as, std::move(cmd), timeout);
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
