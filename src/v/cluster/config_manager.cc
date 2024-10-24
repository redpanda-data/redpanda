/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config_manager.h"

#include "base/vlog.h"
#include "cluster/config_frontend.h"
#include "cluster/controller_service.h"
#include "cluster/controller_snapshot.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "resource_mgmt/io_priority.h"
#include "rpc/connection_cache.h"
#include "utils/file_io.h"

#include <seastar/core/coroutine.hh>

#include <absl/container/flat_hash_map.h>

#include <algorithm>

namespace cluster {

// After failure to send status to leader, how long to
// wait before retrying.
static constexpr std::chrono::seconds status_retry = 5s;

// During first start bootstrap, if we are not leader then
// check again after this long.
static constexpr std::chrono::seconds bootstrap_retry = 5s;

// Timeout for raft write of bootstrap delta
static constexpr std::chrono::seconds bootstrap_write_timeout = 5s;

// In on-disk cache, the key used to store config version number
static constexpr std::string_view version_key = "_version";

// The filename within the data directory to use for config cache
static constexpr std::string_view cache_file = "config_cache.yaml";

// The filename within the data directory to use for first-start bootstrap
static constexpr std::string_view bootstrap_file = ".bootstrap.yaml";

config_manager::config_manager(
  config_manager::preload_result preload,
  ss::sharded<config_frontend>& cf,
  ss::sharded<rpc::connection_cache>& cc,
  ss::sharded<partition_leaders_table>& pl,
  ss::sharded<features::feature_table>& ft,
  ss::sharded<cluster::members_table>& mt,
  ss::sharded<ss::abort_source>& as)
  : _self(*config::node().node_id())
  , _frontend(cf)
  , _connection_cache(cc)
  , _leaders(pl)
  , _feature_table(ft)
  , _members(mt)
  , _as(as) {
    if (ss::this_shard_id() == controller_stm_shard) {
        // Only the controller stm shard handles updates: leave these
        // members in default initialized state on other shards.
        _seen_version = preload.version;
        my_latest_status.node = _self;
        my_latest_status.version = preload.version;
        _raw_values = preload.raw_values;
        my_latest_status.invalid = preload.invalid;
        my_latest_status.unknown = preload.unknown;
        /**
         * Register notification immediately not to lose status updates.
         */
        _member_update_notification
          = _members.local().register_members_updated_notification(
            [this](model::node_id id, model::membership_state new_state) {
                handle_cluster_members_update(id, new_state);
            });
    }
}

/**
 * Start a background fiber that waits for a leader, then on the leader
 * primes the cluster config from its current local configuration.
 *
 * That current local configuration was set earlier by application.cc
 * from a local yaml file.
 *
 * This is the code path for upgrades from pre-central-config redpanda
 * clusters, and for first startup on new clusters/nodes, if the new
 * nodes have a bootstrap yaml file.
 */
void config_manager::start_bootstrap() {
    // Detach fiber
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return ss::do_until(
                            [this] {
                                return _as.local().abort_requested()
                                       || _bootstrap_complete;
                            },
                            [this] { return wait_for_bootstrap(); });
                      }).handle_exception([](const std::exception_ptr& e) {
        // Explicitly handle exception so that we do not risk an
        // 'ignored exceptional future' error.  The only exceptions
        // we expect here are things like sleep_aborted during shutdown.
        vlog(clusterlog.warn, "Exception during bootstrap: {}", e);
    });
}

ss::future<> config_manager::wait_for_bootstrap() {
    if (_seen_version != config_version_unset) {
        vlog(clusterlog.info, "Bootstrap complete (version {})", _seen_version);
        _bootstrap_complete = true;
        co_return;
    } else {
        auto leader = co_await _leaders.local().wait_for_leader(
          model::controller_ntp,
          model::timeout_clock::now() + bootstrap_retry,
          _as.local());
        if (leader == _self) {
            // We are the leader.  Proceed to bootstrap cluster
            // configuration from our local configuration.
            co_await do_bootstrap();
            vlog(clusterlog.info, "Completed bootstrap as leader");
        } else {
            // Someone else got leadership.  Maybe they
            // successfully bootstrap config, maybe they don't.
            // Wait a short time before checking again.
            co_await ss::sleep_abortable(bootstrap_retry, _as.local());
        }
    }
}

/**
 * For all non-default values in the current runtime configuration,
 * inject them into the raft0 cluster configuration as a single delta.
 *
 {* This is done once on the first startup (first overall, or first
 * since upgrading to a redpanda version with central config)
 */
ss::future<> config_manager::do_bootstrap() {
    config_update_request update;

    config::shard_local_cfg().for_each(
      [&update](const config::base_property& p) {
          if (!p.is_default()) {
              json::StringBuffer buf;
              json::Writer<json::StringBuffer> writer(buf);
              p.to_json(writer, config::redact_secrets::no);
              ss::sstring key_str(p.name());
              ss::sstring val_str = buf.GetString();
              vlog(clusterlog.info, "Importing property {}", p);
              update.upsert.emplace_back(key_str, val_str);
          }
      });

    // Version of the first write
    co_await _frontend.local().set_next_version(config_version{1});

    try {
        auto patch_result = co_await _frontend.local().patch(
          std::move(update),
          model::timeout_clock::now() + bootstrap_write_timeout);
        if (patch_result.errc) {
            vlog(
              clusterlog.warn,
              "Failed to write bootstrap delta: {}",
              patch_result.errc.message());
        } else {
            vlog(
              clusterlog.debug,
              "Wrote bootstrap config version {}",
              patch_result.version);
        }
    } catch (...) {
        // On errors, just drop out: start_bootstrap will go around
        // its loop again.
        vlog(
          clusterlog.warn,
          "Failed to write bootstrap delta: {}",
          std::current_exception());
    }
}

ss::future<> config_manager::start() {
    if (_seen_version == config_version_unset) {
        vlog(clusterlog.trace, "Starting config_manager... (initial)");

        // Background: wait till we have a leader. If this node is leader
        // then bootstrap cluster configuration from local config.
        start_bootstrap();
    } else {
        vlog(
          clusterlog.trace,
          "Starting config_manager... (seen version {})",
          _seen_version);
        co_await _frontend.local().set_next_version(
          _seen_version + config_version{1});
    }

    vlog(clusterlog.trace, "Starting reconcile_status...");

    // Detach fiber
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return ss::do_until(
                            [this] { return _as.local().abort_requested(); },
                            [this] { return reconcile_status(); });
                      }).handle_exception([](const std::exception_ptr& e) {
        vlog(clusterlog.warn, "Exception from reconcile_status: {}", e);
    });

    _raft0_leader_changed_notification
      = _leaders.local().register_leadership_change_notification(
        model::controller_ntp,
        [this](model::ntp, model::term_id, model::node_id) {
            _reconcile_wait.signal();
        });

    co_return co_await ss::now();
}
void config_manager::handle_cluster_members_update(
  model::node_id id, model::membership_state new_state) {
    vlog(
      clusterlog.debug,
      "Processing membership notification: {{id: {} state: {}}}",
      id,
      new_state);
    if (new_state == model::membership_state::active) {
        // add an empty status placeholder if node is not yet known
        status.try_emplace(id, config_status{.node = id});
    } else if (new_state == model::membership_state::removed) {
        status.erase(id);
    }
}

ss::future<> config_manager::stop() {
    vlog(clusterlog.info, "Stopping Config Manager...");
    _reconcile_wait.broken();
    _members.local().unregister_members_updated_notification(
      _member_update_notification);
    _leaders.local().unregister_leadership_change_notification(
      _raft0_leader_changed_notification);
    co_await _gate.close();
}

/**
 * For mux_state_machine: indicate which record types we handle
 */
bool config_manager::is_batch_applicable(const model::record_batch& b) {
    return b.header().type == model::record_batch_type::cluster_config_cmd;
}

/**
 * True if our status in the raft0-populated map of node statuses
 * differs from our actual status.
 */
bool config_manager::should_send_status() {
    if (auto status_i = status.find(_self); status_i != status.end()) {
        return my_latest_status.version > config_version_unset
               && my_latest_status != status_i->second;
    } else {
        return my_latest_status.version > config_version_unset;
    }
}
/**
 * Local filesystem location where installers may write initial cluster
 * configuration before first redpanda startup for one-time load.
 *
 * This is defined relative to node configuration file, so that users
 * have some control over the location, without having to define
 * a whole separate node configuration property for where to find the bootstrap
 * file.
 */
std::filesystem::path config_manager::bootstrap_path() {
    return config::node().get_cfg_file_path().parent_path() / bootstrap_file;
}

/**
 * Local filesystem location where we write out the most recently
 * seen configuration.  Does not have to exist.
 */
std::filesystem::path config_manager::cache_path() {
    return config::node().data_directory().path / cache_file;
}

static void preload_local(
  const ss::sstring& key,
  const ss::sstring& raw_value,
  std::optional<std::reference_wrapper<config_manager::preload_result>>
    result) {
    auto& cfg = config::shard_local_cfg();
    if (cfg.contains(key)) {
        auto& property = cfg.get(key);
        try {
            // Cache values are string-ized yaml.  In many cases this
            // is the same as the underlying value (e.g. integers, bools),
            // but for strings it's not (the literal value in the cache is
            // "\"foo\"").
            auto decoded = YAML::Load(raw_value);
            property.set_value(decoded);

            // Because we are in preload, it doesn't matter if the property
            // requires restart.  We are setting it before anything else
            // can be using it.

            if (result.has_value()) {
                vlog(
                  clusterlog.trace,
                  "Loaded property {}={} from local cache",
                  key,
                  property.format_raw(YAML::Dump(decoded)));
            }
        } catch (...) {
            if (result.has_value()) {
                vlog(
                  clusterlog.info,
                  "Ignoring invalid property: {}={}",
                  key,
                  property.format_raw(raw_value));
                result.value().get().invalid.push_back(key);
            }
        }
    } else {
        if (result.has_value()) {
            vlog(clusterlog.info, "Ignoring unknown property: {}", key);
            result.value().get().unknown.push_back(key);
        }
    }
}

static void preload_local(
  const ss::sstring& key,
  const YAML::Node& value,
  std::optional<std::reference_wrapper<config_manager::preload_result>>
    result) {
    auto& cfg = config::shard_local_cfg();
    if (cfg.contains(key)) {
        auto& property = cfg.get(key);
        std::string raw_value;
        try {
            raw_value = value.as<std::string>();
        } catch (...) {
            if (result.has_value()) {
                vlog(
                  clusterlog.info,
                  "Ignoring invalid property: {}={}",
                  key,
                  property.format_raw(YAML::Dump(value)));
                result.value().get().invalid.push_back(key);
            }
            return;
        }

        return preload_local(key, ss::sstring(std::move(raw_value)), result);
    } else {
        if (result.has_value()) {
            vlog(clusterlog.info, "Ignoring unknown property: {}", key);
            result.value().get().unknown.push_back(key);
        }
    }
}

ss::future<config_manager::preload_result>
config_manager::preload_join(const controller_join_snapshot& snap) {
    preload_result result;

    result.version = snap.config.version;
    for (const auto& i : snap.config.values) {
        result.raw_values.insert(i);
        auto& key = i.first;
        auto& value = i.second;

        // Run locally to get validation fields of result
        preload_local(key, value, std::ref(result));

        // Broadcast value to all shards
        co_await ss::smp::invoke_on_all(
          [&key, &value]() { preload_local(key, value, std::nullopt); });
    }

    co_return result;
}

ss::future<config_manager::preload_result>
config_manager::preload(const YAML::Node& legacy_config) {
    auto result = co_await load_cache();

    if (result.version == cluster::config_version_unset) {
        // No config cache, first start?  Try reading bootstrap file.
        auto bootstrap_success = co_await load_bootstrap();

        if (!bootstrap_success) {
            // No cache and no bootstrap file: fall back to legacy
            // config read (this is an upgrade or a cluster configured
            // in the old fashioned way)
            co_await load_legacy(legacy_config);
        }
    } else {
        // We are post-upgrade.  If there are still cluster config
        // settings in redpanda.yaml, nag the user about them.  This
        // helps them figure out what's going on if they are trying
        // to set something in redpanda.yaml and it's not working.
        if (legacy_config["redpanda"]) {
            const auto nag_properties
              = config::shard_local_cfg().property_names_and_aliases();
            for (const auto& node : legacy_config["redpanda"]) {
                auto name = node.first.as<ss::sstring>();
                if (nag_properties.contains(name)) {
                    vlog(
                      clusterlog.info,
                      "Ignoring value for '{}' in redpanda.yaml: use `rpk "
                      "cluster config edit` to edit cluster configuration "
                      "properties.",
                      name);
                }
            }
        }
    }

    co_return result;
}

/**
 * Load the bootstrap config -- call this on first start.
 *
 * @return true if we found a bootstrap file (even if not everything in it was
 * valid)
 */
ss::future<bool> config_manager::load_bootstrap() {
    YAML::Node config;
    try {
        auto config_str = co_await read_fully_to_string(bootstrap_path());
        config = YAML::Load(config_str);
    } catch (const std::filesystem::filesystem_error& e) {
        // This is normal on upgrade from pre-config_manager version or
        // on newly added node.  Also permitted later if user
        // chooses to e.g. blow away config cache during disaster recovery.
        vlog(clusterlog.info, "Can't load config bootstrap file: {}", e);
        co_return false;
    }

    // This node has never seen a cluster configuration message.
    // Bootstrap configuration from local yaml file.
    auto errors = config::shard_local_cfg().read_yaml(config, {});
    for (const auto& i : errors) {
        vlog(
          clusterlog.warn,
          "Invalid bootstrap property '{}' validation error: {}",
          i.first,
          i.second);
    }

    co_await ss::smp::invoke_on_all(
      [&config] { config::shard_local_cfg().read_yaml(config, {}); });

    co_return true;
}

ss::future<> config_manager::load_legacy(const YAML::Node& legacy_config) {
    co_await ss::smp::invoke_on_all(
      [&legacy_config] { config::shard_local_cfg().load(legacy_config); });

    // This node has never seen a cluster configuration message.
    // Bootstrap configuration from local yaml file.
    auto errors = config::shard_local_cfg().load(legacy_config);

    // Report any invalid properties.  Do not refuse to start redpanda,
    // as the properties will have been either ignored or clamped
    // to safe values.
    for (const auto& i : errors) {
        vlog(
          clusterlog.warn,
          "Cluster property '{}' validation error: {}",
          i.first,
          i.second);
    }
}

/**
 * Before other redpanda subsystems are initialized, read the config
 * cache and use it to bring the global cluster configuration somewhat
 * up to date before the raft0 log is replayed.
 */
ss::future<config_manager::preload_result> config_manager::load_cache() {
    preload_result result;

    // Cluster config load requires node config load to already be complete
    assert(!config::node().data_directory().path.empty());

    YAML::Node config;
    try {
        auto config_str = co_await read_fully_to_string(cache_path());
        config = YAML::Load(config_str);
    } catch (const std::filesystem::filesystem_error& e) {
        // This is normal on upgrade from pre-config_manager version or
        // on newly added node.  Also permitted later if user
        // chooses to e.g. blow away config cache during disaster recovery.
        vlog(clusterlog.info, "Can't load config cache: {}", e);
        co_return result;
    }

    for (const auto& i : config) {
        ss::sstring key = i.first.as<std::string>();
        auto& value = i.second;
        if (key == version_key) {
            result.version = value.as<config_version>();
            continue;
        }

        // Run locally to get validation fields of result
        preload_local(key, value, std::ref(result));

        // Store raw values in the result

        if (value.IsScalar()) {
            result.raw_values[key] = value.Scalar();
        } else {
            result.raw_values[key] = YAML::Dump(value);
        }

        // Broadcast value to all shards
        co_await ss::smp::invoke_on_all(
          [&key, &value]() { preload_local(key, value, std::nullopt); });
    }

    co_return result;
}

/**
 * Call this function iteratively on each node to check if it needs
 * to send a config_status_request update to the controller leader.
 *
 * If no update is needed, sleeps on _reconcile_status, the condition
 * variable that is signalled when our local status changes.
 */
ss::future<> config_manager::reconcile_status() {
    bool failed = false;
    if (should_send_status()) {
        auto timeout = 5s;

        auto leader = _leaders.local().get_leader(model::controller_ntp);
        if (!leader) {
            vlog(
              clusterlog.trace,
              "reconcile_status: no leader to send status to");
            failed = true;
        } else {
            vlog(
              clusterlog.trace,
              "reconcile_status: sending status update to leader: {}",
              my_latest_status);
            if (_self == *leader) {
                auto err = co_await _frontend.local().set_status(
                  my_latest_status, model::timeout_clock::now() + timeout);
                if (err) {
                    vlog(
                      clusterlog.info,
                      "reconcile_status: failed applying local status update: "
                      "{}",
                      err.message());
                    failed = true;
                }
            } else {
                auto r
                  = co_await _connection_cache.local()
                      .with_node_client<cluster::controller_client_protocol>(
                        _self,
                        ss::this_shard_id(),
                        *leader,
                        timeout,
                        [this, timeout](controller_client_protocol cp) mutable {
                            return cp.config_status(
                              config_status_request{.status = my_latest_status},
                              rpc::client_opts(
                                model::timeout_clock::now() + timeout));
                        });
                if (r.has_error()) {
                    // This is not logged as warning/error because it's expected
                    // in certain situations like transferring leadership of
                    // the raft0 partition
                    vlog(
                      clusterlog.info,
                      "reconcile_status: failed sending status update to "
                      "leader: {}",
                      r.error().message());
                    failed = true;
                }
            }
        }
    } else {
        vlog(clusterlog.trace, "reconcile_status: up to date, sleeping");
    }

    try {
        if (failed || should_send_status()) {
            co_await _reconcile_wait.wait(status_retry);
        } else {
            // We are clean: sleep until signalled.
            co_await _reconcile_wait.wait(
              [this]() { return should_send_status(); });
        }
    } catch (const ss::condition_variable_timed_out&) {
        // Wait complete - proceed around next loop of do_until
    } catch (const ss::broken_condition_variable&) {
        // Shutting down - nextiteration will drop out
    }
}

/**
 * Apply a configuration delta to the local shard's configuration
 * object.  This delta is already persistent, and now being replayed
 * from raft0 into our in memory state.
 *
 * @param silent if true, do not log issues with properties.  Useful
 *               when invoking on N shards to avoid spamming the
 *               same errors to the log N times.
 *
 * @return an `apply_result` indicating any issues, to be fed back
 *         into cluster_status by the caller.
 */
config_manager::apply_result
apply_local(const cluster_config_delta_cmd_data& data, bool silent) {
    auto& cfg = config::shard_local_cfg();
    auto result = config_manager::apply_result{};
    for (const auto& u : data.upsert) {
        if (!cfg.contains(u.key)) {
            // We never heard of this property.  Record it as unknown
            // in our config_status.
            if (!silent) {
                vlog(clusterlog.info, "Unknown property {}", u.key);
            }
            result.unknown.push_back(u.key);
            continue;
        }
        auto& property = cfg.get(u.key);
        auto& val_yaml = u.value;

        try {
            auto val = YAML::Load(val_yaml);

            vlog(
              clusterlog.trace,
              "apply: upsert {}={}",
              u.key,
              property.format_raw(val_yaml));

            auto validation_err = property.validate(val);
            if (validation_err.has_value()) {
                if (!silent) {
                    vlog(
                      clusterlog.warn,
                      "Invalid property value {}: {} ({})",
                      u.key,
                      property.format_raw(val_yaml),
                      validation_err.value().error_message());
                }
                result.invalid.push_back(u.key);

                // We still proceed to set_value after failing validation.
                // The property class is responsible for clamping or rejecting
                // invalid values as needed, thereby handling invalid values
                // that might have been set forcefully or be stored on disk from
                // earlier redpanda versions with weaker validation.
            }

            bool changed = property.set_value(val);
            result.restart |= (property.needs_restart() && changed);
        } catch (const YAML::ParserException&) {
            if (!silent) {
                vlog(
                  clusterlog.warn,
                  "Invalid syntax in property {}: {}",
                  u.key,
                  property.format_raw(val_yaml));
            }
            result.invalid.push_back(u.key);
            continue;
        } catch (const YAML::BadConversion&) {
            if (!silent) {
                vlog(
                  clusterlog.warn,
                  "Invalid value for property {}: {}",
                  u.key,
                  property.format_raw(val_yaml));
            }
            result.invalid.push_back(u.key);
            continue;
        } catch (const std::bad_alloc&) {
            // Don't include bad_alloc in the catch-all below
            throw;
        } catch (...) {
            // Because `property` is subclassed, it's hard to predict
            // what other exceptions specific property subclasses
            // might emit when trying to parse their values.  We definitely
            // don't want to let those exceptions jam up the controller,
            // so catch-all as an invalid property value.
            if (!silent) {
                vlog(
                  clusterlog.warn,
                  "Unexpected error setting property {}={}: {}",
                  u.key,
                  property.format_raw(val_yaml),
                  std::current_exception());
            }
            result.invalid.push_back(u.key);
            continue;
        }
    }

    for (const auto& r : data.remove) {
        vlog(clusterlog.trace, "apply: remove {}", r);

        if (!cfg.contains(r)) {
            // Ignore: if we have never heard of the property, removing
            // its value is a no-op.
            continue;
        }

        auto& property = cfg.get(r);
        result.restart |= property.needs_restart();
        try {
            property.reset();
        } catch (...) {
            // Most probably one of the watch callbacks is buggy and failed to
            // handle the update. Don't stop the controller STM, but log with
            // error severity so that at least we can catch these bugs in tests.
            if (!silent) {
                vlog(
                  clusterlog.error,
                  "Unexpected error resetting property {}: {}",
                  r,
                  std::current_exception());
            }
            continue;
        }
    }

    return result;
}

void config_manager::merge_apply_result(
  config_status& status,
  const cluster_config_delta_cmd_data& data,
  const apply_result& r) {
    status.restart |= r.restart;

    std::set<ss::sstring> errored_properties;
    for (const auto& i : r.unknown) {
        errored_properties.insert(i);
    }
    for (const auto& i : r.invalid) {
        errored_properties.insert(i);
    }

    // Clear status for any OK or removed properties from this delta
    std::set<ss::sstring> ok_properties;
    for (const auto& i : data.remove) {
        ok_properties.insert(i);
    }
    for (const auto& i : data.upsert) {
        if (!errored_properties.contains(i.key)) {
            ok_properties.insert(i.key);
        }
    }

    // Filter out any previously errored properties that are now OK
    std::erase_if(status.unknown, [&ok_properties](const ss::sstring& p) {
        return ok_properties.contains(p);
    });
    std::erase_if(status.invalid, [&ok_properties](const ss::sstring& p) {
        return ok_properties.contains(p);
    });

    // Add any newly errored properties
    for (const auto& i : r.unknown) {
        if (
          std::find(status.unknown.begin(), status.unknown.end(), i)
          == status.unknown.end()) {
            status.unknown.push_back(i);
        }
    }
    for (const auto& i : r.invalid) {
        if (
          std::find(status.invalid.begin(), status.invalid.end(), i)
          == status.invalid.end()) {
            status.invalid.push_back(i);
        }
    }
}

/**
 * Update persistent local cache of config.
 *
 * This cache is for early loading during startup.  It is NOT required
 * to be strictly up to date when we finish processing a delta.
 *
 * @param version
 * @param data
 * @return
 */
ss::future<>
config_manager::store_delta(const cluster_config_delta_cmd_data& data) {
    auto& cfg = config::shard_local_cfg();

    for (const auto& u : data.upsert) {
        /// skip section
        if (!cfg.contains(u.key)) {
            // passthrough unknown values
            _raw_values[u.key] = u.value;
            continue;
        }

        /// conversion + cleanup section
        auto& prop = cfg.get(u.key);
        if (prop.name() == u.key) {
            // u key is already the main name of the property
            _raw_values[u.key] = u.value;
        } else {
            // ensure only the main name is used
            _raw_values[ss::sstring{prop.name()}] = u.value;
        }
        // cleanup any old alias lying around (it should be normally not
        // necessary, and at most one loop)
        for (const auto& alias : prop.aliases()) {
            _raw_values.erase(ss::sstring{alias});
        }
    }
    for (const auto& d : data.remove) {
        _raw_values.erase(d);
    }

    return write_local_cache(_seen_version, _raw_values);
}

ss::future<> config_manager::write_local_cache(
  config_version seen_version,
  const std::map<ss::sstring, ss::sstring>& raw_values) {
    // Assumption: configuration objects are small.
    // Marshal the whole thing in a single buffer.
    // TODO: enforce a length limit on config
    // settings to avoid malicious users making
    // this buffer/file huge.

    // Construct a YAML document of raw values: the
    // values are whatever we were sent in a delta,
    // so they are stored string-ized.  This file
    // format looks different to a YAML config file
    // -- it is only proximately human readable,
    // for use in disaster recovery, not ordinary
    // hand editing.
    YAML::Emitter out;
    out << YAML::Comment("Auto-generated, do not edit this file "
                         "unless Redpanda will not start");
    out << YAML::BeginMap;
    out << YAML::Key << std::string(version_key) << YAML::Value << seen_version;
    for (const auto& i : raw_values) {
        out << YAML::Key << i.first << YAML::Value << i.second;
    }
    out << YAML::EndMap;

    try {
        // TODO: do a clean write tmp + mv, to
        // avoid possibility of torn writes.  Note
        // that an fsync is not necessary as long
        // as the write is atomic: it is safe for
        // the cache to be a little behind.
        iobuf buf;
        buf.append(out.c_str(), out.size());
        co_await write_fully(cache_path(), std::move(buf));
    } catch (...) {
        // Failure to write the cache (maybe we're
        // out of disk space?) is not fatal.  It
        // just means that on next restart we'll
        // apply the deltas since the last time we
        // successfully wrote it.
        vlog(
          clusterlog.warn,
          "Failed writing config cache: {}",
          std::current_exception());
    }
}

ss::future<std::error_code>
config_manager::apply_delta(cluster_config_delta_cmd&& cmd_in) {
    // Explicitly move onto stack, because the input argument
    // may be invalidated after first coroutine sleep
    auto cmd = std::move(cmd_in);

    const config_version delta_version = cmd.key;
    if (delta_version <= _seen_version) {
        vlog(
          clusterlog.trace,
          "apply delta: ignore {} <= {}",
          delta_version,
          _seen_version);
        co_return errc::success;
    }
    _seen_version = delta_version;
    // version_shard is chosen to match controller_stm_shard, so
    // our raft0 stm apply operations do not need a core jump to
    // update the frontend version state.
    vassert(
      ss::this_shard_id() == config_frontend::version_shard,
      "Must be called on frontend version_shard");
    co_await _frontend.local().set_next_version(
      _seen_version + config_version{1});

    const cluster_config_delta_cmd_data& data = cmd.value;
    vlog(
      clusterlog.trace,
      "apply_delta version {}: {} upserts, {} removes",
      delta_version,
      data.upsert.size(),
      data.remove.size());

    // Update shard-local copies of configuration.  Use our local shard's
    // apply to learn of any bad properties (all copies will have the same
    // errors, so only need to check the errors on one)
    auto apply_r = apply_local(data, false);

    co_await ss::smp::invoke_on_all([&data] { apply_local(data, true); });

    // Merge results from this delta into our status.
    my_latest_status.version = delta_version;
    merge_apply_result(my_latest_status, data, apply_r);

    // Store the raw values (irrespective of any issues applying them) for
    // early replay on next startup.
    co_await store_delta(data);

    // Signal status update loop to wake up
    _reconcile_wait.signal();

    co_return errc::success;
}

ss::future<std::error_code>
config_manager::apply_status(cluster_config_status_cmd&& cmd) {
    auto node_id = cmd.key;
    auto data = std::move(cmd.value);

    vlog(
      clusterlog.trace,
      "apply_status: updating node {}: {}",
      node_id,
      data.status);
    // do not apply status to the map if node is removed
    if (_members.local().contains(node_id)) {
        status[node_id] = data.status;
    }

    co_return errc::success;
}
ss::future<std::error_code>
config_manager::apply_update(model::record_batch b) {
    auto cmd_var = co_await cluster::deserialize(
      std::move(b), accepted_commands);

    auto gate_holder = _gate.hold();

    co_return co_await ss::visit(
      cmd_var,
      [this](cluster_config_delta_cmd cmd) {
          return apply_delta(std::move(cmd));
      },
      [this](cluster_config_status_cmd cmd) {
          return apply_status(std::move(cmd));
      });
}

config_manager::status_map config_manager::get_projected_status() const {
    status_map r = status;

    // If our local status is ahead of the persistent status map,
    // update the projected result: the persistent status map is
    // guaranteed to catch up to this eventually via reconcile_status.
    //
    // This behaviour is useful to get read-after-write consistency
    // when writing config updates to the controller leader + then
    // reading back the status from the same leader node.
    //
    // A more comprehensive approach to waiting for status updates
    // inline with config updates is discussed in
    // https://github.com/redpanda-data/redpanda/issues/5833
    auto it = r.find(_self);
    if (it == r.end() || it->second.version < my_latest_status.version) {
        r[_self] = my_latest_status;
    }

    return r;
}

ss::future<> config_manager::fill_snapshot(controller_snapshot& snap) const {
    snap.config.version = _seen_version;

    snap.config.values.insert(_raw_values.begin(), _raw_values.end());

    auto& nodes_status = snap.config.nodes_status;
    for (const auto& kv : status) {
        nodes_status.push_back(kv.second);
    }

    return ss::now();
}

ss::future<>
config_manager::apply_snapshot(model::offset, const controller_snapshot& snap) {
    status.clear();
    for (const auto& st : snap.config.nodes_status) {
        status.emplace(st.node, st);
    }

    // craft a delta that, when applied, will be equivalent to the set of values
    // in the snapshot.
    const auto& values = snap.config.values;
    cluster_config_delta_cmd_data delta;
    delta.upsert.reserve(values.size());
    for (const auto& [key, val] : values) {
        delta.upsert.emplace_back(key, val);
    }
    for (const auto& [key, val] : _raw_values) {
        if (!values.contains(key)) {
            delta.remove.push_back(key);
        }
    }

    auto ec = co_await apply_delta(
      cluster_config_delta_cmd{snap.config.version, std::move(delta)});
    if (ec) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Failed to apply config_manager part of controller snapshot: {} ({})",
          ec.message(),
          ec));
    }
}

} // namespace cluster
