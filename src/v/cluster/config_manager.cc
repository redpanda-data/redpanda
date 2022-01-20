/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config_manager.h"

#include "cluster/config_frontend.h"
#include "cluster/controller_service.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/partition_leaders_table.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "resource_mgmt/io_priority.h"
#include "utils/file_io.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

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

config_manager::config_manager(
  config_manager::preload_result preload,
  ss::sharded<config_frontend>& cf,
  ss::sharded<rpc::connection_cache>& cc,
  ss::sharded<partition_leaders_table>& pl,
  ss::sharded<ss::abort_source>& as)
  : _self(config::node().node_id())
  , _frontend(cf)
  , _connection_cache(cc)
  , _leaders(pl)
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
    (void)ss::try_with_gate(
      _gate,
      [this] {
          return ss::do_until(
            [this] {
                return _as.local().abort_requested() || _bootstrap_complete;
            },
            [this]() -> ss::future<> {
                if (_seen_version != config_version_unset) {
                    vlog(
                      clusterlog.info,
                      "Bootstrap complete (version {})",
                      _seen_version);
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
                        co_await ss::sleep_abortable(
                          bootstrap_retry, _as.local());
                    }
                }
            });
      })
      .handle_exception_type([](ss::sleep_aborted const&) {})
      .handle_exception_type([](ss::gate_closed_exception const&) {})
      .handle_exception([](const std::exception_ptr&) {
          // Explicitly handle exception so that we do not risk an
          // 'ignored exceptional future' error.  The only exceptions
          // we expect here are things like sleep_aborted during shutdown.
          vlog(
            clusterlog.warn,
            "Exception during bootstrap: {}",
            std::current_exception());
      });
}

/**
 * For all non-default values in the current runtime configuration,
 * inject them into the raft0 cluster configuration as a single delta.
 *
 {* This is done once on the first startup (first overall, or first
 * since upgrading to a redpanda version with central config)
 */
ss::future<> config_manager::do_bootstrap() {
    config_update update;

    config::shard_local_cfg().for_each([&update](
                                         const config::base_property& p) {
        if (!p.is_default()) {
            rapidjson::StringBuffer buf;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
            p.to_json(writer);
            ss::sstring key_str(p.name());
            ss::sstring val_str = buf.GetString();
            vlog(clusterlog.info, "Importing property {}={}", key_str, val_str);
            update.upsert.push_back({key_str, val_str});
        }
    });

    // Version of the first write
    _frontend.local().set_next_version(config_version{1});

    try {
        co_await _frontend.local().patch(
          update, model::timeout_clock::now() + bootstrap_write_timeout);
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

        // Background: wait til we have a leader. If this node is leader
        // then bootstrap cluster configuration from local config.
        start_bootstrap();
    } else {
        vlog(
          clusterlog.trace,
          "Starting config_manager... (seen version {})",
          _seen_version);
        _frontend.local().set_next_version(_seen_version + config_version{1});
    }

    vlog(clusterlog.trace, "Starting reconcile_status...");

    // Detach fiber
    (void)ss::try_with_gate(
      _gate,
      [this] {
          return ss::do_until(
            [this] { return _as.local().abort_requested(); },
            [this] { return reconcile_status(); });
      })
      .handle_exception_type([](ss::gate_closed_exception const&) {})
      .handle_exception([](std::exception_ptr const& e) {
          vlog(clusterlog.warn, "Exception from reconcile_status: {}", e);
      });

    return ss::now();
}

ss::future<> config_manager::stop() {
    vlog(clusterlog.info, "stopping cluster::config_manager...");
    _reconcile_wait.broken();
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
 * Local filesystem location where we write out the most recently
 * seen configuration.  Does not have to exist.
 */
std::filesystem::path config_manager::cache_path() {
    return config::node().data_directory().path / cache_file;
}

static void preload_local(
  ss::sstring const& key,
  YAML::Node const& value,
  std::optional<std::reference_wrapper<config_manager::preload_result>>
    result) {
    auto& cfg = config::shard_local_cfg();
    if (cfg.contains(key)) {
        try {
            // Cache values are string-ized yaml.  In many cases this
            // is the same as the underlying value (e.g. integers, bools),
            // but for strings it's not (the literal value in the cache is
            // "\"foo\"").
            auto decoded = YAML::Load(value.as<std::string>());
            auto& property = cfg.get(key);
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
                  YAML::Dump(value));
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

/**
 * Before other redpanda subsystems are initialized, read the config
 * cache and use it to bring the global cluster configuration somewhat
 * up to date before the raft0 log is replayed.
 */
ss::future<config_manager::preload_result> config_manager::preload() {
    // TODO: refactor yaml load workaround for lack of string view,
    // it's also used in node_config load.

    // TODO: legacy path: if cache isn't present, load from redpanda-cfg
    // (currently we rely on application.cc to do this)

    // TODO: assert that node_config was already loaded (we rely on
    // data_directory)

    preload_result result;

    YAML::Node config;
    try {
        auto buf = co_await read_fully(cache_path());
        auto workaround = ss::uninitialized_string(buf.size_bytes());
        auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
        in.consume_to(buf.size_bytes(), workaround.begin());
        config = YAML::Load(workaround);
    } catch (std::filesystem::filesystem_error const& e) {
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
              "reconcile_status: sending status update to leader");
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
                      "leader: "
                      "{}",
                      r.error().message());
                    failed = true;
                }
            }
        }
    } else {
        vlog(clusterlog.trace, "reconcile_status: up to date, sleeping");
    }

    try {
        if (failed) {
            // If we were dirty & failed to send our update, sleep until retry
            co_await ss::sleep_abortable(status_retry, _as.local());
        } else if (should_send_status()) {
            // Our status updated while we were sending, proceed
            // immediately to next iteration of loop.
        } else {
            // We are clean: sleep until signalled.
            co_await _reconcile_wait.wait();
        }
    } catch (ss::condition_variable_timed_out) {
        // Wait complete - proceed around next loop of do_until
    } catch (ss::broken_condition_variable) {
        // Shutting down - nextiteration will drop out
    } catch (ss::sleep_aborted) {
        // Shutting down - next iteration will drop out
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
apply_local(cluster_config_delta_cmd_data const& data, bool silent) {
    auto& cfg = config::shard_local_cfg();
    auto result = config_manager::apply_result{};
    for (const auto& u : data.upsert) {
        if (!cfg.contains(u.first)) {
            // We never heard of this property.  Record it as unknown
            // in our config_status.
            if (!silent) {
                vlog(clusterlog.info, "Unknown property {}", u.first);
            }
            result.unknown.push_back(u.first);
            continue;
        }
        auto& property = cfg.get(u.first);
        auto& val_yaml = u.second;

        try {
            auto val = YAML::Load(val_yaml);

            vlog(
              clusterlog.trace,
              "apply: upsert {}={}",
              u.first,
              property.format_raw(val_yaml));

            bool changed = property.set_value(val);
            result.restart |= (property.needs_restart() && changed);
        } catch (YAML::ParserException) {
            if (!silent) {
                vlog(
                  clusterlog.warn,
                  "Invalid syntax in property {}: {}",
                  u.first,
                  property.format_raw(val_yaml));
            }
            result.invalid.push_back(u.first);
            continue;
        } catch (YAML::BadConversion) {
            if (!silent) {
                vlog(
                  clusterlog.warn,
                  "Invalid value for property {}: {}",
                  u.first,
                  property.format_raw(val_yaml));
            }
            result.invalid.push_back(u.first);
            continue;
        } catch (std::bad_alloc) {
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
                  u.first,
                  property.format_raw(val_yaml),
                  std::current_exception());
            }
            result.invalid.push_back(u.first);
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
        property.reset();
    }

    return result;
}

void config_manager::merge_apply_result(
  config_status& status,
  cluster_config_delta_cmd_data const& data,
  apply_result const& r) {
    vlog(
      clusterlog.trace,
      "merge_apply_result: data {} {}",
      fmt::ptr(&data),
      data.upsert.size());
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
        if (!errored_properties.contains(i.first)) {
            ok_properties.insert(i.first);
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
ss::future<> config_manager::store_delta(
  config_version const& delta_version,
  cluster_config_delta_cmd_data const& data) {
    _seen_version = delta_version;

    // version_shard is chosen to match controller_stm_shard, so
    // our raft0 stm apply operations do not need a core jump to
    // update the frontend version state.
    vassert(
      ss::this_shard_id() == config_frontend::version_shard,
      "Must be called on frontend version_shard");
    _frontend.local().set_next_version(_seen_version + config_version{1});

    for (const auto& u : data.upsert) {
        _raw_values[u.first] = u.second;
    }
    for (const auto& d : data.remove) {
        _raw_values.erase(d);
    }

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
    out << YAML::Key << std::string(version_key) << YAML::Value
        << _seen_version;
    for (const auto& i : _raw_values) {
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

    const cluster_config_delta_cmd_data& data = cmd.value;
    vlog(
      clusterlog.trace,
      "apply_delta: {} upserts, {} removes",
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
    co_await store_delta(delta_version, data);

    // Signal status update loop to wake up
    _reconcile_wait.signal();

    co_return errc::success;
}

ss::future<std::error_code>
config_manager::apply_status(cluster_config_status_cmd&& cmd) {
    auto node_id = cmd.key;
    auto data = std::move(cmd.value);

    // TODO: hook into node decom to remove nodes from
    // the status map.

    vlog(
      clusterlog.trace,
      "apply_status: updating node {}: {}",
      node_id,
      data.status);

    status[node_id] = data.status;

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

} // namespace cluster
