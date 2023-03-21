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

#include "feature_table.h"

#include "cluster/types.h"
#include "config/node_config.h"
#include "features/logger.h"
#include "version.h"

#include <seastar/core/abort_source.hh>

// The feature table is closely related to cluster and uses many types from it
using namespace cluster;

namespace features {

std::string_view to_string_view(feature f) {
    switch (f) {
    case feature::central_config:
        return "central_config";
    case feature::consumer_offsets:
        return "consumer_offsets";
    case feature::maintenance_mode:
        return "maintenance_mode";
    case feature::mtls_authentication:
        return "mtls_authentication";
    case feature::rm_stm_kafka_cache:
        return "rm_stm_kafka_cache";
    case feature::serde_raft_0:
        return "serde_raft_0";
    case feature::license:
        return "license";
    case feature::raft_improved_configuration:
        return "raft_improved_configuration";
    case feature::transaction_ga:
        return "transaction_ga";
    case feature::raftless_node_status:
        return "raftless_node_status";
    case feature::rpc_v2_by_default:
        return "rpc_v2_by_default";
    case feature::cloud_retention:
        return "cloud_retention";
    case feature::node_id_assignment:
        return "node_id_assignment";
    case feature::replication_factor_change:
        return "replication_factor_change";
    case feature::ephemeral_secrets:
        return "ephemeral_secrets";
    case feature::seeds_driven_bootstrap_capable:
        return "seeds_driven_bootstrap_capable";
    case feature::tm_stm_cache:
        return "tm_stm_cache";
    case feature::kafka_gssapi:
        return "kafka_gssapi";
    case feature::partition_move_revert_cancel:
        return "partition_move_cancel_revert";
    case feature::node_isolation:
        return "node_isolation";
    case feature::group_offset_retention:
        return "group_offset_retention";
    case feature::rpc_transport_unknown_errc:
        return "rpc_transport_unknown_errc";
    case feature::membership_change_controller_cmds:
        return "membership_change_controller_cmds";
    case feature::controller_snapshots:
        return "controller_snapshots";
    case feature::cloud_storage_manifest_format_v2:
        return "cloud_storage_manifest_format_v2";
    case feature::transaction_partitioning:
        return "transaction_partitioning";

    /*
     * testing features
     */
    case feature::test_alpha:
        return "__test_alpha";
    case feature::test_bravo:
        return "__test_bravo";
    }
    __builtin_unreachable();
}

std::string_view to_string_view(feature_state::state s) {
    switch (s) {
    case feature_state::state::unavailable:
        return "unavailable";
    case feature_state::state::available:
        return "available";
    case feature_state::state::preparing:
        return "preparing";
    case feature_state::state::active:
        return "active";
    case feature_state::state::disabled_active:
        return "disabled_active";
    case feature_state::state::disabled_preparing:
        return "disabled_preparing";
    case feature_state::state::disabled_clean:
        return "disabled_clean";
    }
    __builtin_unreachable();
};

// The version that this redpanda node will report: increment this
// on protocol changes to raft0 structures, like adding new services.
//
// For your convenience, a rough guide to the history of how logical
// versions mapped to redpanda release versions:
//  22.1.1 -> 3  (22.1.5 was version 4)
//  22.2.1 -> 5  (22.2.6 later proceeds to version 6)
//  22.3.1 -> 7  (22.3.6 later proceeds to verison 8)
//  23.1.1 -> 9
//  23.2.1 -> 10
//
// Although some previous stable branches have included feature version
// bumps, this is _not_ the intended usage, as stable branches are
// meant to be safely downgradable within the branch, and new features
// imply that new data formats may be written.
static constexpr cluster_version latest_version = cluster_version{10};

// The earliest version we can upgrade from.  This is the version that
// a freshly initialized node will start at: e.g. a 23.1 Redpanda joining
// a cluster of 22.3.6 peers would do this:
// - Start up blank, initialize feature table to version 7
// - Send join request advertising version range 7-9
// - 22.3.x peer accepts join request because version range 7-9 includes its
//   active version (7).
// - The new 23.1 node advances feature table to version 8 when it joins and
//   sees controller log replay.
// - Eventually once all nodes are 23.1, all nodes advance active version to 9
static constexpr cluster_version earliest_version = cluster_version{7};

feature_table::feature_table() {
    // Intentionally undocumented environment variable, only for use
    // in integration tests.
    const bool enable_test_features
      = (std::getenv("__REDPANDA_TEST_FEATURES") != nullptr);

    _feature_state.reserve(feature_schema.size());
    for (const auto& spec : feature_schema) {
        if (spec.name.substr(0, 6) == "__test" && !enable_test_features) {
            continue;
        }
        _feature_state.emplace_back(feature_state{spec});
    }
}

ss::future<> feature_table::stop() {
    _as.request_abort();

    // Don't trust callers to have fired their abort source in the right
    // order wrt this class's stop(): forcibly abort anyone still waiting,
    // so that our gate close is guaranteed to proceed.
    _waiters_active.abort_all();
    _waiters_preparing.abort_all();

    return _gate.close();
}

/**
 * The latest version is hardcoded in normal operation.  This getter
 * exists to enable injection of synthetic versions in integration tests.
 */
cluster_version feature_table::get_latest_logical_version() {
    // Avoid getenv on every call by keeping a shard-local cache
    // of the version after applying any environment override.
    static thread_local cluster_version latest_version_cache{invalid_version};

    if (latest_version_cache == invalid_version) {
        latest_version_cache = latest_version;

        auto override = std::getenv("__REDPANDA_LATEST_LOGICAL_VERSION");
        if (override != nullptr) {
            try {
                latest_version_cache = cluster_version{std::stoi(override)};
            } catch (...) {
                vlog(
                  featureslog.error,
                  "Invalid latest logical version override '{}'",
                  override);
            }
        }
    }

    return latest_version_cache;
}

cluster_version feature_table::get_earliest_logical_version() {
    // Avoid getenv on every call by keeping a shard-local cache
    // of the version after applying any environment override.
    static thread_local cluster_version earliest_version_cache{invalid_version};

    if (earliest_version_cache == invalid_version) {
        earliest_version_cache = earliest_version;

        auto override = std::getenv("__REDPANDA_EARLIEST_LOGICAL_VERSION");
        if (override != nullptr) {
            try {
                earliest_version_cache = cluster_version{std::stoi(override)};
            } catch (...) {
                vlog(
                  featureslog.error,
                  "Invalid earliest logical version override '{}'",
                  override);
            }
        }
    }

    return earliest_version_cache;
}

void feature_state::transition_active() { _state = state::active; }

void feature_state::transition_preparing() {
    if (spec.prepare_rule == feature_spec::prepare_policy::always) {
        // Policy does not require a preparing stage: proceed immediately
        // to the next state
        transition_active();
    } else {
        // Hold in this state, wait for input.
        _state = state::preparing;
    }
}

void feature_state::transition_available() {
    // Even if we have prepare_policy::always, we stop at available here.
    // The responsibility for going active is in feature_manager, where it
    // is conditional on the global features_auto_enable property.
    _state = state::available;
}

void feature_state::notify_version(cluster_version v) {
    if (_state == state::unavailable && v >= spec.require_version) {
        transition_available();
    }
}

void feature_table::set_active_version(
  cluster_version v, feature_table::version_durability durability) {
    _active_version = v;

    if (
      durability == version_durability::durable
      && _original_version == invalid_version) {
        // Rely on controller log replay to call us first with
        // the first version the cluster ever agreed upon.
        set_original_version(v);
    }

    for (auto& fs : _feature_state) {
        fs.notify_version(v);
    }

    on_update();
}

/**
 * The "normal" set_active_version method increments the version but does
 * not activate features (though it may make them available).  This is
 * because activating features on upgrade is optional, and that optionality
 * is respected at time of writing to the controller log, not replaying it.
 *
 * The reason for making auto-activation optional is to enable cautious
 * upgrades, but during cluster bootstrap this motivation goes away, so
 * we will unconditionally switch on all the available features when we
 * see the bootstrap message's cluster version in the controller log.  That
 * is what this function does, as well as calling through to set_active_version.
 */
void feature_table::bootstrap_active_version(
  cluster_version v, feature_table::version_durability durability) {
    if (ss::this_shard_id() == ss::shard_id{0}) {
        vlog(
          featureslog.info, "Activating features from bootstrap version {}", v);
    }

    set_active_version(v, durability);

    for (auto& fs : _feature_state) {
        if (
          fs.get_state() == feature_state::state::available
          && fs.spec.available_rule == feature_spec::available_policy::always
          && _active_version >= fs.spec.require_version) {
            // Intentionally do not pass through the 'preparing' state, as
            // that is for upgrade handling, and we are replaying the bootstrap
            // of a cluster that started at this version.
            fs.transition_active();
        }
    }

    on_update();
}

void feature_table::bootstrap_original_version(cluster_version v) {
    if (ss::this_shard_id() == ss::shard_id{0}) {
        vlog(
          featureslog.info,
          "Set original_version from bootstrap version {}",
          v);
    }

    set_original_version(v);

    // No on_update() call needed: bootstrap version is only advisory and
    // does not drive the feature state machines.
}

/**
 * Call this after changing any state.
 *
 * Update any watchers, update bitmask
 */
void feature_table::on_update() {
    // Update mask for fast is_active() lookup
    _active_features_mask = 0x0;
    for (const auto& fs : _feature_state) {
        if (fs.get_state() == feature_state::state::active) {
            _active_features_mask |= uint64_t(fs.spec.bits);
            _waiters_active.notify(fs.spec.bits);

            // Anyone waiting on 'preparing' is triggered when we
            // reach active, because at this point we're never going
            // to go to 'preparing' and we don't want to wait forever.
            _waiters_preparing.notify(fs.spec.bits);
        } else if (fs.get_state() == feature_state::state::preparing) {
            _waiters_preparing.notify(fs.spec.bits);
        }
    }
}

void feature_table::apply_action(const feature_update_action& fua) {
    auto feature_id_opt = resolve_name(fua.feature_name);
    if (!feature_id_opt.has_value()) {
        vlog(featureslog.warn, "Ignoring action {}, unknown feature", fua);
        return;
    } else {
        if (ss::this_shard_id() == 0) {
            vlog(featureslog.debug, "apply_action {}", fua);
        }
    }

    auto& fstate = get_state(feature_id_opt.value());
    if (fua.action == feature_update_action::action_t::complete_preparing) {
        if (fstate.get_state() == feature_state::state::preparing) {
            fstate.transition_active();
        } else {
            vlog(
              featureslog.warn,
              "Ignoring action {}, feature is in state {}",
              fua,
              fstate.get_state());
        }
    } else if (fua.action == feature_update_action::action_t::activate) {
        auto current_state = fstate.get_state();
        if (current_state == feature_state::state::disabled_clean) {
            if (_active_version >= fstate.spec.require_version) {
                fstate.transition_preparing();
            } else {
                fstate.transition_unavailable();
            }
        } else if (current_state == feature_state::state::disabled_preparing) {
            fstate.transition_preparing();
        } else if (current_state == feature_state::state::disabled_active) {
            fstate.transition_active();
        } else if (current_state == feature_state::state::available) {
            fstate.transition_preparing();
        } else if (
          current_state == feature_state::state::unavailable
          && _active_version >= fstate.spec.require_version) {
            // Activation during upgrade
            fstate.transition_preparing();
        } else {
            vlog(
              featureslog.warn,
              "Ignoring action {}, feature is in state {}",
              fua,
              current_state);
        }
    } else if (fua.action == feature_update_action::action_t::deactivate) {
        auto current_state = fstate.get_state();
        if (
          current_state == feature_state::state::disabled_preparing
          || current_state == feature_state::state::disabled_active
          || current_state == feature_state::state::disabled_clean) {
            vlog(
              featureslog.warn,
              "Ignoring action {}, feature is in state {}",
              fua,
              current_state);
        } else if (current_state == feature_state::state::active) {
            fstate.transition_disabled_active();
        } else if (current_state == feature_state::state::preparing) {
            fstate.transition_disabled_preparing();
        } else {
            fstate.transition_disabled_clean();
        }
    } else {
        vassert(
          false, "Unknown feature action {}", static_cast<uint8_t>(fua.action));
    }

    on_update();
}

/**
 * Wait until this feature becomes active, or the abort
 * source fires.  If the abort source fires, the future
 * will be an exceptional future.
 */
ss::future<> feature_table::await_feature(feature f, ss::abort_source& as) {
    ss::gate::holder guard(_gate);

    if (is_active(f)) {
        vlog(featureslog.trace, "Feature {} already active", to_string_view(f));
        return ss::now();
    } else {
        vlog(
          featureslog.trace,
          "Waiting for feature active {}",
          to_string_view(f));
        return _waiters_active.await(f, as);
    }
}

ss::future<>
feature_table::await_feature_then(feature f, std::function<void(void)> fn) {
    try {
        co_await await_feature(f);
        fn();
    } catch (ss::abort_requested_exception&) {
        // Shutting down
    } catch (...) {
        // Should never happen, abort is the only exception that await_feature
        // can throw, other than perhaps bad_alloc.
        vlog(
          featureslog.error,
          "Unexpected error awaiting {} feature: {} {}",
          to_string_view(f),
          std::current_exception(),
          ss::current_backtrace());
    }
}

/**
 * Wait until this feature hits 'preparing' state, which is the trigger
 * for any data migration work to start.  This will also return if
 * the feature reaches 'active' state: callers should re-check state
 * after the future is ready.
 */
ss::future<>
feature_table::await_feature_preparing(feature f, ss::abort_source& as) {
    ss::gate::holder guard(_gate);

    if (is_preparing(f)) {
        vlog(
          featureslog.trace, "Feature {} already preparing", to_string_view(f));
        return ss::now();
    } else {
        vlog(
          featureslog.trace,
          "Waiting for feature preparing {}",
          to_string_view(f));
        return _waiters_preparing.await(f, as);
    }
}

feature_state& feature_table::get_state(feature f_id) {
    for (auto& i : _feature_state) {
        if (i.spec.bits == f_id) {
            return i;
        }
    }

    // Should never happen: type of `feature` is enum and
    // _feature_state is initialized to include all possible features
    vassert(false, "Invalid feature ID {}", static_cast<uint64_t>(f_id));
}

std::optional<feature>
feature_table::resolve_name(std::string_view feature_name) const {
    for (auto& i : feature_schema) {
        if (i.name == feature_name) {
            return i.bits;
        }
    }

    return std::nullopt;
}

void feature_table::set_license(security::license license) {
    _license = std::move(license);
}

void feature_table::revoke_license() { _license = std::nullopt; }

const std::optional<security::license>& feature_table::get_license() const {
    return _license;
}

void feature_table::testing_activate_all() {
    for (auto& s : _feature_state) {
        if (s.spec.available_rule == feature_spec::available_policy::always) {
            s.transition_active();
        }
    }
    on_update();
}

feature_table::version_fence
feature_table::decode_version_fence(model::record_batch batch) {
    auto records = batch.copy_records();
    if (records.empty()) {
        throw std::runtime_error("Cannot decode empty version fence batch");
    }
    auto& rec = records.front();
    auto key = serde::from_iobuf<ss::sstring>(rec.release_key());
    if (key != version_fence_batch_key) {
        throw std::runtime_error(fmt::format(
          "Version fence batch does not contain expected key {}: found {}",
          version_fence_batch_key,
          key));
    }
    return serde::from_iobuf<version_fence>(rec.release_value());
}

void feature_table::set_original_version(cluster::cluster_version v) {
    _original_version = v;
    if (v != cluster::invalid_version) {
        config::shard_local_cfg().notify_original_version(
          config::legacy_version{v});
    }
}

/*
 * Redpanda does not permit upgrading from arbitrarily old versions.  We
 * test & support upgrades from one feature release series to the next
 * feature release series only.
 *
 * e.g. 22.1.1 -> 22.2.1 is permitted
 *      22.1.1 -> 22.3.1 is NOT permitted.
 *
 * Note that the "old version" in this check is _not_ simply the last
 * version that was run on this local node: it is the last version that
 * the whole cluster agreed upon.  This means that clusters in a bad state
 * will refuse to proceed with an upgrade if an offline node has prevented
 * the cluster from fully upgrading to the previous version.
 *
 * e.g.
 *  - Three nodes 1,2,3 at version 22.1.1
 *  - Node 3 fails.
 *  - User upgrades nodes 1,2 to version 22.2.1
 *  - At this point the cluster's logical version remains at
 *    the logical version of 22.1.1, because not all the nodes
 *    have reported the new version 22.2.1.
 *  - If user now tries to start redpanda 22.3.1 on node 1 or 2,
 *    it will refuse to start.
 *  - The user must get their cluster into a healthy state at version
 *    22.2.1 (e.g. by decommissioning the failed node) before they
 *    may proceed to version 22.3.1
 */
void feature_table::assert_compatible_version(bool override) {
    auto active_version = get_active_version();
    auto binary_version = features::feature_table::get_latest_logical_version();

    // No version currently in the feature table, can't do a safety check.
    if (active_version == invalid_version) {
        return;
    }

    if (active_version < binary_version) {
        // An upgrade is happening.  This may either be a new feature release,
        // or a logical version change happened on a patch release due to
        // some major backport.
        vlog(
          featureslog.info,
          "Upgrading: this node has logical version {} (Redpanda {}), cluster "
          "is undergoing upgrade from previous logical version {}",
          binary_version,
          redpanda_version(),
          active_version);

        // Compare the old version with our compiled-in knowledge of
        // the lowest version we can safely upgrade from.
        if (active_version < get_earliest_logical_version()) {
            if (override) {
                vlog(
                  featureslog.error,
                  "Upgrading from logical version {} which is outside "
                  "compatible range {}-{}",
                  active_version,
                  get_earliest_logical_version(),
                  get_latest_logical_version());
            } else {
                vassert(
                  false,
                  "Attempted to upgrade from incompatible logical version {} "
                  "to logical version {}!",
                  active_version,
                  binary_version);
            }
        }
    }
}

} // namespace features

namespace cluster {
std::ostream& operator<<(std::ostream& o, const feature_update_action& fua) {
    std::string_view action_name;
    switch (fua.action) {
    case feature_update_action::action_t::complete_preparing:
        action_name = "complete_preparing";
        break;
    case feature_update_action::action_t::activate:
        action_name = "activate";
        break;
    case feature_update_action::action_t::deactivate:
        action_name = "deactivate";
        break;
    }

    fmt::print(o, "{{action {} {} }}", fua.feature_name, action_name);
    return o;
}
} // namespace cluster
