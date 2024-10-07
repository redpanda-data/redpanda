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

#include "cluster/feature_update_action.h"
#include "cluster/version.h"
#include "config/configuration.h"
#include "features/logger.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "version/version.h"

#include <seastar/core/abort_source.hh>

#include <chrono>
#include <memory>

// The feature table is closely related to cluster and uses many types from it
using namespace cluster;
using namespace std::chrono_literals;

namespace features {

std::string_view to_string_view(feature f) {
    switch (f) {
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
    case feature::force_partition_reconfiguration:
        return "force_partition_reconfiguration";
    case feature::raft_append_entries_serde:
        return "raft_append_entries_serde";
    case feature::delete_records:
        return "delete_records";
    case feature::raft_coordinated_recovery:
        return "raft_coordinated_recovery";
    case feature::cloud_storage_scrubbing:
        return "cloud_storage_scrubbing";
    case feature::enhanced_force_reconfiguration:
        return "enhanced_force_reconfiguration";
    case feature::broker_time_based_retention:
        return "broker_time_based_retention";
    case feature::wasm_transforms:
        return "wasm_transforms";
    case feature::raft_config_serde:
        return "raft_config_serde";
    case feature::fast_partition_reconfiguration:
        return "fast_partition_reconfiguration";
    case feature::disabling_partitions:
        return "disabling_partitions";
    case feature::cloud_metadata_cluster_recovery:
        return "cloud_metadata_cluster_recovery";
    case feature::audit_logging:
        return "audit_logging";
    case feature::compaction_placeholder_batch:
        return "compaction_placeholder_batch";
    case feature::partition_shard_in_health_report:
        return "partition_shard_in_health_report";
    case feature::role_based_access_control:
        return "role_based_access_control";
    case feature::cluster_topic_manifest_format_v2:
        return "cluster_topic_manifest_format_v2";
    case feature::node_local_core_assignment:
        return "node_local_core_assignment";
    case feature::unified_tx_state:
        return "unified_tx_state";
    case feature::data_migrations:
        return "data_migrations";
    case feature::group_tx_fence_dedicated_batch_type:
        return "group_tx_fence_dedicated_batch_type";
    case feature::transforms_specify_offset:
        return "transforms_specify_offset";
    case feature::remote_labels:
        return "remote_labels";
    case feature::partition_properties_stm:
        return "partition_properties_stm";
    case feature::shadow_indexing_split_topic_property_update:
        return "shadow_indexing_split_topic_property_update";

    /*
     * testing features
     */
    case feature::test_alpha:
        return "__test_alpha";
    case feature::test_bravo:
        return "__test_bravo";
    case feature::test_charlie:
        return "__test_charlie";
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
constexpr cluster_version latest_version = to_cluster_version(
  release_version::MAX);

// The earliest version we can upgrade from. This is the version that
// a freshly initialized node will start at. All features up to this cluster
// version will automatically be enabled when Redpanda starts.
constexpr cluster_version earliest_version = to_cluster_version(
  release_version::v23_3_1);

static_assert(
  latest_version - earliest_version == 3L,
  "Consider upgrading the earliest_version in lockstep whenever you increment "
  "the latest_version");

namespace {
bool is_major_version_release(cluster::cluster_version version) {
    if (
      version < to_cluster_version(release_version::MIN)
      || version > to_cluster_version(release_version::MAX)) {
        // Unknown versions default to being a major version release
        return true;
    }
    switch (static_cast<release_version>(version())) {
    case release_version::v22_1_1:
        return true;
    case release_version::v22_1_5:
        return false;
    case release_version::v22_2_1:
        return true;
    case release_version::v22_2_6:
        return false;
    case release_version::v22_3_1:
        return true;
    case release_version::v22_3_6:
        return false;
    case release_version::v23_1_1:
    case release_version::v23_2_1:
    case release_version::v23_3_1:
    case release_version::v24_1_1:
    case release_version::v24_2_1:
    case release_version::v24_3_1:
        return true;
    }
    __builtin_unreachable();
}
} // namespace

bool is_major_version_upgrade(
  cluster::cluster_version from, cluster::cluster_version to) {
    for (cluster::cluster_version i = from + 1L; i <= to; ++i) {
        if (is_major_version_release(i)) {
            return true;
        }
    }
    return false;
}

// Extra features that will be wired into the feature table if a special
// environment variable is set
static std::array test_extra_schema{
  // For testing, a feature that does not auto-activate
  feature_spec{
    cluster::cluster_version{2001},
    "__test_alpha",
    feature::test_alpha,
    feature_spec::available_policy::explicit_only,
    feature_spec::prepare_policy::always},

  // For testing, a feature that auto-activates
  feature_spec{
    cluster::cluster_version{2001},
    "__test_bravo",
    feature::test_bravo,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},

  // For testing, a feature that auto-activates
  feature_spec{
    cluster::cluster_version{2001},
    "__test_charlie",
    feature::test_charlie,
    feature_spec::available_policy::new_clusters_only,
    feature_spec::prepare_policy::always},
};

class feature_table::probe {
public:
    explicit probe(const feature_table& parent)
      : _parent(parent) {}

    probe(const probe&) = delete;
    probe& operator=(const probe&) = delete;
    probe(probe&&) = delete;
    probe& operator=(probe&&) = delete;
    ~probe() noexcept = default;

    void setup_metrics() {
        if (ss::this_shard_id() != 0) {
            return;
        }

        if (!config::shard_local_cfg().disable_metrics()) {
            setup_metrics_for(_metrics);
        }

        if (!config::shard_local_cfg().disable_public_metrics()) {
            setup_metrics_for(_public_metrics);
        }
    }

    void setup_metrics_for(metrics::metric_groups_base& metrics) {
        namespace sm = ss::metrics;

        static_assert(
          !std::is_move_constructible_v<feature_table>
            && !std::is_move_assignable_v<feature_table>
            && !std::is_copy_constructible_v<feature_table>
            && !std::is_copy_assignable_v<feature_table>,
          "The probe captures a reference to this");

        metrics.add_group(
          prometheus_sanitize::metrics_name("cluster:features"),
          {
            sm::make_gauge(
              "enterprise_license_expiry_sec",
              [&ft = _parent]() {
                  return calculate_expiry_metric(ft.get_license());
              },
              sm::description("Number of seconds remaining until the "
                              "Enterprise license expires"))
              .aggregate({sm::shard_label}),
          });
    }

    const feature_table& _parent;
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

feature_table::feature_table() {
    // Intentionally undocumented environment variable, only for use
    // in integration tests.
    const bool enable_test_features
      = (std::getenv("__REDPANDA_TEST_FEATURES") != nullptr);

    _feature_state.reserve(
      feature_schema.size()
      + (enable_test_features ? test_extra_schema.size() : 0));
    for (const auto& spec : feature_schema) {
        _feature_state.emplace_back(spec);
    }
    if (enable_test_features) {
        for (const auto& spec : test_extra_schema) {
            _feature_state.emplace_back(spec);
        }
    }

    // For integration testing: toggle the activation mode of a test feature
    // to simulate upgrading from a .z release where it is explicit_only
    // to a .z release where it is auto-activating.
    const bool no_auto_activate_bravo
      = (std::getenv("__REDPANDA_TEST_FEATURE_NO_AUTO_ACTIVATE_BRAVO") != nullptr);
    if (no_auto_activate_bravo) {
        for (auto& spec : test_extra_schema) {
            if (spec.name == "__test_bravo") {
                spec.available_rule
                  = feature_spec::available_policy::explicit_only;
                break;
            }
        }
    }

    _probe = std::make_unique<probe>(*this);
    _probe->setup_metrics();
}

feature_table::~feature_table() noexcept = default;

ss::future<> feature_table::stop() {
    _probe.reset();
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
        if (features::retired_features.contains(fua.feature_name)) {
            vlog(featureslog.debug, "Ignoring action {}, retired feature", fua);
        } else {
            vlog(featureslog.warn, "Ignoring action {}, unknown feature", fua);
        }
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
    } catch (const ss::abort_requested_exception&) {
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
    for (auto& i : _feature_state) {
        if (i.spec.name == feature_name) {
            return i.spec.bits;
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
        if (
          s.spec.available_rule == feature_spec::available_policy::always
          || s.spec.available_rule
               == feature_spec::available_policy::new_clusters_only) {
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

long long feature_table::calculate_expiry_metric(
  const std::optional<security::license>& license,
  security::license::clock::time_point now) {
    if (!license) {
        return -1;
    }

    auto rem = license->expiration() - now;
    auto rem_capped = std::max(rem.zero(), rem);
    return rem_capped / 1s;
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
