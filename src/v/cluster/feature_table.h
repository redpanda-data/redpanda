/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/types.h"
#include "utils/waiter_queue.h"

#include <array>
#include <string_view>

namespace cluster {

enum class feature : std::uint64_t {
    central_config = 0x1,
    consumer_offsets = 0x2,
    maintenance_mode = 0x4,

    // Dummy features for testing only
    test_alpha = uint64_t(1) << 63,
};

/**
 * The definition of a feature specifies rules for when it should
 * be activated,
 */
struct feature_spec {
    // Policy defining how the feature behaves when in 'available' state.
    enum class available_policy {
        // The feature proceeds to activate as soon as it is available
        always,

        // The feature only becomes available once all cluster nodes
        // are recent enough *and* an administrator explicitly enables it.
        explicit_only,
    };

    // Policy defining whether the feature passes through 'preparing'
    // state on the way to 'active' state.
    enum class prepare_policy {
        // The feature is activated as soon as it becomes available.
        always,

        // The feature only becomes active once a migration step has
        // completed and the feature manager has been notified of this.
        requires_migration
    };

    constexpr feature_spec(
      cluster_version require_version_,
      std::string_view name_,
      feature bits_,
      available_policy apol,
      prepare_policy ppol)
      : bits(bits_)
      , name(name_)
      , require_version(require_version_)
      , available_rule(apol)
      , prepare_rule(ppol) {}

    feature bits{0};
    std::string_view name;
    cluster_version require_version;

    available_policy available_rule;
    prepare_policy prepare_rule;
};

constexpr static std::array feature_schema{
  feature_spec{
    cluster_version{1},
    "central_config",
    feature::central_config,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster_version{2},
    "consumer_offsets",
    feature::consumer_offsets,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::requires_migration},
  feature_spec{
    cluster_version{3},
    "maintenance_mode",
    feature::maintenance_mode,
    feature_spec::available_policy::always,
    feature_spec::prepare_policy::always},
  feature_spec{
    cluster_version{2001},
    "__test_alpha",
    feature::test_alpha,
    feature_spec::available_policy::explicit_only,
    feature_spec::prepare_policy::always}};

/**
 * Feature states
 * ==============
 *
 * Start as unavailable.  Become available once all nodes
 * are recent enough.
 *
 * Once available, either advance straight to 'preparing'
 * if available_policy is 'always', else wait for administrator
 * to activate the feature.
 *
 * Once in preparing, either advance straight to 'active'
 * if prepare_policy is 'always', else wait for notification
 * that preparation is complete before proceeding.
 *
 * Features may be disabled at any time, but from unavailable/available
 * states they go to disabled_clean, whereas from other states they
 * go to disabled_dirty.  This tracks whether the feature may have
 * written persistent structures.
 *
    ┌──────────────┐           ┌──────────────────┐
    │              ├──────────►│                  │
    │  unavailable │           │  disabled_clean  │
    │              │◄──────────┤                  │
    └───────┬──────┘           └───┬──────────────┘
            │                      │            ▲
            ▼                      │            │
    ┌──────────────┐               │            │
    │              │◄──────────────┘            │
    │   available  │                            │
    │              ├────────────────────────────┘
    └───────┬──────┘
            │
            ▼
    ┌──────────────┐            ┌─────────────────────┐
    │              ├───────────►│                     │
    │   preparing  │            │  disabled_preparing │
    │              │◄───────────┤                     │
    └───────┬──────┘            └─────────────────────┘
            │
            ▼
    ┌──────────────┐            ┌─────────────────────┐
    │              ├───────────►│                     │
    │    active    │            │  disabled_active    │
    │              │◄───────────┤                     │
    └──────────────┘            └─────────────────────┘
 *
 */
class feature_state {
public:
    enum class state {
        // Unavailable means not all nodes in the cluster are recent
        unavailable,

        // Available means the feature is eligible for activation.  If the
        // feature spec allows it, it may proceed autonomously to preparing
        // or active.  Otherwise, it may have too wait for an administrator
        // to permit it to activate.
        available,

        // Preparing means the feature is in the process of a data migration
        // or other preparatory step.  It will proceed to active status
        // autonomously.
        preparing,

        // Active means the feature is up and running and ready to use.  This
        // is the normal state of most features through most of their lifetime.
        active,

        // Administratively disabled, but it was in 'active' or 'preparing'
        // state at some point in the past.  Indicates that while the feature
        // is disabled now, there may be data structures written to disk that
        // depend on this feature to read back.
        // The distinction between _active and _perparing variants is needed
        // so that when an administrator re-activates the feature, we know
        // what state to go back into.
        disabled_active,
        disabled_preparing,

        // Administratively disabled, and never progressed past 'available'.
        // Means that any data structures dependent on this feature were
        // never written to disk, and no migrations for this feature were done.
        disabled_clean,
    };

    const feature_spec& spec;

    feature_state(const feature_spec& spec_)
      : spec(spec_){};

    // External inputs
    void notify_version(cluster_version v);

    // State transition hooks
    void transition_unavailable() { _state = state::unavailable; }

    void transition_disabled_clean() { _state = state::disabled_clean; }
    void transition_disabled_preparing() { _state = state::disabled_preparing; }
    void transition_disabled_active() { _state = state::disabled_active; }

    void transition_available();
    void transition_preparing();
    void transition_active();

    state get_state() const { return _state; };

private:
    state _state{state::unavailable};
};

std::string_view to_string_view(feature);

/**
 * To enable all shards to efficiently check enablement of features
 * in their hot paths, the cluster logical version and features
 * are copied onto each shard.
 *
 * Instances of this class are updated by feature_manager.
 */
class feature_table {
public:
    cluster_version get_active_version() const noexcept {
        return _active_version;
    }

    const std::vector<feature_state>& get_feature_state() const {
        return _feature_state;
    }

    /**
     * Query whether a feature is active, i.e. whether functionality
     * depending on this feature should be allowed to run.
     *
     * Keep this small and simple to be used in hot paths that need to check
     * for feature enablement.
     */
    bool is_active(feature f) const noexcept {
        return (uint64_t(f) & _active_features_mask) != 0;
    }

    /**
     * Query whether a feature has reached ::preparing state, i.e. it is
     * ready to go active, but will wait for preparatory work to be done
     * elsewhere in the system first.
     *
     * Once the preparatory work (like a data migration) is complete,
     * the feature may be advanced to ::active with a `feature_action`
     * RPC to the controller leader
     */
    bool is_preparing(feature f) const noexcept {
        return get_state(f).get_state() == feature_state::state::preparing;
    }

    ss::future<> await_feature(feature f, ss::abort_source& as);

    ss::future<> await_feature_preparing(feature f, ss::abort_source& as);

    static cluster_version get_latest_logical_version();

    feature_table();

    feature_state& get_state(feature f_id);
    const feature_state& get_state(feature f_id) const {
        return const_cast<feature_table&>(*this).get_state(f_id);
    }

    std::optional<feature> resolve_name(std::string_view feature_name) const;

private:
    // Only for use by our friends feature backend & manager
    void set_active_version(cluster_version);
    void apply_action(const feature_update_action& fua);

    void on_update();

    cluster_version _active_version{invalid_version};

    std::vector<feature_state> _feature_state;

    // Bitmask only used at runtime: if we run out of bits for features
    // just use a bigger one.  Do not serialize this as a bitmask anywhere.
    uint64_t _active_features_mask{0};

    // Waiting for a particular feature to be active
    waiter_queue<feature> _waiters_active;

    // Waiting for a particular feature to be preparing
    waiter_queue<feature> _waiters_preparing;

    // feature_manager is a friend so that they can initialize
    // the active version on single-node first start.
    friend class feature_manager;

    // feature_backend is a friend for routine updates when
    // applying raft0 log events.
    friend class feature_backend;
};

} // namespace cluster
