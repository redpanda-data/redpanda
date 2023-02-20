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

#pragma once

#include "cluster/types.h"

namespace features {

struct feature_spec;
struct feature_table_snapshot;

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
    enum class state : uint8_t {
        // Unavailable means not all nodes in the cluster are recent
        unavailable = 0,

        // Available means the feature is eligible for activation.  If the
        // feature spec allows it, it may proceed autonomously to preparing
        // or active.  Otherwise, it may have too wait for an administrator
        // to permit it to activate.
        available = 1,

        // Preparing means the feature is in the process of a data migration
        // or other preparatory step.  It will proceed to active status
        // autonomously.
        preparing = 2,

        // Active means the feature is up and running and ready to use.  This
        // is the normal state of most features through most of their lifetime.
        active = 3,

        // Administratively disabled, but it was in 'active' or 'preparing'
        // state at some point in the past.  Indicates that while the feature
        // is disabled now, there may be data structures written to disk that
        // depend on this feature to read back.
        // The distinction between _active and _perparing variants is needed
        // so that when an administrator re-activates the feature, we know
        // what state to go back into.
        disabled_active = 4,
        disabled_preparing = 5,

        // Administratively disabled, and never progressed past 'available'.
        // Means that any data structures dependent on this feature were
        // never written to disk, and no migrations for this feature were done.
        disabled_clean = 6,
    };

    const feature_spec& spec;

    feature_state(const feature_spec& spec_)
      : spec(spec_){};

    // External inputs
    void notify_version(cluster::cluster_version v);

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

    friend struct feature_table_snapshot;
};

} // namespace features