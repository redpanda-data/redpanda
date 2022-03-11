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

#include "feature_table.h"

#include "cluster/logger.h"
#include "cluster/types.h"

namespace cluster {

std::string_view to_string_view(feature f) {
    switch (f) {
    case feature::central_config:
        return "central_config";
    case feature::consumer_offsets:
        return "consumer_offsets";
    case feature::maintenance_mode:
        return "maintenance_mode";
    case feature::test_alpha:
        return "test_alpha";
    }
    __builtin_unreachable();
}

// The version that this redpanda node will report: increment this
// on protocol changes to raft0 structures, like adding new services.
static constexpr cluster_version latest_version = cluster_version{3};

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

        auto override = std::getenv("__REDPANDA_LOGICAL_VERSION");
        if (override != nullptr) {
            try {
                latest_version_cache = cluster_version{std::stoi(override)};
            } catch (...) {
                vlog(
                  clusterlog.error,
                  "Invalid logical version override '{}'",
                  override);
            }
        }
    }

    return latest_version_cache;
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

void feature_table::set_active_version(cluster_version v) {
    _active_version = v;

    for (auto& fs : _feature_state) {
        fs.notify_version(v);
    }

    on_update();
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
        vlog(clusterlog.warn, "Ignoring action {}, unknown feature", fua);
        return;
    } else {
        if (ss::this_shard_id() == 0) {
            vlog(clusterlog.debug, "apply_action {}", fua);
        }
    }

    auto& fstate = get_state(feature_id_opt.value());
    if (fua.action == feature_update_action::action_t::complete_preparing) {
        if (fstate.get_state() == feature_state::state::preparing) {
            fstate.transition_active();
        } else {
            vlog(
              clusterlog.warn,
              "Ignoring action {}, feature is in state {}",
              fua,
              fstate.get_state());
        }
    } else if (
      fua.action == feature_update_action::action_t::administrative_activate) {
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
              clusterlog.warn,
              "Ignoring action {}, feature is in state {}",
              fua,
              current_state);
        }
    } else if (
      fua.action
      == feature_update_action::action_t::administrative_deactivate) {
        auto current_state = fstate.get_state();
        if (
          current_state == feature_state::state::disabled_preparing
          || current_state == feature_state::state::disabled_active
          || current_state == feature_state::state::disabled_clean) {
            vlog(
              clusterlog.warn,
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
    if (is_active(f)) {
        vlog(clusterlog.trace, "Feature {} already active", to_string_view(f));
        return ss::now();
    } else {
        vlog(
          clusterlog.trace, "Waiting for feature active {}", to_string_view(f));
        return _waiters_active.await(f, as);
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
    if (is_preparing(f)) {
        vlog(
          clusterlog.trace, "Feature {} already preparing", to_string_view(f));
        return ss::now();
    } else {
        vlog(
          clusterlog.trace,
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

} // namespace cluster
