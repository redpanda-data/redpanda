/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/configuration.h"
#include "config/property.h"

#include <absl/container/flat_hash_set.h>
#include <boost/range/iterator_range.hpp>

#include <iosfwd>

namespace features {

enum class license_required_feature {
    audit_logging,
    cloud_storage,
    partition_auto_balancing_continuous,
    core_balancing_continuous,
    gssapi,
    oidc,
    schema_id_validation,
    rbac,
    fips,
    datalake_iceberg,
    leadership_pinning,
};

std::ostream& operator<<(std::ostream&, license_required_feature);

/**
 * Thin wrapper around two sets to indicate the current state of enterprise
 * features in the cluster.
 */
class enterprise_feature_report {
    using vtype = absl::flat_hash_set<license_required_feature>;

public:
    void set(license_required_feature feat, bool enabled);
    bool test(license_required_feature);
    const vtype& enabled() const { return _enabled; }
    const vtype& disabled() const { return _disabled; }

    // This method returns true if there are any feature(s) enabled that require
    // the enterprise license.  Currently the following features require a
    // license:
    // +-------------+---------------------------------+---------------+
    // | Config Type | Config Name                     | Value(s)      |
    // +-------------+---------------------------------+---------------+
    // | Cluster     | `audit_enabled`                 | `true`        |
    // | Cluster     | `cloud_storage_enabled`         | `true`        |
    // | Cluster     | `partition_auto_balancing_mode` | `continuous`  |
    // | Cluster     | `core_balancing_continous`      | `true`        |
    // | Cluster     | `sasl_mechanisms`               | `GSSAPI`      |
    // | Cluster     | `sasl_mechanisms`               | `OAUTHBEARER` |
    // | Cluster     | `http_authentication`           | `OIDC`        |
    // | Cluster     | `enable_schema_id_validation`   | `redpanda`    |
    // | Cluster     | `enable_schema_id_validation`   | `compat`      |
    // | Node        | `fips_mode`                     | `enabled`     |
    // | Node        | `fips_mode`                     | `permissive`  |
    // +-------------+---------------------------------+---------------+
    //
    // Also if there are any non default roles in the role store.
    bool any() const { return !_enabled.empty(); }

private:
    vtype _enabled;
    vtype _disabled;
};

template<config::detail::Property P>
class sanctioning_binding {
public:
    using T = P::value_type;
    explicit sanctioning_binding(config::enterprise<P>& prop)
      : _prop(prop)
      , _binding(_prop.bind()) {
        update_sanctioned_state();
        _binding.watch([this] { update_sanctioned_state(); });
    }

    config::binding<T>& binding() { return _binding; }
    const config::binding<T>& binding() const { return _binding; }

    std::pair<T, bool> operator()(bool should_sanction) const {
        const auto& val = _binding();
        if (should_sanction && _is_sanctioned) [[unlikely]] {
            return std::make_pair(_prop.default_value(), true);
        } else {
            return std::make_pair(val, false);
        }
    }

private:
    config::enterprise<P>& _prop;
    config::binding<T> _binding;
    bool _is_sanctioned{false};

    void update_sanctioned_state() {
        _is_sanctioned = _prop.check_restricted(_binding());
    }
};

namespace details {
template<typename T>
concept NonVoid = !std::is_void_v<T>;

} // namespace details
template<license_required_feature F>
details::NonVoid auto make_sanctioning_binding() {
    if constexpr (
      F == license_required_feature::partition_auto_balancing_continuous) {
        return sanctioning_binding(
          config::shard_local_cfg().partition_autobalancing_mode);
    }

    if constexpr (F == license_required_feature::core_balancing_continuous) {
        return sanctioning_binding(
          config::shard_local_cfg().core_balancing_continuous);
    }

    if constexpr (F == license_required_feature::leadership_pinning) {
        return sanctioning_binding(
          config::shard_local_cfg().default_leaders_preference);
    }
}
} // namespace features
