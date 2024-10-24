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
    using range = boost::iterator_range<vtype::const_iterator>;

public:
    void set(license_required_feature feat, bool enabled);
    bool test(license_required_feature);
    range enabled() const { return _enabled; }
    range disabled() const { return _disabled; }

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

} // namespace features
