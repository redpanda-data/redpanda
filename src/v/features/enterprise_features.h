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
    range enabled() const { return _enabled; }
    range disabled() const { return _disabled; }
    bool any() const { return !_enabled.empty(); }

private:
    vtype _enabled;
    vtype _disabled;
};

} // namespace features
