/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config/types.h"

namespace config {

std::ostream& operator<<(std::ostream& os, audit_failure_policy policy) {
    return os << to_string_view(policy);
}

std::istream& operator>>(std::istream& is, audit_failure_policy& policy) {
    ss::sstring s;
    is >> s;
    policy = string_switch<audit_failure_policy>(s)
               .match(
                 to_string_view(audit_failure_policy::reject),
                 audit_failure_policy::reject)
               .match(
                 to_string_view(audit_failure_policy::permit),
                 audit_failure_policy::permit);
    return is;
}

std::ostream& operator<<(std::ostream& os, datalake_catalog_auth_mode cam) {
    return os << to_string_view(cam);
}

std::istream& operator>>(std::istream& is, datalake_catalog_auth_mode& cam) {
    ss::sstring s;
    is >> s;
    cam = string_switch<datalake_catalog_auth_mode>(s)
            .match(
              to_string_view(datalake_catalog_auth_mode::none),
              datalake_catalog_auth_mode::none)
            .match(
              to_string_view(datalake_catalog_auth_mode::bearer),
              datalake_catalog_auth_mode::bearer)
            .match(
              to_string_view(datalake_catalog_auth_mode::oauth2),
              datalake_catalog_auth_mode::oauth2)
            .match(
              to_string_view(datalake_catalog_auth_mode::aws_sigv4),
              datalake_catalog_auth_mode::aws_sigv4)
            .match(
              to_string_view(datalake_catalog_auth_mode::gcp),
              datalake_catalog_auth_mode::gcp);
    return is;
}

} // namespace config
