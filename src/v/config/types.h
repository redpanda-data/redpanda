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

#include "strings/string_switch.h"

#include <ostream>

/*
 * Because `config::` is used across every part of Redpanda, it's easy to create
 * accidental circular dependencies by including sub-system specific types in
 * the configuration.
 *
 * This file is expected to contain dependency-free types that mirror sub-system
 * specific types. It is then expected that each sub-system convert as needed.
 *
 * Example:
 *
 *    config/types.h
 *    ==============
 *
 *      - defines config::s3_url_style enumeration and uses this in a
 *      configuration option.
 *
 *    cloud_storage_clients/types.h
 *    =============================
 *
 *      - defines its own type T, such as a s3_url_style enumeration, or
 *        whatever representation it wants to use that is independent from
 *        the config type.
 *
 *      - defines a `T from_config(config::s3_url_style)` conversion type used
 *      to convert from the configuration option type to the sub-system type.
 */

#include <seastar/core/sstring.hh>

#include <base/seastarx.h>
namespace config {

enum class s3_url_style { virtual_host = 0, path };

inline std::ostream& operator<<(std::ostream& os, const s3_url_style& us) {
    switch (us) {
    case s3_url_style::virtual_host:
        return os << "virtual_host";
    case s3_url_style::path:
        return os << "path";
    }
}

enum class fips_mode_flag : uint8_t {
    // FIPS mode disabled
    disabled = 0,
    // FIPS mode enabled with permissive environment checks
    permissive = 1,
    // FIPS mode enabled with strict environment checks
    enabled = 2,
};

constexpr std::string_view to_string_view(fips_mode_flag f) {
    switch (f) {
    case fips_mode_flag::disabled:
        return "disabled";
    case fips_mode_flag::enabled:
        return "enabled";
    case fips_mode_flag::permissive:
        return "permissive";
    }
}

inline std::ostream& operator<<(std::ostream& o, fips_mode_flag f) {
    return o << to_string_view(f);
}

inline std::istream& operator>>(std::istream& i, fips_mode_flag& f) {
    ss::sstring s;
    i >> s;
    f = string_switch<fips_mode_flag>(s)
          .match(
            to_string_view(fips_mode_flag::disabled), fips_mode_flag::disabled)
          .match(
            to_string_view(fips_mode_flag::enabled), fips_mode_flag::enabled)
          .match(
            to_string_view(fips_mode_flag::permissive),
            fips_mode_flag::permissive);
    return i;
}

inline bool fips_mode_enabled(fips_mode_flag f) {
    return f != fips_mode_flag::disabled;
}

enum class tls_version { v1_0 = 0, v1_1, v1_2, v1_3 };

constexpr std::string_view to_string_view(tls_version v) {
    switch (v) {
    case tls_version::v1_0:
        return "v1.0";
    case tls_version::v1_1:
        return "v1.1";
    case tls_version::v1_2:
        return "v1.2";
    case tls_version::v1_3:
        return "v1.3";
    }
}

inline std::ostream& operator<<(std::ostream& os, const tls_version& v) {
    return os << to_string_view(v);
}

enum class datalake_catalog_type { object_storage, rest };

constexpr std::string_view to_string_view(datalake_catalog_type ct) {
    switch (ct) {
    case datalake_catalog_type::object_storage:
        return "object_storage";
    case datalake_catalog_type::rest:
        return "rest";
    }
}
static constexpr auto acceptable_datalake_catalog_types() {
    return std::to_array(
      {to_string_view(datalake_catalog_type::rest),
       to_string_view(datalake_catalog_type::object_storage)});
}

inline std::ostream& operator<<(std::ostream& o, datalake_catalog_type ct) {
    return o << to_string_view(ct);
}

inline std::istream& operator>>(std::istream& is, datalake_catalog_type& ct) {
    ss::sstring s;
    is >> s;
    ct = string_switch<datalake_catalog_type>(s)
           .match(
             to_string_view(datalake_catalog_type::rest),
             datalake_catalog_type::rest)
           .match(
             to_string_view(datalake_catalog_type::object_storage),
             datalake_catalog_type::object_storage);
    return is;
}

enum class datalake_catalog_auth_mode { none, bearer, oauth2, aws_sigv4, gcp };

constexpr std::string_view to_string_view(datalake_catalog_auth_mode cam) {
    switch (cam) {
    case datalake_catalog_auth_mode::none:
        return "none";
    case datalake_catalog_auth_mode::bearer:
        return "bearer";
    case datalake_catalog_auth_mode::oauth2:
        return "oauth2";
    case datalake_catalog_auth_mode::aws_sigv4:
        return "aws_sigv4";
    case datalake_catalog_auth_mode::gcp:
        return "gcp";
    }
}

inline std::ostream&
operator<<(std::ostream& os, datalake_catalog_auth_mode cam) {
    return os << to_string_view(cam);
}

inline std::istream&
operator>>(std::istream& is, datalake_catalog_auth_mode& cam) {
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

enum class tls_name_format { legacy, rfc2253 };

constexpr std::string_view to_string_view(tls_name_format format) {
    switch (format) {
    case tls_name_format::legacy:
        return "legacy";
    case tls_name_format::rfc2253:
        return "rfc2253";
    }
}

static constexpr auto acceptable_tls_name_format_values() {
    return std::to_array(
      {to_string_view(tls_name_format::legacy),
       to_string_view(tls_name_format::rfc2253)});
}

inline std::ostream& operator<<(std::ostream& os, tls_name_format format) {
    return os << to_string_view(format);
}

inline std::istream& operator>>(std::istream& is, tls_name_format& format) {
    ss::sstring s;
    is >> s;
    format = string_switch<tls_name_format>(s)
               .match(
                 to_string_view(tls_name_format::legacy),
                 tls_name_format::legacy)
               .match(
                 to_string_view(tls_name_format::rfc2253),
                 tls_name_format::rfc2253);
    return is;
}

enum class audit_failure_policy : uint8_t {
    // If the audit log is full or misconfigured, reject any request that cannot
    // be audited
    reject,
    // If the audit log is full or misconfigured, permit requests that cannot be
    // audited to proceed, but log a warning that the audit message was dropped
    permit,
};

constexpr std::string_view to_string_view(audit_failure_policy policy) {
    switch (policy) {
    case audit_failure_policy::reject:
        return "reject";
    case audit_failure_policy::permit:
        return "permit";
    }
}

static constexpr auto acceptable_audit_log_failure_policy_values() {
    return std::to_array(
      {to_string_view(audit_failure_policy::reject),
       to_string_view(audit_failure_policy::permit)});
}

std::ostream& operator<<(std::ostream&, audit_failure_policy);
std::istream& operator>>(std::istream&, audit_failure_policy&);
} // namespace config
