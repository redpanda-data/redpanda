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

} // namespace config
