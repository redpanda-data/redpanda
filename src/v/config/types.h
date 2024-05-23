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

} // namespace config
