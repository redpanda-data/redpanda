/*
 * Copyright 2023 Redpanda Data, Inc.
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

#include <seastar/core/sstring.hh>

#include <iostream>

namespace pandaproxy::schema_registry {

enum class schema_id_validation_mode {
    // Disabled
    none = 0,
    // Use Redpanda topic properties
    redpanda,
    // Use Redpanda and compatible topic properties
    compat,
};

constexpr std::string_view to_string_view(schema_id_validation_mode m) {
    switch (m) {
    case schema_id_validation_mode::none:
        return "none";
    case schema_id_validation_mode::redpanda:
        return "redpanda";
    case schema_id_validation_mode::compat:
        return "compat";
    }
}

inline std::ostream& operator<<(std::ostream& o, schema_id_validation_mode m) {
    return o << to_string_view(m);
}

inline std::istream& operator>>(std::istream& i, schema_id_validation_mode& m) {
    seastar::sstring s;
    i >> s;
    m = string_switch<schema_id_validation_mode>(s)
          .match(
            to_string_view(schema_id_validation_mode::none),
            schema_id_validation_mode::none)
          .match(
            to_string_view(schema_id_validation_mode::redpanda),
            schema_id_validation_mode::redpanda)
          .match(
            to_string_view(schema_id_validation_mode::compat),
            schema_id_validation_mode::compat);
    return i;
}

} // namespace pandaproxy::schema_registry
