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

#include "types.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

namespace pandaproxy::schema_registry {

std::ostream& operator<<(std::ostream& os, const schema_type& v) {
    return os << to_string_view(v);
}

std::ostream& operator<<(std::ostream& os, const seq_marker& v) {
    if (v.seq.has_value() && v.node.has_value()) {
        fmt::print(
          os,
          "seq={} node={} version={} key_type={}",
          *v.seq,
          *v.node,
          v.version,
          to_string_view(v.key_type));
    } else {
        fmt::print(
          os,
          "unsequenced version={} key_type={}",
          v.version,
          to_string_view(v.key_type));
    }
    return os;
}

} // namespace pandaproxy::schema_registry
