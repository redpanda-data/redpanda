/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "types.h"

#include "util.h"

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

std::ostream& operator<<(
  std::ostream& os,
  const typed_schema_definition<unparsed_schema_definition::tag>& def) {
    fmt::print(
      os,
      "type: {}, definition: {}, references: {}",
      to_string_view(def.type()),
      // TODO BP: Prevent this linearization
      to_string(def.shared_raw()),
      def.refs());
    return os;
}

std::ostream& operator<<(
  std::ostream& os,
  const typed_schema_definition<canonical_schema_definition::tag>& def) {
    fmt::print(
      os,
      "type: {}, definition: {}, references: {}",
      to_string_view(def.type()),
      // TODO BP: Prevent this linearization
      to_string(def.shared_raw()),
      def.refs());
    return os;
}

std::ostream& operator<<(std::ostream& os, const schema_reference& ref) {
    fmt::print(os, "{:l}", ref);
    return os;
}

bool operator<(const schema_reference& lhs, const schema_reference& rhs) {
    return std::tie(lhs.name, lhs.sub, lhs.version)
           < std::tie(rhs.name, rhs.sub, rhs.version);
}

std::ostream& operator<<(std::ostream& os, const unparsed_schema& ref) {
    fmt::print(os, "subject: {}, {}", ref.sub(), ref.def());
    return os;
}

std::ostream& operator<<(std::ostream& os, const canonical_schema& ref) {
    fmt::print(os, "subject: {}, {}", ref.sub(), ref.def());
    return os;
}

std::ostream& operator<<(std::ostream& os, const compatibility_result& res) {
    fmt::print(os, "is_compat: {}, messages: {}", res.is_compat, res.messages);
    return os;
}

} // namespace pandaproxy::schema_registry
