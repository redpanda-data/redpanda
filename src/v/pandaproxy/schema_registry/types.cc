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
#include "utils/to_string.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

namespace pandaproxy::schema_registry {

std::ostream& operator<<(std::ostream& os, const schema_type& v) {
    return os << to_string_view(v);
}

std::ostream& operator<<(std::ostream& os, const output_format& v) {
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

std::ostream& operator<<(std::ostream& os, const schema_definition& def) {
    fmt::print(
      os,
      "type: {}, definition: {}, references: {}, metadata: {}",
      to_string_view(def.type()),
      // TODO BP: Prevent this linearization
      to_string(def.shared_raw()),
      def.refs(),
      def.meta());
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

std::ostream& operator<<(std::ostream& os, const subject_schema& schema) {
    fmt::print(os, "subject: {}, {}", schema.sub(), schema.def());
    return os;
}

std::ostream& operator<<(std::ostream& os, const compatibility_result& res) {
    fmt::print(os, "is_compat: {}, messages: {}", res.is_compat, res.messages);
    return os;
}

fmt::iterator schema_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ properties: {{ {} }} }}", properties);
}

context_subject context_subject::from_string(std::string_view input) {
    // If qualified subject parsing is disabled, treat everything as literal
    if (!enable_qualified_subjects::get()) {
        return context_subject{default_context, subject{input}};
    }

    // Check for qualified syntax: starts with ":."
    if (input.starts_with(":.")) {
        // Find the second colon that separates context from subject
        auto second_colon = input.find(':', 2);

        if (second_colon != std::string_view::npos) {
            auto ctx_str = input.substr(1, second_colon - 1);
            auto sub_str = input.substr(second_colon + 1);

            return context_subject{context{ctx_str}, subject{sub_str}};
        }
    }

    // Default case: unqualified subject or invalid qualified syntax
    return context_subject{default_context, subject{input}};
}

} // namespace pandaproxy::schema_registry
