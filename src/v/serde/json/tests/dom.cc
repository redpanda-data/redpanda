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

#include "serde/json/tests/dom.h"

#include "absl/strings/escaping.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"

#include <seastar/util/variant_utils.hh>

namespace serde::json::test::dom {

namespace {

std::string iobuf_as_string(iobuf b) {
    iobuf_parser p(std::move(b));
    return absl::CHexEscape(p.read_string(p.bytes_left()));
}

void debug_format_value(std::ostream& os, const value& v, int base_indent = 0) {
    os << std::string(base_indent, ' ');

    ss::visit(
      v.data(),
      [&os](const iobuf& v) {
          os << "string(" << iobuf_as_string(v.copy()) << ")";
      },
      [&os, base_indent](const json_object& v) {
          os << "object(";
          for (const auto& [k, v] : v) {
              os << "\n"
                 << std::string(base_indent + 2, ' ') << "key("
                 << iobuf_as_string(k.copy()) << ") :\n";
              debug_format_value(os, v, base_indent + 4);
          }
          if (v.size() == 0) {
              os << ")";
          } else {
              os << "\n" << std::string(base_indent, ' ') << ")";
          }
      },
      [&os, base_indent](const json_array& v) {
          os << "array(";
          for (const auto& v : v) {
              os << "\n";
              debug_format_value(os, v, base_indent + 2);
          }
          if (v.size() == 0) {
              os << ")";
          } else {
              os << "\n" << std::string(base_indent, ' ') << ")";
          }
      },
      [&os](const null_t&) { os << "null"; },
      [&os](bool b) { os << (b ? "true" : "false"); },
      [&os](int64_t i) { os << "int(" << i << ")"; },
      [&os](double d) { os << "double(" << fmt::format("{}", d) << ")"; });
}

} // namespace

std::ostream& operator<<(std::ostream& os, const value& v) {
    os << "json_value(";
    debug_format_value(os, v, 0);
    return os << ")";
}

} // namespace serde::json::test::dom
